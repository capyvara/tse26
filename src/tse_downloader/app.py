"""Collection lifecycle, progress, integrity verification, and read-only status."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import sqlite3
import time
from dataclasses import asdict
from pathlib import Path

from .collector import Collector
from .models import DownloadError, Filters, Settings
from .network import HTTPClient, finish_io
from .store import Store, dump_json, export_csvs, output_lock, utc_now

LOG = logging.getLogger(__name__)


async def report_progress(collector: Collector):
    started = time.monotonic()
    while True:
        await asyncio.sleep(5)
        elapsed = max(0.001, time.monotonic() - started)
        client = collector.client
        LOG.info(
            "Stage=%s requests=%d auxiliaries=%d downloaded=%d skipped=%d errors=%d "
            "received=%.1f MiB average=%.2f MiB/s",
            collector.stage,
            client.requests,
            collector.auxiliaries,
            collector.completed,
            collector.skipped,
            collector.errors,
            client.bytes_received / 1024**2,
            client.bytes_received / 1024**2 / elapsed,
        )


async def download(settings: Settings, filters: Filters) -> tuple[dict, int]:
    with output_lock(settings.output):
        async with Store(settings.output) as store, HTTPClient(settings) as client:
            collector = Collector(settings, filters, store, client)
            # Previous in-progress runs were terminated before their final catalog update.
            await store.write(
                "UPDATE runs SET status='interrupted',finished_at=? WHERE status='running'",
                (utc_now(),),
            )
            await store.write(
                "INSERT INTO runs(id,started_at,status,filters_json) VALUES (?,?,'running',?)",
                (collector.run_id, utc_now(), collector.run_filters()),
            )
            progress = asyncio.create_task(report_progress(collector))
            status = "failed"
            details: dict = {}
            try:
                await collector.discover()
                await collector.collect_auxiliaries()
                await collector.collect_files()
                details = await store.summary(filters)
                details.update(
                    {
                        "run_id": collector.run_id,
                        "errors": collector.errors,
                        "requests": client.requests,
                        "received_bytes": client.bytes_received,
                        "downloaded_files": collector.completed,
                        "skipped_files": collector.skipped,
                        "filters": asdict(filters),
                        "base_url": settings.base_url,
                        "concurrency": settings.concurrency,
                        "rate_limit": settings.rate_limit,
                    }
                )
                if collector.errors:
                    details["published_files_downloaded"] = False
                status = (
                    "failed"
                    if collector.errors
                    else "complete"
                    if details["all_indexed_sections_available"]
                    else "partial"
                )
                return details, 1 if collector.errors else 0
            except asyncio.CancelledError:
                status = "interrupted"
                details["error"] = "Interrupted; completed files will be reused on the next run"
                raise
            except BaseException as error:
                details["error"] = str(error)
                raise
            finally:
                try:
                    collector.stage = "CSV export"
                    await finish_io(export_csvs, settings.output)
                    await store.write(
                        "UPDATE runs SET finished_at=?,status=?,details_json=? WHERE id=?",
                        (utc_now(), status, dump_json(details), collector.run_id),
                    )
                except asyncio.CancelledError:
                    with contextlib.suppress(Exception):
                        await store.write(
                            "UPDATE runs SET finished_at=?,status='interrupted',details_json=? "
                            "WHERE id=?",
                            (
                                utc_now(),
                                dump_json({"error": "Interrupted during finalization"}),
                                collector.run_id,
                            ),
                        )
                    raise
                except Exception as error:
                    LOG.error("Could not finalize catalog or CSV exports: %s", error)
                    with contextlib.suppress(Exception):
                        details["finalization_error"] = str(error)
                        await store.write(
                            "UPDATE runs SET finished_at=?,status=?,details_json=? WHERE id=?",
                            (
                                utc_now(),
                                "interrupted" if status == "interrupted" else "failed",
                                dump_json(details),
                                collector.run_id,
                            ),
                        )
                    # A successful run must not hide a failed required export.
                    if status in {"complete", "partial"}:
                        raise
                finally:
                    progress.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await progress


def file_digest(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


async def verify(output: Path, filters: Filters) -> tuple[dict, int]:
    if not (output / "index.sqlite3").is_file():
        raise DownloadError(f"Catalog not found: {output / 'index.sqlite3'}")
    with output_lock(output):
        async with Store(output) as store:
            counts = {"verified": 0, "missing": 0, "corrupt": 0, "unverified": 0, "errors": 0}
            started = time.monotonic()
            async for file in store.file_pages(filters):
                if not file["sha256"]:
                    counts["unverified"] += 1
                    continue
                path = output / file["relative_path"]
                error = None
                status = "complete"
                try:
                    size, sha256 = await finish_io(file_digest, path)
                    if size != file["size"] or sha256 != file["sha256"]:
                        status = "corrupt"
                        error = "Local size or SHA-256 differs from the recorded download"
                except FileNotFoundError:
                    status, error = "missing", "Local file is missing"
                except OSError as exc:
                    counts["errors"] += 1
                    status, error = "failed", str(exc)
                    LOG.error("Cannot verify %s: %s", path, exc)
                if error:
                    if status in counts:
                        counts[status] += 1
                    await store.write(
                        "UPDATE files SET status=?,last_error=? WHERE id=?",
                        (status, error, file["id"]),
                    )
                elif status == "complete" and file["status"] != "complete":
                    await store.write(
                        "UPDATE files SET status='complete',last_error=NULL WHERE id=?",
                        (file["id"],),
                    )
                counts["verified"] += 1
                if time.monotonic() - started >= 5:
                    LOG.info("Verification progress: %s", counts)
                    started = time.monotonic()
            await finish_io(export_csvs, output)
            return counts, int(
                bool(
                    counts["missing"]
                    or counts["corrupt"]
                    or counts["unverified"]
                    or counts["errors"]
                )
            )


async def status(output: Path, filters: Filters) -> dict:
    path = output / "index.sqlite3"
    if not path.is_file():
        raise DownloadError(f"Catalog not found: {path}")
    # Do not initialize the schema or acquire the exclusive writer lock for read-only status.
    store = Store(output)
    summary = await store.summary(filters)
    runs = await store.query("SELECT * FROM runs ORDER BY started_at DESC,rowid DESC LIMIT 1")
    summary["latest_run"] = runs[0] if runs else None
    integrity = await store.query("SELECT COUNT(*) AS count FROM metadata_versions")
    summary["metadata_versions"] = integrity[0]["count"]
    return summary


def describe_exception(error: BaseException) -> list[str]:
    if isinstance(error, BaseExceptionGroup):
        return [message for inner in error.exceptions for message in describe_exception(inner)]
    if isinstance(error, OSError) and error.errno == 28:
        return ["No space left on device. Free disk space and rerun to resume completed work."]
    if isinstance(error, sqlite3.Error):
        return [f"Catalog error: {error}"]
    return [str(error) or type(error).__name__]
