"""End-to-end catalog, history, filtering, resume, export, and verification tests."""

import asyncio
import csv
import json
import sqlite3
from contextlib import closing

import pytest

from tse_downloader.app import download, status, verify
from tse_downloader.models import Filters, Settings
from tse_downloader.store import Store, output_lock


def rows(output, query):
    with closing(sqlite3.connect(output / "index.sqlite3")) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(query)]


def settings(output, mirror):
    return Settings(output, concurrency=4, rate_limit=100, base_url=mirror.url, retries=0)


async def test_full_collection_history_and_csv(tmp_path, mirror):
    result, code = await download(settings(tmp_path, mirror), Filters())
    assert code == 0
    assert result["published_files_downloaded"]
    assert not result["all_indexed_sections_available"]
    assert result["sections"]["total"] == 6
    assert result["sections"]["aggregated"] == 2
    assert len(rows(tmp_path, "SELECT * FROM files")) == 10
    assert len(rows(tmp_path, "SELECT * FROM receipts")) == 4
    assert len(rows(tmp_path, "SELECT * FROM section_file_index")) == 20
    assert not any("/0009/" in path or "/0010/" in path for path in mirror.requests)
    assert {r["file_type"] for r in rows(tmp_path, "SELECT * FROM files")} == {
        "bu",
        "rdv",
        "log",
        "vota",
    }
    assert rows(tmp_path, "SELECT DISTINCT ibge_code FROM municipalities") == [
        {"ibge_code": "1200013"}
    ]
    for file in rows(tmp_path, "SELECT * FROM files"):
        assert (tmp_path / file["relative_path"]).read_bytes().startswith(b"original-")
    for table in ("states", "municipalities", "zones", "sections", "receipts", "files"):
        with (tmp_path / "indexes" / f"{table}.csv").open(encoding="utf-8", newline="") as stream:
            exported = list(csv.DictReader(stream))
        assert exported
        if table == "files":
            assert exported[0]["municipality_code"] == "01120"
            assert exported[0]["zone_code"] == "0008"
            assert exported[0]["section_code"] == "0008"
            assert exported[0]["municipality_name"] == "ACRELÂNDIA"
    assert not list(tmp_path.rglob("*.part"))


async def test_uppercase_official_metadata_without_idg_and_conditional_resume(tmp_path, mirror):
    # Received EA18 files can use uppercase O, omit idg, and include image files.
    for document in mirror.documents.values():
        if isinstance(document, dict):
            document["f"] = "O"
    auxiliary = mirror.documents[mirror.aux_path()]
    auxiliary.pop("idg")
    auxiliary["hashes"][0]["arq"].append({"tp": "imgbu", "nm": "image-imgbu.dat"})
    config = settings(tmp_path, mirror)
    filters = Filters(states=("ac",))
    result, code = await download(config, filters)
    assert code == 0 and result["errors"] == 0 and result["downloaded_files"] == 5
    section = rows(tmp_path, "SELECT aux_idg,aux_path FROM sections WHERE section_code='0008'")[0]
    assert section["aux_idg"] is None
    raw_path = tmp_path / section["aux_path"]
    assert json.loads(raw_path.read_bytes()) == auxiliary
    assert not any("imgbu" in path for path in mirror.requests)
    result, code = await download(config, filters)
    assert code == 0 and result["skipped_files"] == 5 and result["downloaded_files"] == 0
    assert mirror.not_modified[mirror.aux_path()] == 1
    assert json.loads(raw_path.read_bytes())["f"] == "O"


async def test_rerun_conditional_metadata_and_new_receipts(tmp_path, mirror):
    config = settings(tmp_path, mirror)
    await download(config, Filters(states=("ac",)))
    file_requests = sum(mirror.requests[p] for p in mirror.payloads)
    result, code = await download(config, Filters(states=("ac",)))
    assert code == 0 and result["skipped_files"] == 5
    assert sum(mirror.requests[p] for p in mirror.payloads) == file_requests
    assert mirror.not_modified[mirror.aux_path()] == 1
    old_snapshot = rows(tmp_path, "SELECT aux_path FROM sections WHERE section_code='0008'")[0][
        "aux_path"
    ]
    mirror.add_receipt("ac", "hash-three", ("rdv",))
    # A disappeared historical hash must remain in the local archive.
    mirror.documents[mirror.aux_path()]["hashes"].pop(0)
    result, _ = await download(config, Filters(states=("ac",)))
    assert result["downloaded_files"] == 1
    assert len(rows(tmp_path, "SELECT * FROM files")) == 6
    new_snapshot = rows(tmp_path, "SELECT aux_path FROM sections WHERE section_code='0008'")[0][
        "aux_path"
    ]
    assert old_snapshot != new_snapshot
    assert (tmp_path / old_snapshot).is_file() and (tmp_path / new_snapshot).is_file()
    assert len(rows(tmp_path, "SELECT * FROM metadata_versions WHERE url LIKE '%aux.json'")) == 2


async def test_aggregated_section_filter_resolves_principal(tmp_path, mirror):
    filters = Filters(states=("ac",), municipality="01120", zone="0008", section="0009")
    result, code = await download(settings(tmp_path, mirror), filters)
    assert code == 0
    assert result["downloaded_files"] == 5
    assert result["all_indexed_sections_available"]
    assert {r["section_code"] for r in rows(tmp_path, "SELECT * FROM sections")} == {"0008", "0009"}
    assert not any("/zz/" in path for path in mirror.requests)
    assert len(rows(tmp_path, "SELECT * FROM files")) == 5


async def test_verify_missing_corrupt_files_and_resume(tmp_path, mirror):
    config = settings(tmp_path, mirror)
    filters = Filters(states=("ac",))
    await download(config, filters)
    files = rows(tmp_path, "SELECT * FROM files ORDER BY id")
    (tmp_path / files[0]["relative_path"]).unlink()
    corrupt = tmp_path / files[1]["relative_path"]
    corrupt.write_bytes(b"X" * files[1]["size"])
    result, code = await verify(tmp_path, filters)
    assert code == 1
    assert result == {"verified": 5, "missing": 1, "corrupt": 1, "unverified": 0, "errors": 0}
    assert (await verify(tmp_path, filters))[1] == 1
    result, code = await download(config, filters)
    assert code == 0 and result["downloaded_files"] == 2 and result["skipped_files"] == 3
    assert (await verify(tmp_path, filters))[1] == 0
    # A truncated completed file is detected during ordinary resume without explicit verification.
    corrupt.write_bytes(b"X")
    assert (await download(config, filters))[0]["downloaded_files"] == 1


async def test_metadata_cache_missing_or_corrupt_is_refetched(tmp_path, mirror):
    config = settings(tmp_path, mirror)
    filters = Filters(states=("ac",))
    await download(config, filters)
    documents = rows(tmp_path, "SELECT * FROM metadata_documents")
    (tmp_path / documents[0]["relative_path"]).unlink()
    (tmp_path / documents[1]["relative_path"]).write_bytes(b"invalid")
    _, code = await download(config, filters)
    assert code == 0
    for document in documents:
        json.loads((tmp_path / document["relative_path"]).read_bytes())


async def test_failed_files_are_recorded_and_retried_on_rerun(tmp_path, mirror):
    missing = next(path for path in mirror.payloads if "/ac/" in path)
    original = mirror.documents.pop(missing)
    config = settings(tmp_path, mirror)
    result, code = await download(config, Filters(states=("ac",)))
    assert code == 1 and not result["published_files_downloaded"]
    assert mirror.requests[missing] == 1
    assert rows(tmp_path, "SELECT status FROM files WHERE status='failed'")
    mirror.documents[missing] = original
    assert (await download(config, Filters(states=("ac",))))[1] == 0


async def test_cancel_preserves_run_and_exports(tmp_path, mirror, monkeypatch):
    started = asyncio.Event()

    async def slow_files(self):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("tse_downloader.collector.Collector.collect_files", slow_files)
    task = asyncio.create_task(download(settings(tmp_path, mirror), Filters(states=("ac",))))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rows(tmp_path, "SELECT status FROM runs") == [{"status": "interrupted"}]
    assert (tmp_path / "indexes" / "files.csv").is_file()
    with output_lock(tmp_path):
        pass


def test_output_lock_is_exclusive_and_recoverable(tmp_path):
    with output_lock(tmp_path):
        with pytest.raises(Exception, match="Another process"):
            with output_lock(tmp_path):
                pass
    with output_lock(tmp_path):
        pass


async def test_read_only_status(tmp_path, mirror):
    await download(settings(tmp_path, mirror), Filters(states=("ac",)))
    with output_lock(tmp_path):
        result = await status(tmp_path, Filters())
    assert result["latest_run"]["status"] == "partial"
    assert result["sections"]["total"] == 3


async def test_batched_store_failure_propagates_without_hanging(tmp_path):
    async with Store(tmp_path) as store:
        results = await asyncio.gather(
            store.write("INSERT INTO missing_table VALUES (1)"),
            store.write("INSERT INTO missing_table VALUES (2)"),
            return_exceptions=True,
        )
        assert all(isinstance(result, sqlite3.Error) for result in results)


async def test_late_publication_and_missing_file_types(tmp_path, mirror):
    config = settings(tmp_path, mirror)
    filters = Filters(states=("ac",))
    await download(config, filters)
    section_config = mirror.documents["/oficial/ele2026/custom-urns/config/ac/ac-p003221-cs.json"]
    unpublished = section_config["abr"][0]["mu"][0]["zon"][0]["sec"][2]
    unpublished.update({"da": "25/10/2026", "ha": "20:00:00"})
    path = mirror.aux_path().replace("/0008/p", "/0010/p").replace("-s0008-", "-s0010-")
    mirror.documents[path] = {
        "f": "o",
        "idg": "2",
        "st": "Recebida",
        "hashes": [
            {
                "hash": "new-receipt",
                "st": "Recebido",
                "arq": [
                    {"nm": "exact-name-from-auxiliary.dat", "tp": "bu"},
                ],
            }
        ],
    }
    mirror.documents[path.rsplit("/", 1)[0] + "/new-receipt/exact-name-from-auxiliary.dat"] = b"bu"
    result, code = await download(config, filters)
    assert code == 0
    assert result["downloaded_files"] == 1
    assert result["all_indexed_sections_available"]
    assert result["published_files_downloaded"]
    assert rows(tmp_path, "SELECT name FROM files WHERE name='exact-name-from-auxiliary.dat'")


async def test_aggregated_filter_reports_failed_principal(tmp_path, mirror):
    filters = Filters(states=("ac",), section="0009")
    mirror.documents.pop(mirror.aux_path())
    result, code = await download(settings(tmp_path, mirror), filters)
    assert code == 1
    assert not result["published_files_downloaded"]
    assert not (await status(tmp_path, filters))["published_files_downloaded"]


async def test_disk_full_stops_and_records_failed_run(tmp_path, mirror, monkeypatch):
    from tse_downloader.app import describe_exception

    async def disk_full(*args):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr("tse_downloader.collector.Collector.collect_file", disk_full)
    with pytest.raises(ExceptionGroup) as failure:
        await download(settings(tmp_path, mirror), Filters(states=("ac",)))
    assert "No space left on device" in describe_exception(failure.value)[0]
    assert rows(tmp_path, "SELECT status FROM runs") == [{"status": "failed"}]
    assert (tmp_path / "indexes" / "files.csv").is_file()


async def test_required_export_failure_cannot_report_success(tmp_path, mirror, monkeypatch):
    def export_failure(*args):
        raise OSError("Cannot write CSV output")

    monkeypatch.setattr("tse_downloader.app.export_csvs", export_failure)
    with pytest.raises(OSError, match="Cannot write CSV"):
        await download(settings(tmp_path, mirror), Filters(states=("ac",)))
    assert rows(tmp_path, "SELECT status FROM runs") == [{"status": "failed"}]


async def test_verify_reports_unreadable_and_unverified_files(tmp_path, mirror, monkeypatch):
    filters = Filters(states=("ac",))
    await download(settings(tmp_path, mirror), filters)

    def cannot_read(*args):
        raise PermissionError("Permission denied")

    with monkeypatch.context() as patch:
        patch.setattr("tse_downloader.app.file_digest", cannot_read)
        result, code = await verify(tmp_path, filters)
        assert code == 1 and result["errors"] == 5
    assert {r["status"] for r in rows(tmp_path, "SELECT status FROM files")} == {"failed"}
    assert (await verify(tmp_path, filters))[1] == 0
    async with Store(tmp_path) as store:
        await store.write("UPDATE files SET sha256=NULL,status='pending' WHERE id=1")
    result, code = await verify(tmp_path, filters)
    assert code == 1 and result["unverified"] == 1


async def test_file_pages_release_wal_snapshot_between_batches(tmp_path, mirror):
    await download(settings(tmp_path, mirror), Filters(states=("ac",)))
    async with Store(tmp_path) as store:
        files = store.file_pages(Filters(), batch_size=1)
        try:
            await anext(files)
            await store.write("UPDATE files SET attempts=attempts+1 WHERE id=1")
            checkpoint = (await store.query("PRAGMA wal_checkpoint(TRUNCATE)"))[0]
            assert checkpoint["busy"] == 0
            assert await anext(files)
        finally:
            await files.aclose()


async def test_cancel_during_export_waits_for_writer_before_unlocking(
    tmp_path, mirror, monkeypatch
):
    import threading

    from tse_downloader.store import export_csvs

    exporting = threading.Event()
    release = threading.Event()

    def slow_export(output):
        exporting.set()
        assert release.wait(timeout=10)
        return export_csvs(output)

    monkeypatch.setattr("tse_downloader.app.export_csvs", slow_export)
    task = asyncio.create_task(download(settings(tmp_path, mirror), Filters(states=("ac",))))
    try:
        assert await asyncio.to_thread(exporting.wait, 10)
        task.cancel()
        await asyncio.sleep(0)
        with pytest.raises(Exception, match="Another process"):
            with output_lock(tmp_path):
                pass
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
    assert rows(tmp_path, "SELECT status FROM runs") == [{"status": "interrupted"}]
    assert (tmp_path / "indexes" / "files.csv").is_file()
