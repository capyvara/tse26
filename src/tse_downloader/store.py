"""SQLite catalog with one bounded, batched writer and paginated readers."""

from __future__ import annotations

import asyncio
import csv
import fcntl
import json
import os
import sqlite3
from collections.abc import AsyncIterator, Iterable
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .models import DownloadError, Filters

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
    status TEXT NOT NULL, filters_json TEXT NOT NULL, details_json TEXT
);
CREATE TABLE IF NOT EXISTS states (
    cycle TEXT NOT NULL, poll TEXT NOT NULL, state_code TEXT NOT NULL, state_name TEXT NOT NULL,
    PRIMARY KEY (cycle, poll, state_code)
);
CREATE TABLE IF NOT EXISTS municipalities (
    cycle TEXT NOT NULL, poll TEXT NOT NULL, state_code TEXT NOT NULL,
    municipality_code TEXT NOT NULL, municipality_name TEXT NOT NULL, ibge_code TEXT,
    PRIMARY KEY (cycle, poll, state_code, municipality_code)
);
CREATE TABLE IF NOT EXISTS zones (
    cycle TEXT NOT NULL, poll TEXT NOT NULL, state_code TEXT NOT NULL,
    municipality_code TEXT NOT NULL, zone_code TEXT NOT NULL,
    PRIMARY KEY (cycle, poll, state_code, municipality_code, zone_code)
);
CREATE TABLE IF NOT EXISTS sections (
    id INTEGER PRIMARY KEY, cycle TEXT NOT NULL, poll TEXT NOT NULL, state_code TEXT NOT NULL,
    municipality_code TEXT NOT NULL, zone_code TEXT NOT NULL, section_code TEXT NOT NULL,
    principal_section_code TEXT NOT NULL, aggregated INTEGER NOT NULL,
    aggregated_sections_json TEXT NOT NULL, source_date TEXT, source_time TEXT,
    aux_url TEXT, aux_path TEXT, aux_idg TEXT, aux_status TEXT NOT NULL,
    section_status TEXT, last_error TEXT, last_seen_run TEXT NOT NULL,
    UNIQUE (cycle, poll, state_code, municipality_code, zone_code, section_code)
);
CREATE INDEX IF NOT EXISTS section_run ON sections(last_seen_run, aggregated, id);
CREATE TABLE IF NOT EXISTS receipts (
    id INTEGER PRIMARY KEY, section_id INTEGER NOT NULL REFERENCES sections(id),
    receipt_hash TEXT NOT NULL, received_date TEXT, received_time TEXT, receipt_status TEXT,
    last_seen_run TEXT NOT NULL, UNIQUE (section_id, receipt_hash)
);
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY, receipt_id INTEGER NOT NULL REFERENCES receipts(id),
    file_type TEXT NOT NULL, name TEXT NOT NULL, url TEXT NOT NULL, relative_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', size INTEGER, sha256 TEXT, etag TEXT,
    last_modified TEXT, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, downloaded_at TEXT,
    UNIQUE (receipt_id, name)
);
CREATE INDEX IF NOT EXISTS files_status ON files(status, id);
CREATE TABLE IF NOT EXISTS metadata_documents (
    url TEXT PRIMARY KEY, relative_path TEXT NOT NULL, sha256 TEXT NOT NULL,
    etag TEXT, last_modified TEXT, checked_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metadata_versions (
    url TEXT NOT NULL, sha256 TEXT NOT NULL, relative_path TEXT NOT NULL, saved_at TEXT NOT NULL,
    PRIMARY KEY (url, sha256)
);
CREATE VIEW IF NOT EXISTS file_index AS
SELECT f.id, s.cycle, s.poll, s.state_code, st.state_name, s.municipality_code,
       m.municipality_name, m.ibge_code, s.zone_code, s.section_code,
       r.receipt_hash, r.received_date, r.received_time, r.receipt_status,
       f.file_type, f.name, f.url, f.relative_path, f.status, f.size, f.sha256,
       f.etag, f.last_modified, f.attempts, f.last_error, f.downloaded_at
FROM files f JOIN receipts r ON r.id=f.receipt_id JOIN sections s ON s.id=r.section_id
LEFT JOIN states st USING (cycle, poll, state_code)
LEFT JOIN municipalities m USING (cycle, poll, state_code, municipality_code);
CREATE VIEW IF NOT EXISTS section_index AS
SELECT s.*, st.state_name, m.municipality_name, m.ibge_code,
       p.id AS principal_section_id, p.aux_path AS principal_aux_path,
       p.aux_status AS principal_aux_status, p.section_status AS principal_status
FROM sections s
LEFT JOIN states st USING (cycle, poll, state_code)
LEFT JOIN municipalities m USING (cycle, poll, state_code, municipality_code)
LEFT JOIN sections p ON p.cycle=s.cycle AND p.poll=s.poll AND p.state_code=s.state_code
 AND p.municipality_code=s.municipality_code AND p.zone_code=s.zone_code
 AND p.section_code=s.principal_section_code;
CREATE VIEW IF NOT EXISTS section_file_index AS
SELECT s.id AS indexed_section_id, s.state_code, s.municipality_code, s.zone_code,
       s.section_code, s.principal_section_code, s.aggregated,
       r.id AS receipt_id, f.id AS file_id, f.file_type, f.relative_path, f.status
FROM sections s JOIN sections p ON p.cycle=s.cycle AND p.poll=s.poll
 AND p.state_code=s.state_code AND p.municipality_code=s.municipality_code
 AND p.zone_code=s.zone_code AND p.section_code=s.principal_section_code
JOIN receipts r ON r.section_id=p.id JOIN files f ON f.receipt_id=r.id;
PRAGMA user_version = 1;
"""


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@contextmanager
def output_lock(output: Path):
    """Use an advisory lock that the OS releases even after an unclean exit."""
    output.mkdir(parents=True, exist_ok=True)
    with (output / ".tse-downloader.lock").open("a+") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise DownloadError(f"Another process is using output directory {output}") from error
        try:
            stream.seek(0)
            stream.truncate()
            stream.write(str(os.getpid()))
            stream.flush()
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


class Store:
    def __init__(self, output: Path):
        self.output = output
        self.path = output / "index.sqlite3"
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=1024)
        self.connection: sqlite3.Connection | None = None
        self.writer: asyncio.Task | None = None
        self.failure: Exception | None = None

    async def __aenter__(self):
        def initialize():
            self.output.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.path, check_same_thread=False)
            try:
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                if version not in {0, 1}:
                    raise DownloadError(f"Unsupported catalog schema version: {version}")
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA busy_timeout = 30000")
                connection.executescript(SCHEMA)
                connection.commit()
                return connection
            except BaseException:
                connection.close()
                raise

        self.connection = await asyncio.to_thread(initialize)
        self.writer = asyncio.create_task(self._writer())
        return self

    async def __aexit__(self, *_):
        await self.queue.put(None)
        if self.writer:
            await self.writer
        if self.connection:
            await asyncio.to_thread(self.connection.close)

    async def write(self, sql: str, params: tuple = (), *, rows: Iterable[tuple] | None = None):
        await self.transaction(
            [(sql, tuple(rows) if rows is not None else params, rows is not None)]
        )

    async def transaction(self, statements: list[tuple[str, tuple, bool]]):
        if self.failure:
            raise self.failure
        future = asyncio.get_running_loop().create_future()
        await self.queue.put((statements, future))
        await asyncio.shield(future)

    async def _writer(self):
        stopping = False
        while not stopping:
            first = await self.queue.get()
            if first is None:
                break
            batch = [first]
            await asyncio.sleep(0)
            while len(batch) < 256 and not self.queue.empty():
                item = self.queue.get_nowait()
                if item is None:
                    stopping = True
                    break
                batch.append(item)

            def commit(batch=batch):
                assert self.connection is not None
                with self.connection:
                    for statements, _ in batch:
                        for sql, params, many in statements:
                            if many:
                                self.connection.executemany(sql, params)
                            else:
                                self.connection.execute(sql, params)

            try:
                if self.failure:
                    raise self.failure
                await asyncio.to_thread(commit)
            except Exception as error:
                self.failure = error
                for *_, future in batch:
                    if not future.done():
                        future.set_exception(error)
            else:
                for *_, future in batch:
                    if not future.done():
                        future.set_result(None)

    async def query(self, sql: str, params: tuple = ()) -> list[dict]:
        def read():
            with closing(sqlite3.connect(self.path)) as connection:
                connection.row_factory = sqlite3.Row
                return [dict(row) for row in connection.execute(sql, params)]

        return await asyncio.to_thread(read)

    async def pages(
        self, sql: str, params: tuple = (), batch_size: int = 512
    ) -> AsyncIterator[dict]:
        """The input query must expose a unique, increasing integer id."""
        last_id = 0
        while True:
            rows = await self.query(
                f"SELECT * FROM ({sql}) WHERE id > ? ORDER BY id LIMIT ?",
                (*params, last_id, batch_size),
            )
            if not rows:
                break
            for row in rows:
                yield row
            last_id = rows[-1]["id"]

    async def file_pages(self, filters: Filters, batch_size: int = 512) -> AsyncIterator[dict]:
        """Materialize geographic scope once, without retaining a long WAL read snapshot."""
        where, params = filters.sql()

        def initialize():
            connection = sqlite3.connect(self.path, check_same_thread=False)
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA temp_store = FILE")
                connection.execute("CREATE TEMP TABLE scoped_sections(id INTEGER PRIMARY KEY)")
                connection.execute(
                    "INSERT OR IGNORE INTO scoped_sections SELECT s.id FROM sections s "
                    f"WHERE {where}",
                    params,
                )
                connection.execute(
                    "INSERT OR IGNORE INTO scoped_sections SELECT p.id FROM sections s "
                    "JOIN sections p ON p.cycle=s.cycle AND p.poll=s.poll "
                    "AND p.state_code=s.state_code AND p.municipality_code=s.municipality_code "
                    "AND p.zone_code=s.zone_code AND p.section_code=s.principal_section_code "
                    f"WHERE {where}",
                    params,
                )
                connection.commit()
                return connection
            except BaseException:
                connection.close()
                raise

        opening = asyncio.create_task(asyncio.to_thread(initialize))
        try:
            connection = await asyncio.shield(opening)
        except asyncio.CancelledError:
            connection = await opening
            await asyncio.to_thread(connection.close)
            raise
        try:
            last_id = 0
            while True:
                # CROSS JOIN fixes the scan order: range-scan file IDs, then probe receipt
                # and scoped-section primary keys. Each batch ends its read transaction.
                def read_page(last_id=last_id):
                    with closing(
                        connection.execute(
                            "SELECT f.* FROM files f CROSS JOIN receipts r ON r.id=f.receipt_id "
                            "WHERE f.id>? AND r.section_id IN scoped_sections "
                            "ORDER BY f.id LIMIT ?",
                            (last_id, batch_size),
                        )
                    ) as cursor:
                        return [dict(row) for row in cursor.fetchall()]

                reading = asyncio.create_task(asyncio.to_thread(read_page))
                try:
                    rows = await asyncio.shield(reading)
                except asyncio.CancelledError:
                    await reading
                    raise
                if not rows:
                    break
                for row in rows:
                    yield row
                last_id = rows[-1]["id"]
        finally:
            await asyncio.to_thread(connection.close)

    async def summary(self, filters: Filters | None = None) -> dict:
        filters = filters or Filters()
        where, params = filters.sql()
        sections = await self.query(
            f"SELECT COUNT(*) AS total, COALESCE(SUM(aggregated),0) AS aggregated, "
            "COALESCE(SUM(aggregated=0 AND aux_status='available'),0) AS available_primary, "
            "COALESCE(SUM(aggregated=0 AND aux_status='unpublished'),0) AS unpublished_primary, "
            "COALESCE(SUM(aggregated=0 AND aux_status IN ('failed','pending')),0) "
            "AS failed_primary "
            f"FROM sections s WHERE {where}",
            params,
        )
        # Include a selected aggregated section's principal without expanding to its neighbors.
        selected = (
            "SELECT DISTINCT p.id FROM section_index s JOIN sections p "
            f"ON p.id=s.principal_section_id WHERE {where}"
        )
        # Preserve historical files owned by a section whose aggregation has since changed.
        selected_files = f"{selected} UNION SELECT s.id FROM sections s WHERE {where}"
        files = await self.query(
            "SELECT f.status, COUNT(*) AS count, COALESCE(SUM(f.size),0) AS bytes "
            "FROM files f JOIN receipts r ON r.id=f.receipt_id "
            f"WHERE r.section_id IN ({selected_files}) GROUP BY f.status",
            (*params, *params),
        )
        unavailable = await self.query(
            "SELECT COUNT(*) AS count FROM section_index s "
            f"WHERE {where} AND COALESCE(principal_aux_status,'missing')!='available'",
            params,
        )
        primary_auxiliaries = await self.query(
            "SELECT p.aux_status AS status, COUNT(*) AS count FROM sections p "
            f"WHERE p.id IN ({selected}) GROUP BY p.aux_status",
            params,
        )
        file_types = await self.query(
            "SELECT f.file_type, COUNT(*) AS indexed, SUM(f.status='complete') AS complete "
            "FROM files f JOIN receipts r ON r.id=f.receipt_id "
            f"WHERE r.section_id IN ({selected_files}) GROUP BY f.file_type",
            (*params, *params),
        )
        return {
            "sections": sections[0],
            "files": files,
            "file_types": file_types,
            "primary_auxiliaries": primary_auxiliaries,
            "published_files_downloaded": bool(sections[0]["total"])
            and all(row["status"] in {"available", "unpublished"} for row in primary_auxiliaries)
            and all(row["status"] == "complete" for row in files),
            "all_indexed_sections_available": bool(sections[0]["total"])
            and unavailable[0]["count"] == 0,
        }


def export_csvs(output: Path) -> dict[str, int]:
    """Stream a consistent SQLite read snapshot into atomically replaced UTF-8 CSVs."""
    path = output / "index.sqlite3"
    if not path.is_file():
        raise DownloadError(f"Catalog not found: {path}")
    destination = output / "indexes"
    destination.mkdir(parents=True, exist_ok=True)
    queries = {
        "states": "SELECT * FROM states ORDER BY cycle,poll,state_code",
        "municipalities": (
            "SELECT * FROM municipalities ORDER BY cycle,poll,state_code,municipality_code"
        ),
        "zones": "SELECT * FROM zones ORDER BY cycle,poll,state_code,municipality_code,zone_code",
        "sections": "SELECT * FROM section_index ORDER BY id",
        "receipts": (
            "SELECT r.*,s.cycle,s.poll,s.state_code,s.municipality_code,s.zone_code,s.section_code "
            "FROM receipts r JOIN sections s ON s.id=r.section_id ORDER BY r.id"
        ),
        "files": "SELECT * FROM file_index ORDER BY id",
    }
    counts = {}
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("BEGIN")
        for name, query in queries.items():
            temporary = destination / f"{name}.csv.part"
            try:
                cursor = connection.execute(query)
                count = 0
                with temporary.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow([column[0] for column in cursor.description])
                    for row in cursor:
                        writer.writerow(row)
                        count += 1
                temporary.replace(destination / f"{name}.csv")
                counts[name] = count
            finally:
                temporary.unlink(missing_ok=True)
    return counts


def dump_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
