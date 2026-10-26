"""Discover EA11/EA12/EA16/EA18 metadata and collect all published receipts."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .models import (
    CYCLE,
    ELECTIONS,
    FILE_TYPES,
    POLL,
    CircuitOpen,
    DownloadError,
    Filters,
    Settings,
    Templates,
    append_url,
    bounded_map,
    is_official_metadata,
    padded,
    safe_component,
    state_code,
)
from .network import HTTPClient, HTTPFailure
from .store import Store, dump_json, utc_now

LOG = logging.getLogger(__name__)

SECTION_UPSERT = """
INSERT INTO sections(cycle,poll,state_code,municipality_code,zone_code,section_code,
 principal_section_code,aggregated,aggregated_sections_json,source_date,source_time,
 aux_url,aux_status,last_seen_run)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(cycle,poll,state_code,municipality_code,zone_code,section_code) DO UPDATE SET
 principal_section_code=excluded.principal_section_code, aggregated=excluded.aggregated,
 aggregated_sections_json=excluded.aggregated_sections_json, source_date=excluded.source_date,
 source_time=excluded.source_time, aux_url=excluded.aux_url, aux_status=excluded.aux_status,
 last_seen_run=excluded.last_seen_run, last_error=NULL
"""
RECEIPT_UPSERT = """
INSERT INTO receipts(section_id,receipt_hash,received_date,received_time,
 receipt_status,last_seen_run)
VALUES (?,?,?,?,?,?) ON CONFLICT(section_id,receipt_hash) DO UPDATE SET
 received_date=excluded.received_date,received_time=excluded.received_time,
 receipt_status=excluded.receipt_status,last_seen_run=excluded.last_seen_run
"""
FILE_UPSERT = """
INSERT INTO files(receipt_id,file_type,name,url,relative_path)
VALUES ((SELECT id FROM receipts WHERE section_id=? AND receipt_hash=?),?,?,?,?)
ON CONFLICT(receipt_id,name) DO UPDATE SET
 file_type=excluded.file_type,url=excluded.url,relative_path=excluded.relative_path
"""


@dataclass
class Metadata:
    data: dict
    relative_path: str


def decode_metadata(body: bytes, url: str) -> dict:
    try:
        data = json.loads(body)
    except (ValueError, UnicodeError) as error:
        raise DownloadError(f"Invalid JSON metadata: {url}: {error}") from error
    if not is_official_metadata(data):
        phase = data.get("f") if isinstance(data, dict) else None
        raise DownloadError(
            f"Expected official JSON metadata ('f' must be 'o' or 'O'; received {phase!r}): {url}"
        )
    return data


def atomic_bytes(path: Path, body: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".part")
    try:
        with temporary.open("wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class Collector:
    def __init__(self, settings: Settings, filters: Filters, store: Store, client: HTTPClient):
        self.settings = settings
        self.filters = filters
        self.store = store
        self.client = client
        self.run_id = uuid.uuid4().hex
        self.templates: Templates | None = None
        self.errors = 0
        self.auxiliaries = 0
        self.completed = 0
        self.skipped = 0
        self.stage = "configuration"

    async def metadata(self, url: str) -> Metadata:
        rows = await self.store.query("SELECT * FROM metadata_documents WHERE url=?", (url,))
        cached = rows[0] if rows else None
        body = None
        headers = {}
        if cached:
            try:
                body = await asyncio.to_thread(
                    (self.settings.output / cached["relative_path"]).read_bytes
                )
                if hashlib.sha256(body).hexdigest() != cached["sha256"]:
                    body = None
            except OSError:
                body = None
            if body is not None:
                if cached["etag"]:
                    headers["If-None-Match"] = cached["etag"]
                if cached["last_modified"]:
                    headers["If-Modified-Since"] = cached["last_modified"]
        response = await self.client.get(url, headers)
        if response.status == 304:
            if body is None or cached is None:
                raise DownloadError(f"HTTP 304 without a valid cached document: {url}")
            data = decode_metadata(body, url)
            await self.store.write(
                "UPDATE metadata_documents SET checked_at=? WHERE url=?", (utc_now(), url)
            )
            return Metadata(data, cached["relative_path"])
        assert response.body is not None and response.sha256 is not None
        data = decode_metadata(response.body, url)
        parts = [safe_component(part) for part in urlsplit(url).path.split("/") if part]
        original = Path("metadata", *parts)
        relative = original.with_name(f"{original.stem}.{response.sha256}.json")
        saving = asyncio.create_task(atomic_save(self.settings.output / relative, response.body))
        try:
            await asyncio.shield(saving)
        except asyncio.CancelledError:
            await saving
            raise
        now = utc_now()
        await self.store.transaction(
            [
                (
                    "INSERT INTO metadata_documents VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(url) DO UPDATE SET "
                    "relative_path=excluded.relative_path,sha256=excluded.sha256,etag=excluded.etag,"
                    "last_modified=excluded.last_modified,checked_at=excluded.checked_at",
                    (
                        url,
                        str(relative),
                        response.sha256,
                        response.headers.get("etag"),
                        response.headers.get("last-modified"),
                        now,
                    ),
                    False,
                ),
                (
                    "INSERT OR IGNORE INTO metadata_versions VALUES (?,?,?,?)",
                    (url, response.sha256, str(relative), now),
                    False,
                ),
            ]
        )
        return Metadata(data, str(relative))

    async def discover(self):
        root = self.settings.base_url.rstrip("/")
        config = await self.metadata(f"{root}/oficial/comum/config/ele-c.json")
        self.templates = Templates(root, config.data)
        states = set()
        for election in ELECTIONS:
            directory = self.templates.directory("cm", cd_eleicao=election)
            metadata = await self.metadata(f"{directory}/mun-e{election.zfill(6)}-cm.json")
            for area in metadata.data["abr"]:
                state = state_code(area["cd"])
                if not self.filters.matches_state(state):
                    continue
                states.add(state)
                await self.store.write(
                    "INSERT INTO states VALUES (?,?,?,?) ON CONFLICT(cycle,poll,state_code) "
                    "DO UPDATE SET state_name=excluded.state_name",
                    (CYCLE, POLL, state, area["ds"]),
                )
                municipalities = [
                    (CYCLE, POLL, state, padded(m["cd"], 5), m["nm"], m.get("cdi") or None)
                    for m in area["mu"]
                ]
                await self.store.write(
                    "INSERT INTO municipalities VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(cycle,poll,state_code,municipality_code) DO UPDATE SET "
                    "municipality_name=excluded.municipality_name,"
                    "ibge_code=COALESCE(excluded.ibge_code,municipalities.ibge_code)",
                    rows=municipalities,
                )
        if set(self.filters.states) - states:
            raise DownloadError(
                f"States not found in election coverage: {set(self.filters.states) - states}"
            )
        if not states:
            raise DownloadError("The election configuration contains no matching states")
        self.stage = "sections"
        for state in sorted(states):
            directory = self.templates.directory("cs", uf=state)
            url = f"{directory}/{state}-p{POLL.zfill(6)}-cs.json"
            try:
                document = await self.metadata(url)
                await self.index_sections(state, document.data)
            except (HTTPFailure, DownloadError, KeyError, TypeError, ValueError) as error:
                if isinstance(error, CircuitOpen):
                    raise
                self.errors += 1
                LOG.error("Section configuration failed for %s: %s", state, error)
        where, params = self.filters.sql()
        found = await self.store.query(
            f"SELECT COUNT(*) AS count FROM sections s WHERE {where} AND last_seen_run=?",
            (*params, self.run_id),
        )
        if not found[0]["count"]:
            raise DownloadError(
                "No sections matched the requested filters in the current configuration"
            )

    async def index_sections(self, expected_state: str, data: dict):
        assert self.templates is not None
        if str(data.get("cdp")) != POLL:
            raise DownloadError("Section configuration has an unexpected poll identifier")
        batch = []
        for area in data["abr"]:
            state = state_code(area["cd"])
            if state != expected_state:
                raise DownloadError("Section configuration has an unexpected state")
            for municipality in area["mu"]:
                code = padded(municipality["cd"], 5)
                if self.filters.municipality and code != self.filters.municipality:
                    continue
                await self.store.write(
                    "INSERT INTO municipalities VALUES (?,?,?,?,?,NULL) "
                    "ON CONFLICT(cycle,poll,state_code,municipality_code) DO UPDATE SET "
                    "municipality_name=excluded.municipality_name",
                    (CYCLE, POLL, state, code, municipality["nm"]),
                )
                for zone in municipality["zon"]:
                    zone_code = padded(zone["cd"], 4)
                    if self.filters.zone and zone_code != self.filters.zone:
                        continue
                    sections = {padded(s["ns"], 4): s for s in zone["sec"]}
                    if len(sections) != len(zone["sec"]):
                        raise DownloadError(
                            "Section configuration contains duplicate section codes"
                        )
                    selected = {
                        number
                        for number in sections
                        if self.filters.matches(state, code, zone_code, number)
                    }
                    for number in tuple(selected):
                        principal = padded(sections[number].get("nsp", number), 4)
                        if principal not in sections or sections[principal].get("nsp"):
                            raise DownloadError(
                                f"Invalid principal section for {state}/{code}/{zone_code}/{number}"
                            )
                        selected.add(principal)
                    if not selected:
                        continue
                    await self.store.write(
                        "INSERT OR IGNORE INTO zones VALUES (?,?,?,?,?)",
                        (CYCLE, POLL, state, code, zone_code),
                    )
                    for number in sorted(selected):
                        section = sections[number]
                        principal = padded(section.get("nsp", number), 4)
                        aggregated = principal != number
                        available = bool(section.get("da") and section.get("ha"))
                        status = (
                            "aggregated"
                            if aggregated
                            else "pending"
                            if available
                            else "unpublished"
                        )
                        aux_url = (
                            None
                            if aggregated
                            else self.templates.auxiliary_url(state, code, zone_code, number)
                        )
                        batch.append(
                            (
                                CYCLE,
                                POLL,
                                state,
                                code,
                                zone_code,
                                number,
                                principal,
                                int(aggregated),
                                dump_json([padded(n, 4) for n in section.get("nsa", [])]),
                                section.get("da"),
                                section.get("ha"),
                                aux_url,
                                status,
                                self.run_id,
                            )
                        )
                        if len(batch) >= 2000:
                            await self.store.write(SECTION_UPSERT, rows=batch)
                            batch = []
        if batch:
            await self.store.write(SECTION_UPSERT, rows=batch)

    async def collect_auxiliaries(self):
        self.stage = "auxiliaries"
        source = self.store.pages(
            "SELECT * FROM sections WHERE last_seen_run=? "
            "AND aggregated=0 AND aux_status='pending'",
            (self.run_id,),
        )
        await bounded_map(source, self.collect_auxiliary, self.settings.concurrency)

    async def collect_auxiliary(self, section: dict):
        try:
            document = await self.metadata(section["aux_url"])
            await self.index_auxiliary(section, document)
            self.auxiliaries += 1
        except (HTTPFailure, DownloadError, KeyError, TypeError, ValueError) as error:
            self.errors += 1
            await self.store.write(
                "UPDATE sections SET aux_status='failed',last_error=? WHERE id=?",
                (str(error), section["id"]),
            )
            LOG.error("Auxiliary failed: %s: %s", section["aux_url"], error)
            if isinstance(error, CircuitOpen):
                raise

    async def index_auxiliary(self, section: dict, document: Metadata):
        data = document.data
        receipts = []
        files = []
        for receipt in data["hashes"]:
            receipt_hash = safe_component(receipt["hash"])
            receipts.append(
                (
                    section["id"],
                    receipt_hash,
                    receipt.get("dr"),
                    receipt.get("hr"),
                    receipt.get("st"),
                    self.run_id,
                )
            )
            for file in receipt["arq"]:
                if file["tp"] not in FILE_TYPES:
                    continue
                name = safe_component(file["nm"])
                url = append_url(section["aux_url"].rsplit("/", 1)[0], receipt_hash, name)
                relative = Path(
                    CYCLE,
                    POLL,
                    section["state_code"],
                    section["municipality_code"],
                    section["zone_code"],
                    section["section_code"],
                    receipt_hash,
                    name,
                )
                files.append((section["id"], receipt_hash, file["tp"], name, url, str(relative)))
        await self.store.transaction(
            [
                (RECEIPT_UPSERT, tuple(receipts), True),
                (FILE_UPSERT, tuple(files), True),
                (
                    "UPDATE sections SET aux_status='available',aux_path=?,aux_idg=?,"
                    "section_status=?,"
                    "last_error=NULL WHERE id=?",
                    (document.relative_path, data.get("idg"), data.get("st"), section["id"]),
                    False,
                ),
            ]
        )

    async def collect_files(self):
        self.stage = "files"
        await bounded_map(
            self.store.file_pages(self.filters), self.collect_file, self.settings.concurrency
        )

    async def collect_file(self, file: dict):
        path = self.settings.output / file["relative_path"]
        if file["status"] == "complete" and file["sha256"]:
            try:
                size = await asyncio.to_thread(lambda: path.stat().st_size)
                if size == file["size"]:
                    self.skipped += 1
                    return
            except FileNotFoundError:
                pass
        try:
            response = await self.client.download(file["url"], path)
        except (HTTPFailure, CircuitOpen) as error:
            self.errors += 1
            await self.store.write(
                "UPDATE files SET status='failed',last_error=?,attempts=attempts+? WHERE id=?",
                (str(error), getattr(error, "attempts", 0), file["id"]),
            )
            LOG.error("File failed: %s: %s", file["url"], error)
            if isinstance(error, CircuitOpen):
                raise
            return
        await self.store.write(
            "UPDATE files SET status='complete',size=?,sha256=?,etag=?,last_modified=?,"
            "attempts=attempts+?,last_error=NULL,downloaded_at=? WHERE id=?",
            (
                response.size,
                response.sha256,
                response.headers.get("etag"),
                response.headers.get("last-modified"),
                response.attempts,
                utc_now(),
                file["id"],
            ),
        )
        self.completed += 1

    def run_filters(self) -> str:
        return dump_json(asdict(self.filters))


async def atomic_save(path: Path, body: bytes):
    await asyncio.to_thread(atomic_bytes, path, body)
