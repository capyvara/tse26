"""Election identifiers, input validation, and bounded pipeline helpers."""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar
from urllib.parse import quote, urlsplit

CYCLE = "ele2026"
POLL = "3221"
ELECTIONS = ("6258", "6260")
FILE_TYPES = frozenset({"bu", "rdv", "log", "vota"})
T = TypeVar("T")


class DownloadError(Exception):
    """An actionable collection or input error."""


class CircuitOpen(DownloadError):
    """Stop issuing requests after a potentially dangerous sequence of errors."""


def is_official_metadata(data: object) -> bool:
    """TSE publishes the official phase as both lowercase and uppercase O."""
    return isinstance(data, dict) and data.get("f") in ("o", "O")


def padded(value: str, width: int) -> str:
    if not re.fullmatch(r"[0-9]{1," + str(width) + r"}", str(value)):
        raise DownloadError(f"Expected a numeric code with at most {width} digits: {value!r}")
    return str(value).zfill(width)


def safe_component(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", value):
        raise DownloadError(f"Unsafe URL or path component: {value!r}")
    return value


def state_code(value: str) -> str:
    value = value.lower()
    if not re.fullmatch(r"[a-z]{2}", value) or value == "br":
        raise DownloadError(f"Invalid state code: {value!r}")
    return value


@dataclass(frozen=True)
class Filters:
    states: tuple[str, ...] = ()
    municipality: str | None = None
    zone: str | None = None
    section: str | None = None

    def matches_state(self, value: str) -> bool:
        return not self.states or value in self.states

    def matches(self, state: str, municipality: str, zone: str, section: str) -> bool:
        return (
            self.matches_state(state)
            and (self.municipality is None or municipality == self.municipality)
            and (self.zone is None or zone == self.zone)
            and (self.section is None or section == self.section)
        )

    def sql(self, alias: str = "s") -> tuple[str, tuple]:
        clauses = [f"{alias}.cycle = ?", f"{alias}.poll = ?"]
        params: list[str] = [CYCLE, POLL]
        if self.states:
            clauses.append(f"{alias}.state_code IN ({','.join('?' for _ in self.states)})")
            params.extend(self.states)
        for column, value in (
            ("municipality_code", self.municipality),
            ("zone_code", self.zone),
            ("section_code", self.section),
        ):
            if value is not None:
                clauses.append(f"{alias}.{column} = ?")
                params.append(value)
        return " AND ".join(clauses), tuple(params)


@dataclass(frozen=True)
class Settings:
    output: Path
    concurrency: int = 64
    rate_limit: float = 80
    base_url: str = "https://resultados.tse.jus.br"
    retries: int = 5
    timeout: float = 120

    def __post_init__(self):
        if not 1 <= self.concurrency <= 1024:
            raise DownloadError("Concurrency must be between 1 and 1024")
        if not 0 < self.rate_limit <= 100:
            raise DownloadError("Rate limit must be greater than zero and at most 100 requests/s")
        if self.timeout <= 0 or self.retries < 0:
            raise DownloadError("Timeout must be positive and retries must not be negative")
        parsed = urlsplit(self.base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
        ):
            raise DownloadError("Base URL must be an HTTP(S) URL without query or fragment")


class Templates:
    def __init__(self, base_url: str, config: dict):
        polls = [p for p in config["pl"] if p["cd"] == POLL and p["c"] == CYCLE]
        if len(polls) != 1 or polls[0].get("dt") != "25/10/2026":
            raise DownloadError("The configuration does not describe the expected 2026 runoff poll")
        elections = {e["cd"] for e in polls[0]["e"] if e.get("t") == "2"}
        if not set(ELECTIONS) <= elections or not is_official_metadata(config):
            raise DownloadError("Official second-round elections 6258 and 6260 are required")
        self.base_url = base_url.rstrip("/")
        self.paths = {entry["tp"]: entry["dir"] for entry in config["arq"]}
        if not {"cm", "cs", "aux"} <= self.paths.keys():
            raise DownloadError("The configuration is missing cm, cs, or aux directory templates")

    def directory(self, kind: str, **values: str) -> str:
        replacements = {
            "base": self.base_url,
            "ambiente": "oficial",
            "ciclo": CYCLE,
            "cd_pleito": POLL,
            **values,
        }

        def replace(match):
            name = match.group(1)
            if name not in replacements:
                raise DownloadError(f"Unsupported or missing template token: {name}")
            return replacements[name]

        url = re.sub(r"<([^>]+)>", replace, self.paths[kind]).rstrip("/")
        parsed = urlsplit(url)
        base = urlsplit(self.base_url)
        if (
            parsed.scheme != base.scheme
            or parsed.netloc != base.netloc
            or parsed.query
            or parsed.fragment
            or not parsed.path.startswith(base.path.rstrip("/") + "/oficial/")
            or any(part in {".", ".."} or "%" in part for part in parsed.path.split("/"))
        ):
            raise DownloadError(f"Unsafe directory template: {url}")
        return url

    def auxiliary_url(self, state: str, municipality: str, zone: str, section: str) -> str:
        directory = self.directory(
            "aux", uf=state, municipio=municipality, zona=zone, secao=section
        )
        return f"{directory}/p{POLL.zfill(6)}-{state}-m{municipality}-z{zone}-s{section}-aux.json"


def append_url(directory: str, *parts: str) -> str:
    return directory.rstrip("/") + "/" + "/".join(quote(safe_component(p)) for p in parts)


async def bounded_map[T](
    source: AsyncIterable[T], worker: Callable[[T], Awaitable[None]], concurrency: int
) -> None:
    """Limit both running tasks and queued work, including for very large sources."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency * 2)
    sentinel = object()

    async def produce():
        try:
            async for item in source:
                await queue.put(item)
            for _ in range(concurrency):
                await queue.put(sentinel)
        finally:
            close = getattr(source, "aclose", None)
            if close:
                await close()

    async def consume():
        while True:
            item = await queue.get()
            try:
                if item is sentinel:
                    return
                await worker(item)
            finally:
                queue.task_done()

    async with asyncio.TaskGroup() as group:
        group.create_task(produce())
        for _ in range(concurrency):
            group.create_task(consume())
