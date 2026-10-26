"""Shared HTTP concurrency, pacing, retries, and atomic streaming downloads."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import aiohttp

from .models import CircuitOpen, DownloadError, Settings

LOG = logging.getLogger(__name__)


class HTTPFailure(DownloadError):
    def __init__(self, message: str, status: int | None = None, attempts: int = 1):
        super().__init__(message)
        self.status = status
        self.attempts = attempts


async def finish_io(function, *args, **kwargs):
    """Finish a short filesystem operation before propagating cancellation."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


class RateLimiter:
    """Space request starts evenly and apply server cooldowns to all workers."""

    def __init__(self, rate: float):
        self.interval = 1 / rate
        self.next_start = 0.0
        self.paused_until = 0.0
        self.lock = asyncio.Lock()

    def pause(self, seconds: float):
        self.paused_until = max(self.paused_until, time.monotonic() + seconds)

    async def acquire(self):
        async with self.lock:
            while True:
                delay = max(self.next_start, self.paused_until) - time.monotonic()
                if delay <= 0:
                    self.next_start = time.monotonic() + self.interval
                    return
                await asyncio.sleep(delay)


def cooldown_seconds(value: str | None) -> float:
    if value:
        try:
            return max(600.0, float(value))
        except ValueError:
            try:
                date = parsedate_to_datetime(value)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=UTC)
                return max(600.0, (date - datetime.now(UTC)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                pass
    return 600.0


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes | None = None
    size: int = 0
    sha256: str | None = None
    attempts: int = 1


class HTTPClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.limiter = RateLimiter(settings.rate_limit)
        self.semaphore = asyncio.Semaphore(settings.concurrency)
        self.consecutive_not_found = 0
        self.stopped = False
        self.requests = 0
        self.bytes_received = 0
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.settings.concurrency),
            timeout=aiohttp.ClientTimeout(
                total=None, connect=min(30, self.settings.timeout), sock_read=self.settings.timeout
            ),
            headers={"User-Agent": "tse-downloader/0.1.0", "Accept-Encoding": "identity"},
            auto_decompress=False,
            trust_env=True,
        )
        return self

    async def __aexit__(self, *_):
        if self.session:
            await self.session.close()

    async def get(self, url: str, headers: dict | None = None) -> Response:
        return await self._fetch(url, headers or {}, None)

    async def download(self, url: str, destination: Path) -> Response:
        return await self._fetch(url, {}, destination)

    async def _consume(
        self, response: aiohttp.ClientResponse, destination: Path | None
    ) -> Response:
        result = Response(
            response.status, {key.lower(): value for key, value in response.headers.items()}
        )
        if response.status == 304:
            return result
        digest = hashlib.sha256()
        body = bytearray()
        stream: Any = None
        temporary = destination.with_name(destination.name + ".part") if destination else None
        try:
            if destination:
                await finish_io(destination.parent.mkdir, parents=True, exist_ok=True)
                opening = asyncio.create_task(asyncio.to_thread(temporary.open, "wb"))
                try:
                    stream = await asyncio.shield(opening)
                except asyncio.CancelledError:
                    stream = await opening
                    raise
            async for chunk in response.content.iter_chunked(256 * 1024):
                self.bytes_received += len(chunk)
                result.size += len(chunk)
                digest.update(chunk)
                if stream:
                    await finish_io(stream.write, chunk)
                else:
                    if result.size > 64 * 1024 * 1024:
                        raise DownloadError("Metadata document exceeds the 64 MiB size limit")
                    body.extend(chunk)
            if response.content_length is not None and response.content_length != result.size:
                raise aiohttp.ClientPayloadError("Response body does not match Content-Length")
            if result.size == 0:
                raise aiohttp.ClientPayloadError("Received an empty response body")
            if stream:
                await finish_io(stream.flush)
                await finish_io(os.fsync, stream.fileno())
                await finish_io(stream.close)
                stream = None
                await finish_io(os.replace, temporary, destination)
            else:
                result.body = bytes(body)
            result.sha256 = digest.hexdigest()
            return result
        finally:
            if stream:
                await finish_io(stream.close)
            if temporary:
                await finish_io(temporary.unlink, missing_ok=True)

    async def _fetch(self, url: str, headers: dict, destination: Path | None) -> Response:
        assert self.session is not None
        for attempt in range(self.settings.retries + 1):
            blocked = False
            try:
                async with self.semaphore:
                    if self.stopped:
                        raise CircuitOpen(
                            "HTTP collection stopped after 20 consecutive 404 responses"
                        )
                    await self.limiter.acquire()
                    if self.stopped:
                        raise CircuitOpen(
                            "HTTP collection stopped after 20 consecutive 404 responses"
                        )
                    self.requests += 1
                    async with self.session.get(
                        url, headers=headers, allow_redirects=False
                    ) as response:
                        status = response.status
                        if status == 404:
                            self.consecutive_not_found += 1
                            if self.consecutive_not_found >= 20:
                                self.stopped = True
                                raise CircuitOpen(
                                    "20 consecutive HTTP 404 responses; "
                                    "stopped to avoid an IP block. "
                                    "Check the source URLs and rerun later."
                                )
                        else:
                            self.consecutive_not_found = 0
                        if status in {403, 429}:
                            delay = cooldown_seconds(response.headers.get("Retry-After"))
                            self.limiter.pause(delay)
                            blocked = True
                            LOG.warning(
                                "HTTP %s; pausing all requests for at least %.0f seconds",
                                status,
                                delay,
                            )
                        if status not in {200, 304}:
                            raise HTTPFailure(f"HTTP {status}: {url}", status, attempt + 1)
                        if destination and status == 304:
                            raise HTTPFailure(
                                f"Unexpected HTTP 304 for a file download: {url}", status
                            )
                        if "text/html" in response.headers.get("Content-Type", "").lower():
                            raise HTTPFailure(
                                f"Unexpected HTML response: {url}", status, attempt + 1
                            )
                        result = await self._consume(response, destination)
                        result.attempts = attempt + 1
                        return result
            except HTTPFailure as error:
                retryable = error.status in {403, 429} or (
                    error.status is not None and error.status >= 500
                )
                if not retryable or attempt == self.settings.retries:
                    raise
                LOG.debug("Retryable HTTP error: %s", error)
            except (aiohttp.ClientError, TimeoutError) as error:
                if attempt == self.settings.retries:
                    raise HTTPFailure(
                        f"Request failed: {url}: {error}", attempts=attempt + 1
                    ) from error
                LOG.debug("Retryable network error for %s: %s", url, error)
            if not blocked:
                await asyncio.sleep(min(60, 2**attempt) + random.uniform(0, 0.5))
        raise AssertionError("Retry loop exhausted without a result")
