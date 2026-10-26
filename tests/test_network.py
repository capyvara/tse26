"""Exercise shared pacing, retries, cooldowns, atomicity, and cancellation over HTTP."""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest
from aiohttp import web
from conftest import serve

from tse_downloader.models import CircuitOpen, Settings
from tse_downloader.network import HTTPClient, HTTPFailure, cooldown_seconds


async def test_global_pacing_concurrency_and_retry(tmp_path):
    starts = []
    active = 0
    peak = 0
    failed = False

    async def handler(request):
        nonlocal active, peak, failed
        starts.append(time.monotonic())
        active += 1
        peak = max(active, peak)
        try:
            await asyncio.sleep(0.09)
            if request.path == "/retry" and not failed:
                failed = True
                return web.Response(status=503)
            return web.Response(body=b"payload")
        finally:
            active -= 1

    async with serve(handler) as url:
        async with HTTPClient(
            Settings(tmp_path, concurrency=3, rate_limit=50, retries=1)
        ) as client:
            await asyncio.gather(
                client.get(url + "/retry"),
                *[client.download(url + f"/file-{i}", tmp_path / str(i)) for i in range(7)],
            )
            assert client.requests == 9
    assert 1 < peak <= 3
    assert all(later - earlier >= 0.015 for earlier, later in zip(starts, starts[1:], strict=False))


@pytest.mark.parametrize("response_status", [403, 429])
async def test_cooldown_is_shared_by_metadata_files_and_retries(
    tmp_path, monkeypatch, response_status
):
    monkeypatch.setattr("tse_downloader.network.cooldown_seconds", lambda _: 0.15)
    starts = []

    async def handler(request):
        starts.append((request.path, time.monotonic()))
        if len(starts) == 1:
            return web.Response(status=response_status, headers={"Retry-After": "600"})
        return web.Response(body=b"payload")

    async with serve(handler) as url:
        async with HTTPClient(
            Settings(tmp_path, concurrency=4, rate_limit=100, retries=1)
        ) as client:
            paused = asyncio.Event()
            original_pause = client.limiter.pause

            def record_pause(seconds):
                original_pause(seconds)
                paused.set()

            monkeypatch.setattr(client.limiter, "pause", record_pause)
            first = asyncio.create_task(client.get(url + "/blocked"))
            await paused.wait()
            await asyncio.gather(first, client.download(url + "/file", tmp_path / "payload"))
    assert len(starts) == 3
    assert all(start - starts[0][1] >= 0.14 for _, start in starts[1:])


def test_production_cooldown_is_at_least_ten_minutes():
    assert cooldown_seconds(None) == 600
    assert cooldown_seconds("0") == 600
    assert cooldown_seconds("invalid") == 600
    assert cooldown_seconds("1200") == 1200
    future = format_datetime(datetime.now(UTC) + timedelta(seconds=1800))
    assert 1790 <= cooldown_seconds(future) <= 1800


async def test_twenty_404_responses_open_circuit_without_retry(tmp_path):
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        return web.Response(status=404)

    async with serve(handler) as url:
        async with HTTPClient(Settings(tmp_path, rate_limit=100, retries=5)) as client:
            for _ in range(19):
                with pytest.raises(HTTPFailure) as error:
                    await client.get(url)
                assert error.value.status == 404
            with pytest.raises(CircuitOpen, match="20 consecutive"):
                await client.get(url)
            with pytest.raises(CircuitOpen):
                await client.get(url)
    assert calls == 20


async def test_truncated_transfer_retries_atomically(tmp_path):
    attempts = 0
    payload = b"original compressed bytes\x00\xff" * 100
    destination = tmp_path / "file.dat"
    destination.write_bytes(b"previous version")

    async def handler(request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            response = web.StreamResponse(headers={"Content-Length": str(len(payload))})
            await response.prepare(request)
            await response.write(payload[:5])
            request.transport.close()
            return response
        assert destination.read_bytes() == b"previous version"
        return web.Response(body=payload)

    async with serve(handler) as url:
        async with HTTPClient(Settings(tmp_path, rate_limit=100, retries=1)) as client:
            result = await client.download(url, destination)
    assert result.attempts == 2 and attempts == 2
    assert destination.read_bytes() == payload
    assert not list(tmp_path.glob("*.part"))


async def test_timeout_retries_and_cleans_up(tmp_path):
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.1)
        return web.Response(body=b"too late")

    async with serve(handler) as url:
        async with HTTPClient(
            Settings(tmp_path, rate_limit=100, retries=1, timeout=0.02)
        ) as client:
            with pytest.raises(HTTPFailure) as error:
                await client.download(url, tmp_path / "file")
    assert error.value.attempts == 2 and calls == 2
    assert not (tmp_path / "file").exists()
    assert not list(tmp_path.glob("*.part"))


async def test_cancel_stream_does_not_publish_partial_file(tmp_path):
    sent = asyncio.Event()
    release = asyncio.Event()

    async def handler(request):
        response = web.StreamResponse()
        await response.prepare(request)
        await response.write(b"partial")
        sent.set()
        await release.wait()
        return response

    async with serve(handler) as url:
        async with HTTPClient(Settings(tmp_path, rate_limit=100)) as client:
            task = asyncio.create_task(client.download(url, tmp_path / "file"))
            await sent.wait()
            # Wait for the client's asynchronous file open/write before cancellation.
            for _ in range(100):
                if (tmp_path / "file.part").exists():
                    break
                await asyncio.sleep(0.001)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
    assert not (tmp_path / "file").exists()
    assert not list(tmp_path.glob("*.part"))
