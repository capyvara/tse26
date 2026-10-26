"""Exercise the actual command-line entry point and OS interruption handling."""

import asyncio
import signal
import sys
from contextlib import closing

from conftest import serve


async def test_ctrl_c_returns_130_and_records_interrupted_run(tmp_path, mirror):
    waiting = asyncio.Event()
    release = asyncio.Event()

    async def slow_binary(request):
        if request.path in mirror.payloads:
            waiting.set()
            await release.wait()
        return await mirror.handler(request)

    async with serve(slow_binary) as url:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "tse_downloader.cli",
            "download",
            "--output",
            str(tmp_path),
            "--base-url",
            url,
            "--state",
            "ac",
            "--concurrency",
            "2",
            "--rate-limit",
            "100",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(waiting.wait(), timeout=15)
            process.send_signal(signal.SIGINT)
            _, error = await asyncio.wait_for(process.communicate(), timeout=15)
            assert process.returncode == 130, error.decode()
            assert b"Rerun the same download command" in error
        finally:
            release.set()
            if process.returncode is None:
                process.kill()
                await process.wait()
    import sqlite3

    with closing(sqlite3.connect(tmp_path / "index.sqlite3")) as connection:
        assert connection.execute("SELECT status FROM runs").fetchone()[0] == "interrupted"
    assert (tmp_path / "indexes" / "files.csv").is_file()
    assert not list(tmp_path.rglob("*.part"))


async def test_cli_rejects_rate_above_cap_before_creating_output(tmp_path):
    output = tmp_path / "should-not-exist"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tse_downloader.cli",
        "download",
        "--output",
        str(output),
        "--rate-limit",
        "101",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, error = await process.communicate()
    assert process.returncode == 1
    assert b"at most 100" in error
    assert not output.exists()
