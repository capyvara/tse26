"""URL correctness, input validation, and a 500,000-section bounded workload."""

import asyncio
import json

import pytest

from tse_downloader.collector import SECTION_UPSERT, decode_metadata
from tse_downloader.models import (
    DownloadError,
    Filters,
    Settings,
    Templates,
    append_url,
    bounded_map,
    padded,
    safe_component,
)
from tse_downloader.store import Store


@pytest.mark.parametrize("phase", ["o", "O"])
def test_official_metadata_accepts_both_phase_cases_without_changing_source(phase):
    document = {"f": phase, "st": "Recebida", "hashes": []}
    assert (
        decode_metadata(json.dumps(document).encode(), "https://example.invalid/aux.json")
        == document
    )


@pytest.mark.parametrize("phase", ["s", "S", "official", "", None, 0, [], {}])
def test_metadata_rejects_simulation_or_invalid_phases(phase):
    with pytest.raises(DownloadError, match="received"):
        decode_metadata(json.dumps({"f": phase}).encode(), "https://example.invalid/aux.json")


@pytest.mark.parametrize("phase", ["o", "O"])
def test_election_configuration_accepts_both_official_phase_cases(mirror, phase):
    mirror.config["f"] = phase
    Templates(mirror.url, mirror.config)


def test_padding_templates_and_file_names(mirror):
    templates = Templates(mirror.url, mirror.config)
    url = templates.auxiliary_url("ac", padded("1120", 5), padded("8", 4), padded("8", 4))
    assert url.endswith("/ac/01120/0008/0008/p003221-ac-m01120-z0008-s0008-aux.json")
    assert "/custom-urns/" in url  # Directory templates are authoritative.
    assert append_url(url.rsplit("/", 1)[0], "hash", "original-bu.dat").endswith(
        "/hash/original-bu.dat"
    )


@pytest.mark.parametrize(
    "value", ["../escape", "name/path", "name\\path", "%2fescape", "", ".", ".."]
)
def test_reject_unsafe_components(value):
    with pytest.raises(DownloadError):
        safe_component(value)


@pytest.mark.parametrize("rate", [0, -1, 101, float("nan")])
def test_enforce_official_rate_cap(tmp_path, rate):
    with pytest.raises(DownloadError):
        Settings(tmp_path, rate_limit=rate)


async def test_reject_wrong_poll_or_untrusted_template(mirror):
    mirror.config["pl"][0]["e"][0]["t"] = "1"
    with pytest.raises(DownloadError, match="second-round"):
        Templates(mirror.url, mirror.config)
    mirror.config["pl"][0]["e"][0]["t"] = "2"
    mirror.config["arq"][0]["dir"] = "https://another-host.example/config"
    with pytest.raises(DownloadError, match="Unsafe"):
        Templates(mirror.url, mirror.config).directory("cm", cd_eleicao="6258")


async def test_500000_sections_bounded_pipeline_and_catalog(tmp_path):
    total = 500_000
    concurrency = 8
    produced = 0
    completed = 0
    peak_outstanding = 0
    peak_tasks = 0

    async def source():
        nonlocal produced, peak_outstanding
        for number in range(total):
            produced += 1
            peak_outstanding = max(peak_outstanding, produced - completed)
            yield number

    async def worker(number):
        nonlocal completed, peak_tasks
        if number % 10_000 == 0:
            peak_tasks = max(peak_tasks, len(asyncio.all_tasks()))
        await asyncio.sleep(0)
        completed += 1

    await bounded_map(source(), worker, concurrency)
    assert completed == total
    assert peak_outstanding <= concurrency * 3 + 1
    assert peak_tasks < concurrency + 10
    async with Store(tmp_path) as store:
        for offset in range(0, total, 2000):
            await store.write(
                SECTION_UPSERT,
                rows=(
                    (
                        "ele2026",
                        "3221",
                        "ac",
                        "01120",
                        f"{number // 1000 + 1:04d}",
                        f"{number % 1000 + 1:04d}",
                        f"{number % 1000 + 1:04d}",
                        0,
                        "[]",
                        None,
                        None,
                        None,
                        "unpublished",
                        "scale-run",
                    )
                    for number in range(offset, offset + 2000)
                ),
            )
        count = (await store.query("SELECT COUNT(*) AS count FROM sections"))[0]["count"]
        assert count == total
        # Read all rows in bounded pages and verify that each principal resolves correctly.
        seen = 0
        async for section in store.pages("SELECT id,principal_section_id FROM section_index"):
            assert section["id"] == section["principal_section_id"]
            seen += 1
        assert seen == total
        await store.write(
            "INSERT INTO receipts(id,section_id,receipt_hash,last_seen_run) "
            "SELECT id,id,'scale-hash','scale-run' FROM sections"
        )
        await store.write(
            "INSERT INTO files(id,receipt_id,file_type,name,url,relative_path) "
            "SELECT id,id,'bu','file.dat','https://example.invalid/'||id,'data/'||id FROM sections"
        )
        seen_files = 0
        async for file in store.file_pages(Filters()):
            assert file["id"] == file["receipt_id"]
            seen_files += 1
        assert seen_files == total
        assert (await store.query("PRAGMA foreign_key_check")) == []
