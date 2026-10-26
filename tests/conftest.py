"""A local TSE mirror with mutable publication state and request accounting."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from contextlib import asynccontextmanager

import pytest
from aiohttp import web


@asynccontextmanager
async def serve(handler):
    application = web.Application()
    application.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


class Mirror:
    def __init__(self):
        self.documents = {}
        self.requests = Counter()
        self.not_modified = Counter()
        self.payloads = {}
        self.config = {
            "f": "o",
            "arq": [
                {"tp": "cm", "dir": "<base>/<ambiente>/<ciclo>/<cd_eleicao>/config"},
                {"tp": "cs", "dir": "<base>/<ambiente>/<ciclo>/custom-urns/config/<uf>"},
                {
                    "tp": "aux",
                    "dir": (
                        "<base>/<ambiente>/<ciclo>/custom-urns/data/<uf>/<municipio>/<zona>/<secao>"
                    ),
                },
            ],
            "pl": [
                {
                    "cd": "3221",
                    "c": "ele2026",
                    "dt": "25/10/2026",
                    "e": [
                        {"cd": "6258", "t": "2"},
                        {"cd": "6260", "t": "2"},
                    ],
                }
            ],
        }
        self.documents["/oficial/comum/config/ele-c.json"] = self.config
        for election in ("6258", "6260"):
            self.documents[f"/oficial/ele2026/{election}/config/mun-e00{election}-cm.json"] = {
                "f": "o",
                "abr": [
                    {
                        "cd": state,
                        "ds": name,
                        "mu": [
                            {"cd": "01120", "nm": "ACRELÂNDIA", "cdi": "1200013"},
                        ],
                    }
                    for state, name in (("ac", "ACRE"), ("zz", "EXTERIOR"))
                ],
            }
        for state in ("ac", "zz"):
            self.documents[
                f"/oficial/ele2026/custom-urns/config/{state}/{state}-p003221-cs.json"
            ] = {
                "f": "o",
                "cdp": "3221",
                "abr": [
                    {
                        "cd": state,
                        "mu": [
                            {
                                "cd": "01120",
                                "nm": "ACRELÂNDIA",
                                "zon": [
                                    {
                                        "cd": "0008",
                                        "sec": [
                                            {
                                                "ns": "0008",
                                                "nsa": ["0009"],
                                                "da": "25/10/2026",
                                                "ha": "18:00:00",
                                            },
                                            {"ns": "0009", "nsp": "0008"},
                                            {"ns": "0010"},
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
            auxiliary = {"f": "o", "idg": "1", "st": "Totalizada", "hashes": []}
            self.documents[self.aux_path(state)] = auxiliary
            self.add_receipt(state, "hash-one", ("bu", "rdv", "log", "vota"))
            self.add_receipt(state, "hash-two", ("bu",))

    def aux_path(self, state="ac"):
        return (
            f"/oficial/ele2026/custom-urns/data/{state}/01120/0008/0008/"
            f"p003221-{state}-m01120-z0008-s0008-aux.json"
        )

    def add_receipt(self, state, receipt_hash, types):
        names = {"bu": "bu.dat", "rdv": "rdv.dat", "log": "log.jez", "vota": "vota.vsc"}
        files = []
        for file_type in types:
            name = f"o03221{state}0112000080008-{names[file_type]}"
            files.append({"tp": file_type, "nm": name})
            path = self.aux_path(state).rsplit("/", 1)[0] + f"/{receipt_hash}/{name}"
            self.documents[path] = f"original-{state}-{receipt_hash}-{file_type}".encode()
            self.payloads[path] = self.documents[path]
        self.documents[self.aux_path(state)]["hashes"].append(
            {
                "hash": receipt_hash,
                "dr": "25/10/2026",
                "hr": "18:00:00",
                "st": "Rejeitado" if receipt_hash == "hash-two" else "Totalizado",
                "arq": files,
            }
        )

    async def handler(self, request):
        self.requests[request.path] += 1
        document = self.documents.get(request.path)
        if document is None:
            return web.Response(status=404)
        body = json.dumps(document).encode() if isinstance(document, dict) else document
        etag = '"' + hashlib.sha256(body).hexdigest() + '"'
        if request.headers.get("If-None-Match") == etag:
            self.not_modified[request.path] += 1
            return web.Response(status=304, headers={"ETag": etag})
        return web.Response(
            body=body,
            headers={"ETag": etag},
            content_type="application/json"
            if isinstance(document, dict)
            else "application/octet-stream",
        )


@pytest.fixture
async def mirror():
    fixture = Mirror()
    async with serve(fixture.handler) as url:
        fixture.url = url
        yield fixture
