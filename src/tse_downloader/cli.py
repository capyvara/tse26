"""English command-line interface for collecting and querying the catalog."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from . import __version__
from .app import describe_exception, download, status, verify
from .models import DownloadError, Filters, Settings, padded, state_code
from .store import export_csvs, output_lock


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        description="Download all published 2026 runoff voting-machine files"
    )
    root.add_argument("--version", action="version", version=__version__)
    commands = root.add_subparsers(dest="command", required=True)
    for command in ("download", "status", "export", "verify"):
        child = commands.add_parser(command)
        child.add_argument(
            "--output", type=Path, default=Path("data"), help="Catalog and data directory"
        )
        child.add_argument("--verbose", action="store_true", help="Include retry diagnostics")
        if command != "export":
            child.add_argument(
                "--state", action="append", default=[], help="UF code; repeat for multiple UFs"
            )
            child.add_argument("--municipality", help="TSE municipality code (up to 5 digits)")
            child.add_argument("--zone", help="Electoral zone (up to 4 digits)")
            child.add_argument("--section", help="Electoral section (up to 4 digits)")
        if command == "download":
            child.add_argument(
                "--concurrency",
                type=int,
                default=64,
                help="Maximum concurrent requests (default: 64)",
            )
            child.add_argument(
                "--rate-limit",
                type=float,
                default=80,
                help="Global requests/s, maximum 100 (default: 80)",
            )
            child.add_argument(
                "--retries",
                type=int,
                default=5,
                help="Retries after the initial attempt (default: 5)",
            )
            child.add_argument(
                "--timeout",
                type=float,
                default=120,
                help="Socket read timeout in seconds (default: 120)",
            )
            child.add_argument(
                "--base-url",
                default="https://resultados.tse.jus.br",
                help="Source root or test mirror",
            )
    return root


def main() -> int:
    arguments = parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if arguments.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    output = arguments.output.expanduser().resolve()
    try:
        if arguments.command == "export":
            if not (output / "index.sqlite3").is_file():
                raise DownloadError(f"Catalog not found: {output / 'index.sqlite3'}")
            with output_lock(output):
                result, exit_code = export_csvs(output), 0
        else:
            filters = Filters(
                states=tuple(sorted({state_code(s) for s in arguments.state})),
                municipality=padded(arguments.municipality, 5) if arguments.municipality else None,
                zone=padded(arguments.zone, 4) if arguments.zone else None,
                section=padded(arguments.section, 4) if arguments.section else None,
            )
            if arguments.command == "download":
                settings = Settings(
                    output=output,
                    concurrency=arguments.concurrency,
                    rate_limit=arguments.rate_limit,
                    base_url=arguments.base_url.rstrip("/"),
                    retries=arguments.retries,
                    timeout=arguments.timeout,
                )
                result, exit_code = asyncio.run(download(settings, filters))
            elif arguments.command == "verify":
                result, exit_code = asyncio.run(verify(output, filters))
            else:
                result, exit_code = asyncio.run(status(output, filters)), 0
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return exit_code
    except KeyboardInterrupt:
        print("Interrupted. Rerun the same download command to resume.", file=sys.stderr)
        return 130
    except Exception as error:
        for message in dict.fromkeys(describe_exception(error)):
            print(f"Error: {message}", file=sys.stderr)
        if arguments.verbose:
            logging.exception("Command failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
