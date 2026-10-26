# TSE voting-machine file downloader

Download and index the original voting-machine files published for Brazil's **2026 second round**:

- Poll `3221`, cycle `ele2026`, official environment.
- Federal election `6258` and state election `6260`.
- BU (`bu`), digital vote records (`rdv`), machine logs (`log`), and signatures (`vota`).
- All published receipt hashes, including rejected or superseded receipts, across all states and overseas (`zz`).

The downloader discovers locations through EA11, EA12, and EA16, then obtains exact file names from each EA18 section auxiliary. It does not guess binary file names. Official phase markers `o` and `O` are both accepted; original metadata is saved without normalization. A poll can also contain supplementary elections held on the same day; the archive follows the poll's published section files.

Python 3.12 or newer and [uv](https://docs.astral.sh/uv/) are required. Supported systems: macOS and Linux, including WSL. Code, documentation, and CLI messages are English; original TSE metadata retains its original language.

## Run

From this project directory:

```bash
uv sync --locked
uv run tse-downloader download --output ./data --concurrency 64 --rate-limit 80
```

Without geographic filters, this collects the entire published coverage, including overseas. It can involve millions of files. Use a destination with sufficient storage and filesystem capacity. Actual volume depends on publication and retransmissions; the program does not assume that each section has all four types.

To try one section first:

```bash
uv run tse-downloader download --output ./sample \
  --state ac --municipality 01120 --zone 0008 --section 0008
```

Repeat `--state` to select multiple states. Municipality codes are TSE codes, not IBGE codes. Short numeric codes are padded automatically. Selecting an aggregated section also indexes and downloads its principal section; the aggregated section does not trigger duplicate auxiliary or file requests.

Run the same command again to refresh the configuration and auxiliaries, fetch newly published files, and retry failures. Previously archived receipt hashes remain in the catalog even when they disappear from a newer auxiliary. A previously unpublished primary section is revisited when EA16 supplies its auxiliary publication date and time.

## Output and queries

```text
data/
  index.sqlite3
  indexes/
    states.csv
    municipalities.csv
    zones.csv
    sections.csv
    receipts.csv
    files.csv
  metadata/oficial/.../<original-stem>.<content-sha256>.json
  ele2026/3221/<state>/<municipality>/<zone>/<section>/<receipt-hash>/<original-name>
```

SQLite is the authoritative catalog and resume state. CSV exports use UTF-8, English column names, and the original numeric codes with leading zeros. Import code columns as text in spreadsheet software to keep those zeros.

Tables: `runs`, `states`, `municipalities`, `zones`, `sections`, `receipts`, `files`, `metadata_documents`, and `metadata_versions`. Views:

- `file_index`: physical files with state, municipality, IBGE code, zone, section, receipt, URL, relative path, integrity, and download status.
- `section_index`: all indexed sections with principal section references, publication state, and metadata paths.
- `section_file_index`: maps both principal and aggregated sections to their physical files.

The six CSVs are exported after collection and controlled interruption, including failed runs when storage remains writable. Each CSV is replaced atomically from a consistent SQLite snapshot. On export failure, older CSVs may remain; rerun `export` after resolving the error. JSON source versions are content-addressed and retained.

```bash
uv run tse-downloader status --output ./data
uv run tse-downloader export --output ./data
uv run tse-downloader verify --output ./data
```

`status` is a local catalog report and can run during collection. It does not contact the TSE or check local file contents. It reports published files downloaded separately from all indexed sections available; geographic filters and the last run's outcome are recorded in `runs`. A filtered collection only establishes coverage for that selection.

`verify` streams local files and compares their sizes and SHA-256 values to the recorded downloads. Missing and corrupt files are marked for replacement during the next `download`. Files with no recorded checksum are reported as unverified. Geographic filters also work with `status` and `verify`.

Example queries:

```sql
-- All archived files, with their electoral locations.
SELECT state_code, municipality_name, municipality_code, zone_code, section_code,
       receipt_hash, file_type, status, relative_path
FROM file_index
ORDER BY state_code, municipality_code, zone_code, section_code, receipt_hash, file_type;

-- Published auxiliaries that could not be collected.
SELECT state_code, municipality_code, zone_code, section_code, aux_url, last_error
FROM sections
WHERE aux_status = 'failed';

-- Resolve an aggregated section to its physical files.
SELECT section_code, principal_section_code, file_type, relative_path
FROM section_file_index
WHERE state_code = 'ac' AND municipality_code = '01120'
  AND zone_code = '0008' AND section_code = '0009';
```

## Transfer behavior

- Default: 64 concurrent requests and 80 request starts per second, evenly paced. Metadata, binary files, and retries share the same limiter. Queues, active tasks, database batches, CSV export, and file reads are bounded. EA16 is parsed one state at a time.
- The TSE limit is **100 requests per second per IP**. The CLI rejects higher rates. Use one collection process per public IP or divide that budget between processes and other users sharing the IP. The output lock prevents concurrent writers to the same output directory; it does not coordinate separate directories or machines.
- HTTP 403/429 pauses all new requests for at least ten minutes, or longer if `Retry-After` requires it. Requests already in flight can still finish. Leave the process running during the pause; restarting it does not preserve the in-memory cooldown.
- Network errors, socket timeouts, and HTTP 5xx get up to five retries after the initial attempt, with exponential backoff and jitter. `--retries` and `--timeout` override these defaults.
- HTTP 404 is recorded without immediate retry. Twenty consecutive 404 responses open a circuit breaker and stop the run. Fix the source problem before rerunning.
- Files stream to `.part` files, are flushed and synced, then atomically renamed. Interrupted transfers restart that file from the beginning; completed files are retained. A file renamed before a catalog update may be downloaded again after an abrupt termination.
- Regular resume trusts a completed file when its size matches the catalog. Run `verify` to detect corruption that preserves file size. A receipt hash is an opaque TSE identifier, not an assumed checksum. Files at an unchanged receipt/name are treated as immutable.
- Ctrl+C records an interrupted run, exports the available catalog, and returns exit code 130. An OS-level kill still leaves completed database transactions and atomically published files recoverable. An interrupted `.part` is overwritten on the next transfer.
- Storage and catalog errors stop the run. Free space or correct permissions, then rerun. Original file compression is retained; there is no ASN.1 decoding, extraction, or cryptographic signature verification.

Exit codes: `0` means no operational failures (unpublished sections may remain), `1` means a failure or an unsuccessful integrity check, and `130` means user interruption. Progress is written to stderr every five seconds, with a JSON result on stdout after success or a completed run with recorded download failures.

The source root can be overridden with `--base-url` for an HTTP test mirror. The default always uses the official TSE HTTPS endpoint. Redirects are not followed, so requests cannot bypass the shared limiter or silently change source hosts.

## Development and validation

```bash
uv sync --locked --group dev
uv run pytest -q
uv run ruff check src tests
uv run ruff format --check src tests
```

The test suite uses a local HTTP mirror and a synthetic 500,000-section catalog. It checks exact URLs, all receipt versions, missing types, aggregation, conditional refresh, historical snapshots, incremental publication, resume, checksums, atomic writes, retries, truncation, cancellation, global pacing, shared cooldowns, and the 404 circuit breaker. Tests never start a nationwide download.

## Official references

- [TSE technical information and access limits](https://www.tse.jus.br/eleicoes/informacoes-tecnicas-sobre-a-divulgacao-de-resultados)
- [2026 download instructions](https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/divulgacao-de-resultados/tse-instrucoes-para-download-dos-arquivos-da-divulgacao-2026)
- [EA16 section configuration](https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/divulgacao-de-resultados/tse-ea16-arquivo-de-configuracao-de-secoes-eleitorais)
- [EA18 section auxiliary](https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/divulgacao-de-resultados/tse-ea18-arquivo-auxiliar-de-secao)
