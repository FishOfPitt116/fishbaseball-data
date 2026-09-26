# fishbaseball-data

Validated, versioned [Parquet](https://parquet.apache.org/) copies of baseball datasets,
published as GitHub Release assets. A pipeline runs daily in GitHub Actions and publishes only
when the upstream data really changed.

Right now it mirrors one source, the **[Lahman Baseball Database](https://sabr.org/lahman-database/)**
(SABR): 27 tables of batting, pitching, fielding, teams, people, awards and more, 1871 to the
latest season, including Negro Leagues data. Retrosheet is planned to reuse the same pipeline.

This repo has no dependency on the `fishbaseball` library. It publishes files and a manifest,
and anything can consume them.

## Using the data

Everything is under [Releases](../../releases). Two kinds of release exist:

| Release | What it is |
|---|---|
| `lahman-YYYY-MM-DD` | An immutable data release, named for its UTC publish date (`-2`, `-3` if there are several in a day). Contains one `<table>.parquet` per table, `manifest.json`, the raw upstream zip and `NOTICE.md`. |
| `lahman-latest` | A pointer with no data. Its `latest.json` names the current release. |

To find the current data, read `latest.json`, then that release's `manifest.json`:

```
https://github.com/FishOfPitt116/fishbaseball-data/releases/download/lahman-latest/latest.json
```

```json
{"source": "lahman", "latest": "lahman-2026-09-26", "by_schema": {"1": "lahman-2026-09-26"}, ...}
```

`manifest.json` lists every table with its download URL, row count, SHA-256, min/max season and
more. Example, using Polars:

```python
import json, urllib.request, polars as pl

base = "https://github.com/FishOfPitt116/fishbaseball-data/releases/download"
latest = json.load(urllib.request.urlopen(f"{base}/lahman-latest/latest.json"))["latest"]
manifest = json.load(urllib.request.urlopen(f"{base}/{latest}/manifest.json"))
batting = pl.read_parquet(manifest["tables"]["batting"]["url"])
```

Things to know:
- **Column names are snake_case**: `playerID` is `player_id`, `2B` is `doubles`, `IPouts` is
  `ip_outs`. The full mapping from SABR's names is in the manifest's `column_map`.
- **Types are fixed**: counts are `Int32`, ids and codes are strings, `debut` and `final_game` are
  dates, ERA and similar are floats. Blank values are nulls.
- **`schema_version`**: bumped on a breaking change to tables, columns or types. `by_schema` in
  `latest.json` points each schema version at its newest release, so a consumer pinned to an
  older schema keeps working.
- Public download URLs are CDN-cached, so a new pointer can take a few minutes to be visible.

## How the pipeline works

```
detect  ->  build  ->  publish
```

1. **detect**: reads the SABR page for the version and release date, downloads the CSVs, and
   hashes them. If the hash matches the last one seen, it stops. The CSVs are a public Box folder,
   so `pipelines/core/box.py` reads it page by page and zips it deterministically.
2. **build**: reads each CSV, strips byte-order marks, renames columns, casts to the fixed types
   (any bad value fails the run), then runs the checks: unique keys, valid player ids, sane stats
   (hits at most at-bats and so on), a full league in the latest season, and known values such as
   Ruth's 60 home runs in 1927. It also compares against the previous release, so no table or
   column may disappear, row counts may not shrink, and the newest season may not go backwards.
   Output is deterministic Parquet, plus a content hash per table.
3. **publish**: creates the dated release, downloads back a file to verify it, and only then
   moves the `lahman-latest` pointer. A failure before that leaves users on the previous release.

A release is cut when there is a new SABR version, or when table content changed within the same
version. If only the bytes changed (for example SABR fixing byte-order marks), nothing is released.
Published releases are never edited or deleted. A bad one is withdrawn by pointing `latest.json`
back at the previous release.

**Alerts**: any failure opens a GitHub issue (one per stage, commented on if it recurs, closed by
the next successful run). A daily **canary** checks SABR itself: page reachable, version text
parses, download link present, zip downloads, files and headers are as expected. Format changes
alert immediately, and outages after three failed days in a row.

## Repository layout

```
pipelines/
  __main__.py        CLI: python -m pipelines <source> <detect|build|publish|all>
  sources.py         registry of sources (currently just lahman)
  core/              generic and knows nothing about Lahman:
                     config, detect, download, box, convert, validate, versioning,
                     manifest, publish, alerts, canary, pipeline
  lahman/
    tables.py        CSV file name -> table name
    columns.py       source column -> snake_case column, per table (explicit, no guessing)
    schema.py        dtypes, primary keys, and the Lahman-specific checks
tests/
  unit/              fast tests with fixtures and fakes (no network)
  integration/       hits the live SABR site
  fixtures/          trimmed real Lahman CSV zips, saved pages, and a sample manifest
.github/workflows/
  ci.yml             lint, type check and unit tests on every push
  publish-lahman.yml daily at 13:17 UTC, also runnable by hand
  canary-upstream.yml daily upstream health checks
```

## Working on it

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/pytest tests/unit              # unit tests
.venv/bin/pytest tests/integration       # live SABR checks (takes ~1 min)
.venv/bin/ruff check . && .venv/bin/pyright
```

Run the whole pipeline locally without publishing anything:

```bash
.venv/bin/python -m pipelines lahman all --dry-run --out build/
```

Useful options: `--source-url` (a zip URL or local zip path, replacing the Box download),
`--force` (skip the "unchanged" shortcut), `--dry-run`, and `--out DIR`. The stages can also be
run one at a time, each reading the previous stage's output from `--out`.

To run it for real, use **Actions > publish-lahman > Run workflow**.

**When SABR adds a table or column**, the build fails on purpose with a message saying so. Add it
to `tables.py`, `columns.py` and `schema.py`, and the next run publishes it.

## Licenses

- **Code**: MIT (see `LICENSE`).
- **Data**: the Lahman Baseball Database is copyright SABR, via Sean Lahman, under
  [CC BY-SA 3.0](http://creativecommons.org/licenses/by-sa/3.0/). Derived Parquet files carry the
  same license, and every release includes a `NOTICE.md` with the attribution. Negro Leagues data
  is licensed to SABR by Seamheads.com.
