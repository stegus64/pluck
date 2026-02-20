# Agent Instructions for `pluck`

## Purpose
- Data ingestion and warehousing tool in C#/.NET 8.
- Architecture should remain extensible for multiple source and destination types.

## Project Layout
- `Auth/`: authentication helpers.
- `Config/`: YAML loading and config models.
- `Source/`: source readers/querying.
- `Staging/`: staging writers (CSV/parquet) and upload helpers.
- `Target/`: destination loaders/schema managers.
- `Util/`: shared utility helpers.
- Main entry point: `program.cs`.

## Working Rules
- Keep changes minimal and focused to the requested task.
- Follow existing repository style and patterns.
- Do not use local functions; extract private methods or classes instead.
- Avoid nested ternary operators; use explicit `if`/`else`.
- Always log SQL using the standard pattern:
  - `Debug`: compact SQL summary.
  - `Trace`: full SQL text and parameters.
- Prefer streaming over loading full chunk datasets into memory.
- Do not add new frameworks/dependencies without explicit user approval.
- When config options are added/changed (for `connections.yaml` or `streams.yaml`), update `streams-config.md` (and related docs) in the same change.

## Verification
- Run `dotnet build` after changes.
