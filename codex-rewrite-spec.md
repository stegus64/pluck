# Pluck Rewrite Specification for Codex (New Repository)

This document is the implementation spec for rebuilding `pluck` with cleaner architecture. It integrates:
- current observed functionality
- desired architecture from `architecture-suggestion.md`
- explicit requirement that configuration remains split into `connections.yaml` and `streams.yaml`

## 1. Product Intent

Build a .NET 8 console app named `pluck` for ELT-style incremental loading.

Primary goals:
- Keep behavior understandable and predictable.
- Avoid NxM orchestration complexity in a monolithic Program class.
- Keep connectors extensible through interfaces and factory registration.
- Preserve configuration split:
  - `connections.yaml` contains environment + connection details and may contain secrets.
  - `streams.yaml` contains run definition only and must not contain secrets.

## 2. Scope and Versioning

### 2.1 V1 required (must implement)
- Source: SQL Server
- Destination: SQL Server
- Incremental mode: watermark (chunked by update-key interval)
- Update strategies:
  - `replace_all`
  - `append`
  - `upsert`
- Stream-level parallelism with configurable max concurrency
- Ordered chunk commits within each stream
- Planning phase + execution phase separation
- Structured logging + per-stream summary
- Graceful cancellation on Ctrl+C
- Unit tests for interval parsing + chunk plan generation

### 2.2 V1 scaffolding required (no full implementation)
- Incremental mode abstraction with:
  - `watermark` (implemented)
  - `sqlserver_change_tracking` (scaffold only)
- Incremental state store abstraction (for CT high-water/state)
- Target/source connector abstractions for additional systems

### 2.3 V2-ready extension points (not required now)
- SQL Server -> Fabric Warehouse target path
- delete detection (`subset`) soft-delete flow
- BigQuery source

## 3. Non-Negotiable Configuration Model

## 3.1 Two files (mandatory)
- `connections.yaml`
- `streams.yaml`

Do not collapse into one file.

## 3.2 Secret boundary
- `connections.yaml` may contain secrets (connection strings, app secrets, credentials).
- `streams.yaml` must not contain secrets.

## 3.3 CLI must support explicit file paths
- `--connections-file <path>`
- `--streams-file <path>`
- `--env <name>` to select environment in `connections.yaml`

## 4. High-Level Architecture

Use clear layers and small interfaces.

Suggested modules:
- `Config`:
  - YAML loading
  - schema validation
  - defaults + stream resolution
- `Planning`:
  - planner orchestration
  - chunk interval parsing
  - chunk spec generation
- `Execution`:
  - stream scheduler with bounded concurrency
  - per-stream chunk executor enforcing ordered commits
- `Connectors`:
  - source connectors
  - target connectors
  - registry/factory by `type`
- `Incremental`:
  - incremental mode strategy abstraction
  - state store abstraction
- `Observability`:
  - logging helpers
  - summary models

No runtime plugin discovery or assembly scanning.
Use compiled registrations keyed by connector `type`.

## 5. Core Interfaces (must exist)

- `ISourceConnectorFactory`
- `ITargetConnectorFactory`
- `ISourceConnector`
- `IRangedSourceReader`
  - `GetSourceRangeAsync(watermark)` => `(min, max)`
  - `ReadChunkAsync(lowerInclusive, upperExclusive)` => streaming reader
- `ITargetConnector`
  - `GetWatermarkAsync(updateKeyColumn)`
  - `PrepareAsync(strategy)`
  - `WriteChunkAsync(dataReader)`
  - `CompleteAsync()`
- `IIncrementalPlanner`
  - `BuildPlanAsync(streamConfig, sourceReader, targetConnector)` => execution plan
- `IIncrementalStateStore` (CT scaffolding)

## 6. Execution Model

## 6.1 Planning phase
For each selected stream:
- Validate stream + referenced connections.
- Resolve source/target connector implementations by `type`.
- Build stream execution plan.
- For watermark mode, create ordered `ChunkSpec` list:
  - `ChunkId`
  - `LowerBoundInclusive`
  - `UpperBoundExclusive`

## 6.2 Execution phase
- Run streams concurrently up to `maxParallelStreams`.
- Within one stream:
  - chunk extraction/preparation may be pipelined,
  - but target commit/write order must follow `ChunkId` order.
  - Avoid loading a full chunk into memory. Prefer streaming data either directly to target or an intermediate temporary file

## 6.3 Failure and cancellation
- If one stream fails, it is marked failed and reported.
- Support `--failfast` to cancel remaining work after first stream failure.
- Ctrl+C stops execution a s soon as possible

## 7. Watermark + Chunking Semantics (V1)

Per stream config:
- `update_key`
- `update_key_type`: `numeric` | `datetime`
- `chunk_size`

Algorithm semantics:
1. Watermark = highest `update_key` in target (`MAX(update_key)`), or null if target empty.
2. Source range = `(min, max)` where source `update_key > watermark` (or all rows if watermark null).
3. If no range, stream has no chunks.
4. Else iterate half-open chunks:
   - `[lower, lower + chunk_size)`
   - `lower += chunk_size`
   - stop when `lower > max`

Supported duration suffixes for datetime chunk size:
- `s`, `m`, `h`, `d`, `y`

Use parameterized SQL for bounds.

## 8. V1 SQL Server -> SQL Server Behavior

- Source extraction via `SqlConnection` / `SqlCommand`.
- Target load via `SqlBulkCopy`.
- Stream rows from source to target load path; avoid full in-memory chunk materialization unless required.

Update strategies:
- `replace_all`
  - before loading eligible dataset, clear target table (`TRUNCATE` if allowed, else equivalent safe delete strategy)
- `append`
  - only insert rows from selected incremental chunks
- `upsert`
  - use a primary_key to either update or insert new data

## 9. Change Tracking Design Contract (Scaffold)

Incremental mode enum must include:
- `watermark`
- `sqlserver_change_tracking`

For CT mode contract (future):
- Delta runs are not chunked.
- If first CT run (no state), do initial full load via watermark path, then initialize CT state.

V1 only needs:
- mode parsing + validation
- planner routing scaffold
- state store interface + minimal implementation (no-op or SQL-backed table)

## 10. Configuration Schema (cleaned for rewrite)

## 10.1 `connections.yaml`
Top-level:
- `environments`

Environment object:
- `sourceConnections` (named)
- `destinationConnections` (named)
- optional operational settings (timeouts, cleanup defaults)

V1 required connection types:
- source `sqlServer`
- destination `sqlServer`

Keep type-based structure extensible so future destinations (Fabric, Snowflake, BigQuery, Databricks) can be added without redesign.

## 10.2 `streams.yaml`
Top-level:
- `maxParallelStreams`
- `defaults`
- `streams`

Per stream (resolved from defaults + override):
- `name` (map key)
- `sourceConnection` (reference to `connections.yaml`)
- `destinationConnection` (reference)
- `sourceSql` or `sourceTable` (at least one approach in V1; prefer `sourceSql` first)
- `targetTable`
- `update_strategy`: `replace_all | append | upsert`
- `incremental`:
  - `mode`: `watermark | sqlserver_change_tracking`
  - `update_key`
  - `update_key_type`
  - `chunk_size` (optional, load everything in a single chunk if not specified)

No secrets allowed in this file.

## 11. Observability

- Structured logs including stream and chunk context.
- Log start/end for each stream and chunk.
- Log per-stream totals (rows, chunks, duration, status).
- Print concise final summary table/list.

## 12. Compatibility and Behavior Changes Allowed

Backwards compatibility is not strict.
You may simplify legacy behavior to improve architecture, as long as:
- two-file config model is preserved,
- core incremental chunked loading semantics are preserved,
- V1 required scope works end-to-end.

Examples of acceptable simplifications:
- reduce CLI flags to essential set,
- postpone delete detection
- postpone BigQuery/Fabric runtime support,
- tighten validation and make config errors explicit.

## 13. Recommended CLI for Rewrite

Minimum required:
- `--env <name>`
- `--connections-file <path>`
- `--streams-file <path>`
- `--streams <a,b,c>` (optional filter)
- `--failfast`
- `--test-connections`
- `--log-level <INFO|DEBUG|TRACE|ERROR>`
- `--help`

## 14. Minimum Deliverables

- Working .NET 8 console app (`pluck`).
- Clean project structure matching architecture above.
- Example `connections.yaml` and `streams.yaml`.
- README with run instructions and config explanation.
- Unit tests:
  - interval parsing (`numeric`, `datetime` units)
  - chunk plan generation correctness
- Successful `dotnet build` and tests in local repo.

## 15. Suggested Implementation Order for Codex

1. Create solution/project skeleton and architecture folders.
2. Implement config models + YAML loading + validation + defaults resolution.
3. Implement planning engine and chunk interval math.
4. Implement SQL Server source reader and SQL Server target bulk writer.
5. Implement stream scheduler with max concurrency + ordered per-stream chunk commits.
6. Add structured logging + final summary.
7. Add CT scaffolding interfaces and no-op/basic state store.
8. Add tests and sample config files.

