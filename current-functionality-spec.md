# Pluck Current Functionality Specification (Architecture-Independent)

This document defines what the current `pluck` program does today, as a functional contract for a clean rewrite in a new repository.

## 1. Product Goal

`pluck` is a .NET 8 CLI that incrementally replicates data from configured source connections into configured destination connections.

Supported source types:
- `sqlServer`
- `bigQuery` (via ODBC)

Supported destination types:
- `fabricWarehouse`
- `sqlServer`

Replication is stream-based. A stream defines:
- source query
- update watermark strategy
- target table
- primary key
- optional delete detection
- optional SQL Server Change Tracking mode

## 2. Inputs and Outputs

Inputs:
- `connections.yaml`: environment-scoped source/destination connection definitions and cleanup settings.
- `streams.yaml`: stream defaults + stream-specific replication definitions.
- CLI flags.

Output effects:
- Creates/updates target schemas/tables.
- Loads changed/new rows by upsert merge behavior.
- Optionally marks rows as soft-deleted.
- Optionally persists per-stream change-tracking version state.
- Writes logs to stdout.

## 3. CLI Contract

### 3.1 Supported options
- `--help`, `-h`
- `--env <name>`
- `--connections-file <path>`
- `--streams-file <path>`
- `--temp-path <path>`
- `--streams <name1,name2,...>`
- `--test-connections`
- `--delete_detection`
- `--failfast`
- `--log-level <INFO|DEBUG|TRACE|ERROR>`
- `--debug`
- `--trace`

### 3.2 Argument parsing behavior
- Unknown option => error.
- Positional arg => error.
- Duplicate option => error.
- Missing value for value option => error.
- Flag options cannot have `--flag=value`.
- `--trace` overrides `--debug` and `--log-level`.
- `--debug` overrides `--log-level`.
- If no logging option is provided, default level is `INFO`.

### 3.3 Defaults
- Environment: `dev`
- Connections file: `connections.yaml`
- Streams file: `streams.yaml`
- Temp path: OS temp path

### 3.4 Exit codes
- `0`: success (including `--help` and successful `--test-connections`)
- `1`: general run failure (parse failure, stream failures, unexpected exception)
- `2`: source connection test failure (`--test-connections` mode)
- `3`: destination connection test failure (`--test-connections` mode)

## 4. `connections.yaml` Functional Contract

Top-level:
- `environments` map (keyed by env name).

Per environment:
- `sourceConnections` (required, at least one)
- `destinationConnections` (required, at least one after resolution)
- `cleanup` (optional)

### 4.1 Source connection schema
Fields:
- `type` optional, defaults to `sqlServer`
- `connectionString` required
- `commandTimeoutSeconds` optional, defaults to `3600`

Accepted source type aliases:
- SQL Server: `sqlserver`, `sql_server`, `sql`
- BigQuery: `bigquery`, `big_query`, `googlebigquery`, `google_bigquery`, `bq`

### 4.2 Destination connection schema
Fields:
- `type` required (`fabricWarehouse` or `sqlServer`, aliases accepted)

If `fabricWarehouse`:
- `fabricWarehouse.server` required
- `fabricWarehouse.database` required
- `fabricWarehouse.targetSchema` optional
- `fabricWarehouse.commandTimeoutSeconds` optional, default `3600`
- `oneLakeStaging` required section
- `auth` required section

`oneLakeStaging` behavior:
- If `dataLakeUrl` is set: parse DFS URL and use it directly.
- Else require `workspaceId` + `lakehouseId`; optional `filesPath` default `staging/incremental-repl`.

`auth` fields:
- `tenantId`, `clientId`, `clientSecret`

If `sqlServer`:
- `sqlServer.connectionString` required
- `sqlServer.targetSchema` optional
- `sqlServer.commandTimeoutSeconds` optional, default `3600`
- `sqlServer.bulkLoad` optional:
  - `method` default `SqlBulkCopy` (only this value currently supported)
  - `batchSize` default `10000`
  - `bulkCopyTimeoutSeconds` default `0`

### 4.3 Destination type aliases
Accepted aliases:
- Fabric: `fabricwarehouse`, `fabric`
- SQL Server: `sqlserver`, `sql_server`, `sql`

### 4.4 Backward compatibility
If `destinationConnections` is missing but legacy top-level Fabric keys exist:
- `fabricWarehouse`
- `oneLakeStaging`
- `auth`

Then a synthetic destination named `fabric` is created.

### 4.5 Cleanup settings
Defaults:
- `deleteLocalTempFiles: true`
- `deleteStagedFiles: true`
- `dropTempTables: true`

## 5. `streams.yaml` Functional Contract

Top-level:
- `maxParallelStreams` optional, default `1`, must be `>= 1`
- `defaults` optional
- `streams` required map

### 5.1 Per-stream required fields after defaults merge
- `sourceSql`
- `sourceConnection`
- `targetTable`
- `primaryKey` (non-empty list)
- `updateKey`

### 5.2 Optional stream fields
- `destinationConnection`
- `targetSchema`
- `excludeColumns`
- `chunkSize`
- `stagingFileFormat`
- `sql_server.bufferChunksToCsvBeforeBulkCopy` (default false)
- `sql_server.createClusteredColumnstoreOnCreate` (default false)
- `delete_detection`:
  - `type`: `none` (default) or `subset`
  - `where`: optional SQL predicate
- `change_tracking`:
  - `enabled`: default false
  - `sourceTable` required when enabled

### 5.3 Stream defaults merge and token replacement
For each stream:
1. Merge defaults + stream override (override wins).
2. Apply token substitution on string/list fields: `{stream_table}` -> stream key.

Fields receiving substitution include:
- `sourceSql`, `sourceConnection`, `destinationConnection`, `targetTable`, `targetSchema`, `updateKey`, `stagingFileFormat`
- `primaryKey[]`, `excludeColumns[]`
- `delete_detection.where`
- `change_tracking.sourceTable`

### 5.4 Destination connection assignment rule
If a stream has no `destinationConnection`:
- If exactly one destination exists, it is auto-assigned.
- If multiple destinations exist, run fails.

### 5.5 Stream filter (`--streams`)
- Comma-separated list.
- Names are matched case-insensitively.
- Unknown stream name => run fails.

## 6. Core Replication Algorithm (Standard Mode)

For each stream:
1. Resolve source and destination config names.
2. Build source schema reader + source chunk reader.
3. Normalize stream staging format (`csv.gz` default).
4. Discover source query schema.
5. Reject source schema if it already includes reserved names:
   - `_pluck_update_datetime`
   - `_pluck_update_op`
6. Apply `excludeColumns`.
   - Reject if `updateKey` excluded.
   - Reject if any `primaryKey` excluded.
7. Validate required columns still exist in source schema.
8. Ensure destination table/schema exists and aligned.
   - Add missing source columns.
   - Ensure metadata columns exist.
9. Read target watermark = `MAX(updateKey)` in target table.
10. Read source min/max update key where `updateKey > watermark` (or all rows if watermark null).
11. Chunk loop:
   - If `chunkSize` is set: process `[lowerBound, upperBound)` intervals.
   - If `chunkSize` unset: process one inclusive range `[lowerBound, srcMax]`.
12. For each non-empty chunk:
   - Materialize or stream chunk depending on destination/options.
   - Load into destination temp table.
   - MERGE temp table into target.
13. Optionally run delete detection (requires both stream config + CLI switch).
14. Stream completes.

If source min/max is null (no rows after watermark), stream does no chunk load.

## 7. Chunking Semantics

`chunkSize` is an update-key interval, not a row count.

Allowed formats:
- Numeric: `50000`, `1000000` (unitless)
- Time units: `h`, `d`, `m`, `y` (e.g. `12h`, `7d`, `2m`, `1y`)

Rules:
- Amount must be positive integer.
- Numeric intervals require numeric update key type.
- Time intervals require `DateTime`/`DateTimeOffset` update key type.
- Temporal precision conversion and math are runtime-type driven.
- Chunk query predicates do not include explicit ordering.

## 8. Destination-Specific Load Behavior

### 8.1 Common merge semantics (both destinations)
MERGE is keyed by `primaryKey`.

On match:
- update non-PK columns from source
- set `_pluck_update_datetime = SYSUTCDATETIME()`
- set `_pluck_update_op = 'U'`

On not matched by target:
- insert source columns
- insert metadata:
  - `_pluck_update_datetime = SYSUTCDATETIME()`
  - `_pluck_update_op = 'I'`

### 8.2 Fabric Warehouse destination
Per chunk:
1. Source rows are written to local staging file (`csv.gz`, `csv`, or `parquet`).
2. File uploaded to OneLake/ADLS path.
3. Temp table created in warehouse.
4. `COPY INTO` temp table from uploaded file.
5. Row count in temp table must equal source row count.
6. MERGE temp -> target.
7. Temp table dropped if `cleanup.dropTempTables`.
8. Local file deleted if `cleanup.deleteLocalTempFiles`.
9. Uploaded staged file deleted if `cleanup.deleteStagedFiles`.

### 8.3 SQL Server destination
Default mode (streaming):
1. Stream chunk rows directly via `SqlBulkCopy` into temp table (no chunk file required).
2. Verify temp row count equals rows copied.
3. MERGE temp -> target (if rows copied > 0).
4. Drop temp table based on cleanup setting.

Buffered mode (`sql_server.bufferChunksToCsvBeforeBulkCopy = true`):
1. Write each chunk to local CSV first.
2. Bulk copy from CSV into temp table.
3. Verify row count equals expected chunk row count.
4. MERGE temp -> target.
5. Optional local file cleanup.

`sql_server.bulkLoad.method` currently supports only `SqlBulkCopy`.

## 9. Metadata Columns

Every target table is ensured to have:
- `_pluck_update_datetime datetime2(0) NULL`
- `_pluck_update_op char(1) NULL`

These are added even when delete detection is not used.

## 10. Delete Detection Behavior

Delete detection runs only when both conditions are true:
- stream config: `delete_detection.type = subset`
- CLI flag: `--delete_detection`

If stream is in change-tracking mode, delete detection step is skipped.

Algorithm:
1. Extract source primary keys from `sourceSql` with optional `delete_detection.where` filter.
2. Load keys into a temp table in destination.
3. Soft-delete target rows that match subset filter but are missing from source key set.

Soft delete update:
- `_pluck_update_datetime = SYSUTCDATETIME()`
- `_pluck_update_op = 'D'`
- rows already marked `'D'` are not updated again.

If `where` is absent, full target/source key set is compared.

## 11. Change Tracking Behavior (`change_tracking.enabled`)

Applies only to SQL Server sources.

Required config when enabled:
- `change_tracking.sourceTable` (simple identifier format only; one- to three-part)

Runtime flow:
1. Read source `CHANGE_TRACKING_CURRENT_VERSION()` as sync target version.
2. Read destination stream state (`dbo.__pluck_stream_state`) for last synced CT version.

Cases:
- Prior state exists:
  - validate `lastSyncVersion >= CHANGE_TRACKING_MIN_VALID_VERSION(sourceTable)`.
  - read CT upserts (`I`/`U`) and apply merge.
  - read CT deleted keys and soft-delete by keys.
  - persist new `ct_last_sync_version`.
- Prior state missing (first CT run):
  - log warning.
  - perform standard full/incremental sync for this run.
  - initialize stream CT version to current source CT version at end.

CT stream state table:
- `dbo.__pluck_stream_state(stream_name PK, ct_last_sync_version, updated_utc)`
- auto-created if missing.

CT stale-state behavior:
- If stored version is older than min valid version, run fails and requires re-init.

## 12. Schema Management Rules

For both destination types, stream startup ensures:
- target schema exists (creates if missing)
- target table exists (creates if missing with source-derived columns)
- missing source columns are added to target
- metadata columns are present

Additional SQL Server-only behavior:
- if table was created during this run and `sql_server.createClusteredColumnstoreOnCreate = true`, create clustered columnstore index `__pluck_cci`

## 13. Data Type Mapping Behavior

Source schema discovery:
- SQL Server preferred path: `sp_describe_first_result_set`
- fallback for all providers: schema-only query wrapper (`SELECT * FROM (...) WHERE 1=0`)

BigQuery source types are mapped to SQL Server-like internal types (e.g. `INT64 -> bigint`, `TIMESTAMP -> datetime2(6)`, etc.).

Fabric type normalization includes:
- Unicode types converted to `varchar(...)` / `varchar(max)`
- temporal precision normalized to max 6

## 14. Concurrency and Failure Behavior

Cross-stream concurrency:
- controlled by `maxParallelStreams`
- `1` = sequential
- `>1` = parallel stream execution

Within a stream:
- chunk apply order is sequential

Failure behavior:
- Stream exceptions are logged and stream marked failed.
- Overall run exits non-zero if any stream failed.
- With `--failfast`, first stream failure cancels scheduling/execution of remaining streams.

## 15. Connection Test Mode (`--test-connections`)

Source checks:
- open each configured source connection.

Destination checks:
- Fabric: acquire token, open SQL endpoint, test OneLake filesystem/path access.
- SQL Server: open destination SQL connection.

Stops on first failure and exits with code:
- `2` for source failure
- `3` for destination failure

## 16. Logging Behavior

Console logging with timestamp; single-line format.

Level behavior:
- `INFO`: high-level progress and chunk summaries
- `DEBUG`: compact SQL operation summaries and timings
- `TRACE`: full SQL text + SQL parameter values

This debug/trace split is consistently implemented in SQL-heavy components.

## 17. Cleanup Behavior

Controlled by `connections.yaml -> cleanup`:
- `deleteLocalTempFiles`: delete local temp chunk/key files and parent directory (best effort)
- `deleteStagedFiles`: delete uploaded OneLake staged files (best effort)
- `dropTempTables`: drop destination temp tables after chunk/delete work

## 18. Important Compatibility Notes for Rewrite

To preserve external behavior, retain:
- same YAML syntax and field names
- defaults/merge/token rules
- CLI flags and exit codes
- destination auto-selection rule when exactly one destination exists
- metadata column semantics (`I/U/D` + UTC timestamp)
- CT fallback-on-first-run behavior
- CT stale-version failure behavior
- delete detection gating (`type=subset` + `--delete_detection`)
- strict requirement that PK/updateKey cannot be excluded
- staging format aliases: `gz -> csv.gz`, `pq -> parquet`
- source/destination type aliases listed above

