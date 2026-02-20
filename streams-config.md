# streams.yaml Configuration Guide

This document describes all supported settings in `streams.yaml`.

## File Structure

`streams.yaml` has two top-level sections:

- `maxParallelStreams`: optional max number of streams to process concurrently (default `1`).
- `defaults`: optional values applied to every stream.
- `streams`: required map of stream definitions keyed by stream name.

Example:

```yaml
maxParallelStreams: 2

defaults:
  sourceConnection: "landSql"
  destinationConnection: "fabric"
  primaryKey: ["DW_Rowid"]
  updateKey: "DW_Rowid"
  chunkSize: "50000"
  stagingFileFormat: "csv.gz"
  targetTable: "{stream_table}"
  sourceSql: "SELECT * FROM dbo.{stream_table}"

streams:
  M3_MITMAS:
    sourceSql: "SELECT * FROM dbo.M3_MITMAS WHERE DW_Rowid <= 600000"
    excludeColumns: ["DW_Version", "LargeBlobCol"]

  M3_CFACIL:
    stagingFileFormat: "csv"
```

## Top-Level Properties

### `maxParallelStreams` (optional)

Maximum number of streams processed in parallel.

```yaml
maxParallelStreams: 4
```

Notes:

- Default is `1` (sequential stream processing).
- Value must be `>= 1`.
- Chunk ordering is still preserved within each stream.
- This setting controls only cross-stream concurrency.

## How Defaults and Overrides Work

For each stream:

1. Values are taken from `defaults`.
2. Any matching value in the stream overrides the default.
3. Token replacement is applied:
   - `{stream_table}` is replaced with the stream key name.

Example:

- Stream key: `M3_MITMAS`
- `defaults.targetTable: "land.{stream_table}"`
- Resolved value: `land.M3_MITMAS`

## Stream Properties

Each stream supports these properties.

### `sourceSql` (required)

Source query to read from SQL Server.

```yaml
sourceSql: "SELECT * FROM dbo.M3_MITMAS"
```

### `sourceConnection` (required)

Named source connection to use for this stream. The name must exist in
`connections.yaml` under `environments.<env>.sourceConnections`.

```yaml
sourceConnection: "landSql"
```

### `destinationConnection` (recommended in `defaults`)

Named destination to use for this stream. The name must exist in
`connections.yaml` under `environments.<env>.destinationConnections`.

```yaml
destinationConnection: "fabric"
```

Notes:

- If exactly one destination exists in `connections.yaml`, streams may omit this and Pluck will auto-select that single destination.
- If multiple destinations exist, set `defaults.destinationConnection` (or per-stream `destinationConnection`).

### `targetTable` (required)

Target table name in Fabric Warehouse.

```yaml
targetTable: "M3_MITMAS"
```

### `targetSchema` (optional)

Target schema for this stream. If omitted, the environment default schema from `connections.yaml` is used.

```yaml
targetSchema: "land"
```

### `primaryKey` (required)

Primary key columns used for merge/upsert.

```yaml
primaryKey: ["DW_Rowid"]
```

### `updateKey` (required)

Column used for incremental chunking and watermarking.

```yaml
updateKey: "DW_Rowid"
```

### `chunkSize` (optional, can come from `defaults`)

Chunk interval size (not row count). Supports numeric and date/time formats.

Numeric examples:

- `"50000"` for numeric update keys.
- `"1000000"`

Date/time examples:

- `"7d"` = 7 days
- `"2m"` = 2 months
- `"1y"` = 1 year
- `"12h"` = 12 hours

Notes:

- For numeric update keys, use unitless numeric values.
- For `date`/`datetime` update keys, use time units (`h`, `d`, `m`, `y`).
- If `chunkSize` is omitted, all rows after watermark are processed in a single chunk.

### `stagingFileFormat` (optional)

Staging file format uploaded to data lake and used by `COPY INTO`.

Supported values:

- `"csv.gz"` (default)
- `"csv"` (uncompressed)
- `"parquet"`

Aliases also accepted:

- `"gz"` -> `csv.gz`
- `"pq"` -> `parquet`

### `excludeColumns` (optional)

List of columns to exclude from replication.

```yaml
excludeColumns: ["LargeBinaryCol", "DebugJson", "DW_Version"]
```

Behavior:

- Excluded columns are removed from schema and load pipeline.
- Chunk read queries project only included columns.
- Excluded columns are not fetched in the chunk row read step.
- You cannot exclude `updateKey` or any `primaryKey` column.

### `delete_detection` (optional)

Controls hard-delete detection and soft-delete marking in the target table.

```yaml
delete_detection:
  type: subset   # subset or none
  where: "created_date > dateadd(year, -1, getdate())"
```

Fields:

- `type`:
  - `none` (default): do not run delete detection.
  - `subset`: run delete detection after all chunks for the stream.
- `where` (optional):
  - SQL predicate applied to both:
    - source key extraction subset
    - target rows considered for soft-delete comparison
  - if omitted, the full source/target set is used.

Behavior:

- Delete detection runs as a separate post-chunk step per stream.
- Source primary keys are fetched in one pass (not chunked).
- Any target row in the selected subset with no matching source key is soft-deleted.
- Soft delete means:
  - `_pluck_update_op = 'D'`
- `_pluck_update_datetime = SYSUTCDATETIME()`

### `change_tracking` (optional)

Enables SQL Server Change Tracking based replication for changed and deleted rows.

```yaml
change_tracking:
  enabled: true
  sourceTable: "dbo.M3_ORDERS"
```

Fields:

- `enabled`:
  - `false` (default): use standard update-key watermark processing.
  - `true`: use SQL Server Change Tracking for this stream.
- `sourceTable` (required when `enabled: true`):
  - One- to three-part table identifier used by `CHANGETABLE(CHANGES ...)`.
  - Example: `dbo.M3_ORDERS`

Behavior:

- When enabled and stream state exists:
  - Upserts are read from change tracking `I`/`U` rows and merged to target.
  - Deletes are read from change tracking `D` rows and soft-deleted in target.
- Stream state is stored in warehouse table `dbo.__pluck_stream_state` as last synced CT version.
- First run with `change_tracking.enabled: true` and no prior stream state:
  - falls back to standard sync for that run
  - then initializes CT stream state for the next run.
- If stored CT version is older than `CHANGE_TRACKING_MIN_VALID_VERSION`, the stream fails and requires re-initialization.
- When change tracking is enabled, `delete_detection` is skipped.

## Full Example

```yaml
defaults:
  sourceConnection: "landSql"
  destinationConnection: "fabric"
  primaryKey: ["DW_Rowid"]
  updateKey: "DW_Rowid"
  chunkSize: "7d"
  stagingFileFormat: "parquet"
  targetSchema: "land"
  targetTable: "{stream_table}"
  sourceSql: "SELECT * FROM dbo.{stream_table}"

streams:
  M3_MITMAS:
    # Uses 7-day datetime intervals from defaults
    sourceSql: "SELECT * FROM dbo.M3_MITMAS WHERE ModifiedDate >= '2024-01-01'"
    updateKey: "ModifiedDate"
    primaryKey: ["MITMAS_ID"]
    excludeColumns: ["LargeImage", "RowBlob"]

  M3_CFACIL:
    # Numeric interval chunking override
    updateKey: "DW_Rowid"
    chunkSize: "100000"
    stagingFileFormat: "csv"

  M3_ORDERS:
    change_tracking:
      enabled: true
      sourceTable: "dbo.M3_ORDERS"
```

## Target Metadata Columns

The loader always ensures these metadata columns exist in every target table:

- `_pluck_update_datetime` (`datetime2(0)`): timestamp of last insert/update/soft-delete.
- `_pluck_update_op` (`char(1)`): last operation:
  - `'I'` = insert
  - `'U'` = update
  - `'D'` = soft delete

Notes:

- These metadata columns are added even if delete detection is disabled.
- They are maintained by merge and delete-detection steps.

## Validation Rules

A stream must resolve to valid values for:

- `sourceConnection`
- `destinationConnection` (or exactly one destination defined in connections)
- `sourceSql`
- `targetTable`
- `primaryKey`
- `updateKey`

If required values are missing or invalid, the run fails early with an error.
