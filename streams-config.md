# streams.yaml Configuration Guide

This document describes all supported settings in `streams.yaml`.

## File Structure

`streams.yaml` has two top-level sections:

- `defaults`: optional values applied to every stream.
- `streams`: required map of stream definitions keyed by stream name.

Example:

```yaml
defaults:
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
  - `_sg_update_op = 'D'`
  - `_sg_update_datetime = SYSUTCDATETIME()`

## Full Example

```yaml
defaults:
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
    delete_detection:
      type: "subset"
      where: "created_date > dateadd(year, -1, getdate())"
```

## Target Metadata Columns

The loader always ensures these metadata columns exist in every target table:

- `_sg_update_datetime` (`datetime2(6)`): timestamp of last insert/update/soft-delete.
- `_sg_update_op` (`char(1)`): last operation:
  - `'I'` = insert
  - `'U'` = update
  - `'D'` = soft delete

Notes:

- These metadata columns are added even if delete detection is disabled.
- They are maintained by merge and delete-detection steps.

## Validation Rules

A stream must resolve to valid values for:

- `sourceSql`
- `targetTable`
- `primaryKey`
- `updateKey`

If required values are missing or invalid, the run fails early with an error.
