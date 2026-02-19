# Pluck

Pluck is a .NET 8 CLI for incremental replication from SQL Server sources into Microsoft Fabric Warehouse using OneLake staging files.

## What it does

- Reads stream definitions from `streams.yaml`
- Reads environment and connection settings from `connections.yaml`
- Pulls source data in update-key chunks
- Supports per-stream SQL Server Change Tracking mode for changed/deleted rows
- Writes chunk files (`csv`, `csv.gz`, or `parquet`) locally and uploads to OneLake
- Loads into Fabric Warehouse with `COPY INTO` + merge/upsert
- Optionally runs delete detection (`delete_detection.type: subset`) when `--delete_detection` is enabled

## Prerequisites

- .NET SDK 8.0+
- Network access to:
  - Source SQL Server(s)
  - Fabric Warehouse SQL endpoint
  - OneLake/ADLS Gen2 endpoint
- Entra app credentials with access required by your Fabric/OneLake setup

## Configuration

- `connections.yaml`: environment-specific source, warehouse, staging, auth, and cleanup settings
- `streams.yaml`: stream defaults and per-stream replication settings

Detailed config references:

- `connections-config.md`
- `streams-config.md`

For automatic changed/deleted row detection, configure per-stream `change_tracking.enabled: true` with `change_tracking.sourceTable` in `streams.yaml`.

## Build

```bash
dotnet build
```

## Test

Run unit tests:

```bash
dotnet test pluck.sln --filter "Category!=Integration"
```

Run integration tests (requires Docker running locally):

```bash
dotnet test pluck.sln --filter "Category=Integration"
```

Automated test runs are configured in GitHub Actions: `.github/workflows/test.yml`.

## Run

Default run (`dev`, `connections.yaml`, `streams.yaml`):

```bash
dotnet run --project pluck.csproj
```

Run a specific environment:

```bash
dotnet run --project pluck.csproj -- --env prod
```

Run only selected streams:

```bash
dotnet run --project pluck.csproj -- --streams M3_MITMAS,M3_CFACIL
```

Enable delete detection for streams configured with `delete_detection.type: subset`:

```bash
dotnet run --project pluck.csproj -- --delete_detection
```

Stop all remaining streams after the first stream failure:

```bash
dotnet run --project pluck.csproj -- --failfast
```

Validate connections and exit:

```bash
dotnet run --project pluck.csproj -- --test-connections
```

Use alternate config file paths:

```bash
dotnet run --project pluck.csproj -- \
  --connections-file ./connections.yaml \
  --streams-file ./streams.yaml
```

## CLI options

```text
--help, -h
--env <name>
--connections-file <path>
--streams-file <path>
--streams <name1,name2,...>
--test-connections
--delete_detection
--failfast
--log-level <INFO|DEBUG|TRACE|ERROR>
--debug
--trace
```

## Notes

- `maxParallelStreams` in `streams.yaml` controls cross-stream concurrency.
- Metadata columns are managed in target tables: `_pluck_update_datetime`, `_pluck_update_op`.
- Keep secrets out of source control for production environments.
