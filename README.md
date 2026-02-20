# Pluck

Pluck is a .NET 8 CLI for incremental replication from SQL Server sources into configurable named destinations.

## What it does

- Reads stream definitions from `streams.yaml`
- Reads environment and connection settings from `connections.yaml`
- Pulls source data in update-key chunks
- Supports per-stream SQL Server Change Tracking mode for changed/deleted rows
- Supports named destination connections (one destination per stream)
- Fabric Warehouse destination: uploads staged files to OneLake and loads with `COPY INTO` + merge/upsert
- SQL Server destination: uses `SqlBulkCopy` into temp tables, then `MERGE` upsert/soft-delete
- Optionally runs delete detection (`delete_detection.type: subset`) when `--delete_detection` is enabled

## Prerequisites

- .NET SDK 8.0+
- Network access to:
  - Source SQL Server(s)
  - Fabric Warehouse SQL endpoint
  - OneLake/ADLS Gen2 endpoint
- Entra app credentials with access required by your Fabric/OneLake setup

## Configuration

- `connections.yaml`: environment-specific source and destination connection settings plus cleanup behavior
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

Run integration tests against a local SQL Server instead of Docker:

```bash
export PLUCK_ITEST_USE_LOCAL_SQLSERVER=true
export PLUCK_ITEST_SQLSERVER="Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword;Encrypt=False;TrustServerCertificate=True"
dotnet test pluck.sln --filter "Category=Integration"
```

Run the Fabric-target CT integration test against a real Fabric Warehouse (uses real OneLake upload + `WarehouseLoader` apply path):

```bash
export PLUCK_ITEST_USE_LOCAL_SQLSERVER=true
export PLUCK_ITEST_SQLSERVER="Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword;Encrypt=False;TrustServerCertificate=True"

export PLUCK_ITEST_USE_FABRIC_WAREHOUSE=true

# Reuse your existing connections.yaml secrets/config (defaults shown)
export PLUCK_ITEST_CONNECTIONS_FILE="connections.yaml" # optional, default connections.yaml
export PLUCK_ITEST_ENV="dev"                           # optional, default dev

# Optional: override any fabric value per run with PLUCK_ITEST_FABRIC_* env vars

dotnet test Pluck.Tests/Pluck.Tests.csproj --filter "FullyQualifiedName~ChangeTracking_Apply_To_Fabric_Target_Should_SoftDelete_Real_Deletes"
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

## Testing tests
