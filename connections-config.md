# connections.yaml Configuration Guide

This document describes all supported settings in `connections.yaml`.

## File Structure

`connections.yaml` has a top-level `environments` map.
Each key under `environments` is an environment name (for example `dev`, `test`, `prod`).

Example:

```yaml
environments:
  dev:
    sourceConnections:
      landSql:
        connectionString: "Server=sql1;Database=LandDb;User Id=user;Password=secret;Encrypt=True;TrustServerCertificate=True"
        commandTimeoutSeconds: 3600
      erpSql:
        connectionString: "Server=sql2;Database=ErpDb;User Id=user;Password=secret;Encrypt=True;TrustServerCertificate=True"
        commandTimeoutSeconds: 3600

    fabricWarehouse:
      server: "warehouse-host.datawarehouse.fabric.microsoft.com"
      database: "wh_edp_land"
      targetSchema: "land"
      commandTimeoutSeconds: 3600

    oneLakeStaging:
      dataLakeUrl: "https://account.dfs.core.windows.net/filesystem/staging/incremental-repl"

    auth:
      tenantId: "00000000-0000-0000-0000-000000000000"
      clientId: "00000000-0000-0000-0000-000000000000"
      clientSecret: "your-secret"

    cleanup:
      deleteLocalTempFiles: true
      deleteStagedFiles: true
      dropTempTables: true
```

## CLI Selection

Select environment with:

```bash
--env <environment-name>
```

If omitted, default is `dev`.

## Environment Properties

### `sourceConnections` (required)

Dictionary of named source SQL connections.
Each stream in `streams.yaml` must reference one of these names via `sourceConnection`.

```yaml
sourceConnections:
  landSql:
    connectionString: "Server=sql;Database=db;..."
    commandTimeoutSeconds: 3600
```

Fields per source connection:

- `connectionString` (required): SQL Server connection string.
- `commandTimeoutSeconds` (optional): command timeout in seconds. Default `3600`.

### `fabricWarehouse` (required)

Fabric Warehouse connection settings.

```yaml
fabricWarehouse:
  server: "warehouse-host.datawarehouse.fabric.microsoft.com"
  database: "wh_edp_land"
  targetSchema: "land"
  commandTimeoutSeconds: 3600
```

Fields:

- `server` (required): warehouse SQL endpoint host.
- `database` (required): database name.
- `targetSchema` (optional): default target schema for streams that do not set `targetSchema`.
- `commandTimeoutSeconds` (optional): command timeout in seconds. Default `3600`.

### `oneLakeStaging` (required)

Staging location for uploaded files used by `COPY INTO`.

You can configure this in one of two ways:

1. `dataLakeUrl` (recommended when available)
2. `workspaceId` + `lakehouseId` (+ optional `filesPath`)

Example using `dataLakeUrl`:

```yaml
oneLakeStaging:
  dataLakeUrl: "https://account.dfs.core.windows.net/filesystem/staging/incremental-repl"
```

Example using workspace/lakehouse:

```yaml
oneLakeStaging:
  workspaceId: "workspace-guid"
  lakehouseId: "lakehouse-guid"
  filesPath: "staging/incremental-repl"
```

Fields:

- `dataLakeUrl` (optional): full ADLS Gen2 DFS URL (filesystem + optional folder path).
- `workspaceId` (required when `dataLakeUrl` is not set).
- `lakehouseId` (required when `dataLakeUrl` is not set).
- `filesPath` (optional): relative path under `Files/`. Default `staging/incremental-repl`.

### `auth` (required)

Entra ID client credential settings used for Fabric/OneLake access.

```yaml
auth:
  tenantId: "tenant-guid"
  clientId: "app-guid"
  clientSecret: "secret"
```

Fields:

- `tenantId` (required)
- `clientId` (required)
- `clientSecret` (required)

### `cleanup` (optional)

Controls cleanup behavior after processing.

```yaml
cleanup:
  deleteLocalTempFiles: true
  deleteStagedFiles: true
  dropTempTables: true
```

Defaults:

- `deleteLocalTempFiles: true`
- `deleteStagedFiles: true`
- `dropTempTables: true`

## Validation Notes

- YAML parsing is strict: unknown/misspelled keys cause errors.
- `sourceConnections` must contain at least one named connection in the selected environment.
- Stream `sourceConnection` values must match a key in `sourceConnections`.
