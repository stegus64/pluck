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
        type: "sqlServer"
        connectionString: "Server=sql1;Database=LandDb;User Id=user;Password=secret;Encrypt=True;TrustServerCertificate=True"
        commandTimeoutSeconds: 3600
      erpSql:
        type: "bigQuery"
        connectionString: "Driver={Simba Google BigQuery ODBC Driver};ProjectId=my-project;OAuthType=3;..."
        commandTimeoutSeconds: 3600
      erpSqlReplica:
        type: "sqlServer"
        connectionString: "Server=sql2;Database=ErpDb;User Id=user;Password=secret;Encrypt=True;TrustServerCertificate=True"
        commandTimeoutSeconds: 3600

    destinationConnections:
      fabric:
        type: "fabricWarehouse"
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

      onprem:
        type: "sqlServer"
        sqlServer:
          connectionString: "Server=onprem-sql;Database=LandDbReplica;User Id=user;Password=secret;Encrypt=True;TrustServerCertificate=True"
          targetSchema: "land"
          commandTimeoutSeconds: 3600
          bulkLoad:
            method: "SqlBulkCopy"
            batchSize: 10000
            bulkCopyTimeoutSeconds: 0

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
    type: "sqlServer"
    connectionString: "Server=sql;Database=db;..."
    commandTimeoutSeconds: 3600
```

Fields per source connection:

- `type` (optional): `sqlServer` (default) or `bigQuery`.
- `connectionString` (required):
  - for `sqlServer`: SQL Server connection string.
  - for `bigQuery`: Google BigQuery ODBC connection string.
- `commandTimeoutSeconds` (optional): command timeout in seconds. Default `3600`.

### `destinationConnections` (required)

Dictionary of named destination connections.  
Each stream in `streams.yaml` must reference one destination name via `destinationConnection`
(or set `defaults.destinationConnection` once for all streams).

```yaml
destinationConnections:
  fabric:
    type: "fabricWarehouse"
    fabricWarehouse:
      server: "warehouse-host.datawarehouse.fabric.microsoft.com"
      database: "wh_edp_land"
      targetSchema: "land"
    oneLakeStaging:
      dataLakeUrl: "https://account.dfs.core.windows.net/filesystem/staging/incremental-repl"
    auth:
      tenantId: "tenant-guid"
      clientId: "app-guid"
      clientSecret: "secret"
  onprem:
    type: "sqlServer"
    sqlServer:
      connectionString: "Server=onprem;Database=replica;..."
      targetSchema: "land"
      bulkLoad:
        method: "SqlBulkCopy"
```

Per destination fields:

- `type` (required): `fabricWarehouse` or `sqlServer`.

When `type: fabricWarehouse`, these sections are required:

- `fabricWarehouse`
- `oneLakeStaging`
- `auth`

`fabricWarehouse` fields:

- `server` (required): warehouse SQL endpoint host.
- `database` (required): database name.
- `targetSchema` (optional): default target schema for streams that do not set `targetSchema`.
- `commandTimeoutSeconds` (optional): default `3600`.

`oneLakeStaging` fields:

- `dataLakeUrl` (optional): full ADLS Gen2 DFS URL (filesystem + optional folder path).
- `workspaceId` (required when `dataLakeUrl` is not set).
- `lakehouseId` (required when `dataLakeUrl` is not set).
- `filesPath` (optional): relative path under `Files/`. Default `staging/incremental-repl`.

`auth` fields:

- `tenantId` (required)
- `clientId` (required)
- `clientSecret` (required)

When `type: sqlServer`, this section is required:

- `sqlServer`

`sqlServer` fields:

- `connectionString` (required): destination SQL Server connection string.
- `targetSchema` (optional): default schema for streams that do not set `targetSchema`.
- `commandTimeoutSeconds` (optional): default `3600`.
- `bulkLoad` (optional):
  - `method`: currently only `SqlBulkCopy` is supported.
  - `batchSize`: default `10000`.
  - `bulkCopyTimeoutSeconds`: default `0` (no limit).

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
- `destinationConnections` must contain at least one named destination in the selected environment.
- Stream `destinationConnection` values must match a key in `destinationConnections`.

## Backward Compatibility

Legacy top-level destination keys are still accepted:

- `fabricWarehouse`
- `oneLakeStaging`
- `auth`

If `destinationConnections` is omitted and legacy Fabric keys are present, Pluck auto-creates a destination named `fabric`.
