using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Pluck.Auth;
using Pluck.Config;
using Pluck.Source;
using Pluck.Staging;
using Pluck.Target;
using Testcontainers.MsSql;
using Xunit;

namespace Pluck.Tests;

public sealed class ChangeTrackingIntegrationTests
{
    private const string UseLocalSqlServerEnvVar = "PLUCK_ITEST_USE_LOCAL_SQLSERVER";
    private const string LocalSqlServerConnectionEnvVar = "PLUCK_ITEST_SQLSERVER";
    private const string UseFabricWarehouseEnvVar = "PLUCK_ITEST_USE_FABRIC_WAREHOUSE";
    private const string TestConnectionsFileEnvVar = "PLUCK_ITEST_CONNECTIONS_FILE";
    private const string TestEnvironmentNameEnvVar = "PLUCK_ITEST_ENV";
    private const string FabricWarehouseServerEnvVar = "PLUCK_ITEST_FABRIC_SERVER";
    private const string FabricWarehouseDatabaseEnvVar = "PLUCK_ITEST_FABRIC_DATABASE";
    private const string FabricWarehouseTargetSchemaEnvVar = "PLUCK_ITEST_FABRIC_TARGET_SCHEMA";
    private const string FabricDataLakeUrlEnvVar = "PLUCK_ITEST_FABRIC_DATALAKE_URL";
    private const string FabricWorkspaceIdEnvVar = "PLUCK_ITEST_FABRIC_WORKSPACE_ID";
    private const string FabricLakehouseIdEnvVar = "PLUCK_ITEST_FABRIC_LAKEHOUSE_ID";
    private const string FabricFilesPathEnvVar = "PLUCK_ITEST_FABRIC_FILES_PATH";
    private const string FabricTenantIdEnvVar = "PLUCK_ITEST_FABRIC_TENANT_ID";
    private const string FabricClientIdEnvVar = "PLUCK_ITEST_FABRIC_CLIENT_ID";
    private const string FabricClientSecretEnvVar = "PLUCK_ITEST_FABRIC_CLIENT_SECRET";

    [SkippableFact]
    [Trait("Category", "Integration")]
    public async Task SourceChunkReader_Should_Read_ChangeTracking_Upserts_And_Deletes()
    {
        var (sqlTarget, skipReason) = await TryResolveSqlTargetAsync();
        Skip.If(sqlTarget is null, skipReason);
        await using var _ = sqlTarget!;

        var testDbConnectionString = await InitializeTestDatabaseAsync(sqlTarget!);
        var reader = new SourceChunkReader(testDbConnectionString);
        var fromVersion = await reader.GetChangeTrackingCurrentVersionAsync();
        fromVersion.Should().NotBeNull();

        await using (var conn = new SqlConnection(testDbConnectionString))
        {
            await conn.OpenAsync();
            await ExecuteNonQueryAsync(conn, @"
UPDATE dbo.Customers
SET Name = N'Alice-2', UpdatedAt = SYSUTCDATETIME()
WHERE Id = 1;

DELETE FROM dbo.Customers
WHERE Id = 2;

INSERT INTO dbo.Customers (Id, Name, UpdatedAt)
VALUES (3, N'Cara', SYSUTCDATETIME());
");
        }

        var toVersion = await reader.GetChangeTrackingCurrentVersionAsync();
        var minValid = await reader.GetChangeTrackingMinValidVersionAsync("dbo.Customers");
        toVersion.Should().NotBeNull();
        minValid.Should().NotBeNull();
        minValid!.Value.Should().BeLessOrEqualTo(fromVersion!.Value);

        var columns = new List<SourceColumn>
        {
            new("Id", "int", false),
            new("Name", "nvarchar(100)", true),
            new("UpdatedAt", "datetime2(0)", true),
        };

        var upserts = await ToListAsync(
            reader.ReadChangeTrackingUpsertsAsync(
                sourceSql: "SELECT Id, Name, UpdatedAt FROM dbo.Customers",
                sourceTable: "dbo.Customers",
                columns: columns,
                primaryKey: ["Id"],
                lastSyncVersionExclusive: fromVersion.Value,
                syncToVersionInclusive: toVersion!.Value));

        var changedIds = upserts.Select(r => Convert.ToInt32(r[0]!)).OrderBy(x => x).ToArray();
        changedIds.Should().Equal(1, 3);

        var deletedKeys = await ToListAsync(
            reader.ReadChangeTrackingDeletedKeysAsync(
                sourceTable: "dbo.Customers",
                primaryKey: ["Id"],
                lastSyncVersionExclusive: fromVersion.Value,
                syncToVersionInclusive: toVersion.Value));

        var deletedIds = deletedKeys.Select(r => Convert.ToInt32(r[0]!)).OrderBy(x => x).ToArray();
        deletedIds.Should().Equal(2);
    }

    [SkippableFact]
    [Trait("Category", "Integration")]
    public async Task SourceChunkReader_Should_Return_Null_CurrentVersion_When_Database_ChangeTracking_Is_Disabled()
    {
        var (sqlTarget, skipReason) = await TryResolveSqlTargetAsync();
        Skip.If(sqlTarget is null, skipReason);
        await using var _ = sqlTarget!;

        string testDbConnectionString;
        try
        {
            testDbConnectionString = await InitializeTestDatabaseWithoutChangeTrackingAsync(sqlTarget!);
        }
        catch (SqlException ex) when (
            ShouldUseLocalSqlServer() &&
            ex.Message.Contains("permission denied", StringComparison.OrdinalIgnoreCase))
        {
            Skip.If(true, $"Skipping local SQL Server CT-disabled test due to missing permissions: {ex.Message}");
            return;
        }

        var reader = new SourceChunkReader(testDbConnectionString);

        var currentVersion = await reader.GetChangeTrackingCurrentVersionAsync();

        currentVersion.Should().BeNull();
    }

    [SkippableFact]
    [Trait("Category", "Integration")]
    public async Task SourceChunkReader_Should_Throw_When_Table_ChangeTracking_Is_Not_Enabled()
    {
        var (sqlTarget, skipReason) = await TryResolveSqlTargetAsync();
        Skip.If(sqlTarget is null, skipReason);
        await using var _ = sqlTarget!;

        var testDbConnectionString = await InitializeTestDatabaseWithDatabaseChangeTrackingOnlyAsync(sqlTarget!);
        var reader = new SourceChunkReader(testDbConnectionString);
        var fromVersion = await reader.GetChangeTrackingCurrentVersionAsync();
        fromVersion.Should().NotBeNull();

        await using (var conn = new SqlConnection(testDbConnectionString))
        {
            await conn.OpenAsync();
            await ExecuteNonQueryAsync(conn, @"
UPDATE dbo.Customers
SET Name = N'Alice-2', UpdatedAt = SYSUTCDATETIME()
WHERE Id = 1;
");
        }

        var toVersion = await reader.GetChangeTrackingCurrentVersionAsync();
        toVersion.Should().NotBeNull();

        var columns = new List<SourceColumn>
        {
            new("Id", "int", false),
            new("Name", "nvarchar(100)", true),
            new("UpdatedAt", "datetime2(0)", true),
        };

        var act = async () => await ToListAsync(
            reader.ReadChangeTrackingUpsertsAsync(
                sourceSql: "SELECT Id, Name, UpdatedAt FROM dbo.Customers",
                sourceTable: "dbo.Customers",
                columns: columns,
                primaryKey: ["Id"],
                lastSyncVersionExclusive: fromVersion!.Value,
                syncToVersionInclusive: toVersion!.Value));

        await act.Should().ThrowAsync<SqlException>();
    }

    [SkippableFact]
    [Trait("Category", "Integration")]
    public async Task ChangeTracking_Apply_To_Target_Should_Keep_Resurrected_Row()
    {
        var (sqlTarget, skipReason) = await TryResolveSqlTargetAsync();
        Skip.If(sqlTarget is null, skipReason);
        await using var _ = sqlTarget!;

        var testDbConnectionString = await InitializeTestDatabaseAsync(sqlTarget!);
        var reader = new SourceChunkReader(testDbConnectionString);

        await using var conn = new SqlConnection(testDbConnectionString);
        await conn.OpenAsync();
        await ExecuteNonQueryAsync(conn, @"
IF OBJECT_ID(N'dbo.CustomersTarget', N'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.CustomersTarget;
END;

CREATE TABLE dbo.CustomersTarget (
    Id int NOT NULL PRIMARY KEY,
    Name nvarchar(100) NULL,
    UpdatedAt datetime2(0) NULL,
    _pluck_update_datetime datetime2(0) NULL,
    _pluck_update_op nvarchar(1) NULL
);
");

        var fromVersion = await reader.GetChangeTrackingCurrentVersionAsync();
        fromVersion.Should().NotBeNull();

        await ExecuteNonQueryAsync(conn, @"
INSERT INTO dbo.Customers (Id, Name, UpdatedAt)
VALUES (10, N'First', SYSUTCDATETIME());

DELETE FROM dbo.Customers
WHERE Id = 10;

INSERT INTO dbo.Customers (Id, Name, UpdatedAt)
VALUES (10, N'Reborn', SYSUTCDATETIME());
");

        var toVersion = await reader.GetChangeTrackingCurrentVersionAsync();
        toVersion.Should().NotBeNull();

        var columns = new List<SourceColumn>
        {
            new("Id", "int", false),
            new("Name", "nvarchar(100)", true),
            new("UpdatedAt", "datetime2(0)", true),
        };

        var upserts = await ToListAsync(
            reader.ReadChangeTrackingUpsertsAsync(
                sourceSql: "SELECT Id, Name, UpdatedAt FROM dbo.Customers",
                sourceTable: "dbo.Customers",
                columns: columns,
                primaryKey: ["Id"],
                lastSyncVersionExclusive: fromVersion!.Value,
                syncToVersionInclusive: toVersion!.Value));

        var deletedKeys = await ToListAsync(
            reader.ReadChangeTrackingDeletedKeysAsync(
                sourceTable: "dbo.Customers",
                primaryKey: ["Id"],
                lastSyncVersionExclusive: fromVersion.Value,
                syncToVersionInclusive: toVersion.Value));

        await ApplyChangeTrackingToTargetAsync(conn, upserts, deletedKeys);

        await using var verifyCmd = new SqlCommand(@"
SELECT Name, _pluck_update_op
FROM dbo.CustomersTarget
WHERE Id = 10;
", conn);
        await using var rdr = await verifyCmd.ExecuteReaderAsync();
        var found = await rdr.ReadAsync();
        found.Should().BeTrue("resurrected row should remain in target after CT apply");
        rdr.GetString(0).Should().Be("Reborn");
        (rdr.IsDBNull(1) ? null : rdr.GetString(1)).Should().NotBe("D");
    }

    [SkippableFact]
    [Trait("Category", "Integration")]
    public async Task ChangeTracking_Apply_To_Target_Should_SoftDelete_Real_Deletes()
    {
        var (sqlTarget, skipReason) = await TryResolveSqlTargetAsync();
        Skip.If(sqlTarget is null, skipReason);
        await using var _ = sqlTarget!;

        var testDbConnectionString = await InitializeTestDatabaseAsync(sqlTarget!);
        var reader = new SourceChunkReader(testDbConnectionString);

        await using var conn = new SqlConnection(testDbConnectionString);
        await conn.OpenAsync();
        await ExecuteNonQueryAsync(conn, @"
IF OBJECT_ID(N'dbo.CustomersTarget', N'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.CustomersTarget;
END;

CREATE TABLE dbo.CustomersTarget (
    Id int NOT NULL PRIMARY KEY,
    Name nvarchar(100) NULL,
    UpdatedAt datetime2(0) NULL,
    _pluck_update_datetime datetime2(0) NULL,
    _pluck_update_op nvarchar(1) NULL
);

INSERT INTO dbo.CustomersTarget (Id, Name, UpdatedAt, _pluck_update_datetime, _pluck_update_op)
VALUES (2, N'Bob', SYSUTCDATETIME(), SYSUTCDATETIME(), 'I');
");

        var fromVersion = await reader.GetChangeTrackingCurrentVersionAsync();
        fromVersion.Should().NotBeNull();

        await ExecuteNonQueryAsync(conn, @"
DELETE FROM dbo.Customers
WHERE Id = 2;
");

        var toVersion = await reader.GetChangeTrackingCurrentVersionAsync();
        toVersion.Should().NotBeNull();

        var columns = new List<SourceColumn>
        {
            new("Id", "int", false),
            new("Name", "nvarchar(100)", true),
            new("UpdatedAt", "datetime2(0)", true),
        };

        var upserts = await ToListAsync(
            reader.ReadChangeTrackingUpsertsAsync(
                sourceSql: "SELECT Id, Name, UpdatedAt FROM dbo.Customers",
                sourceTable: "dbo.Customers",
                columns: columns,
                primaryKey: ["Id"],
                lastSyncVersionExclusive: fromVersion!.Value,
                syncToVersionInclusive: toVersion!.Value));

        var deletedKeys = await ToListAsync(
            reader.ReadChangeTrackingDeletedKeysAsync(
                sourceTable: "dbo.Customers",
                primaryKey: ["Id"],
                lastSyncVersionExclusive: fromVersion.Value,
                syncToVersionInclusive: toVersion.Value));

        deletedKeys.Select(r => Convert.ToInt32(r[0]!)).Should().Contain(2);

        await ApplyChangeTrackingToTargetAsync(conn, upserts, deletedKeys);

        await using var verifyCmd = new SqlCommand(@"
SELECT _pluck_update_op
FROM dbo.CustomersTarget
WHERE Id = 2;
", conn);
        var op = await verifyCmd.ExecuteScalarAsync();
        (op as string).Should().Be("D");
    }

    [SkippableFact]
    [Trait("Category", "Integration")]
    public async Task ChangeTracking_Apply_To_Fabric_Target_Should_SoftDelete_Real_Deletes()
    {
        var (sqlTarget, sqlSkipReason) = await TryResolveSqlTargetAsync();
        Skip.If(sqlTarget is null, sqlSkipReason);
        await using var _ = sqlTarget!;

        var (fabricTarget, fabricSkipReason) = TryResolveFabricTarget();
        Skip.If(fabricTarget is null, fabricSkipReason);

        var sourceConnectionString = await InitializeTestDatabaseAsync(sqlTarget!);
        var reader = new SourceChunkReader(sourceConnectionString);
        var sourceColumns = new List<SourceColumn>
        {
            new("Id", "int", false),
            new("Name", "nvarchar(100)", true),
            new("UpdatedAt", "datetime2(0)", true),
        };
        var targetSchema = fabricTarget!.Warehouse.TargetSchema ?? "dbo";
        var targetTable = $"it_ct_customers_{Guid.NewGuid():N}".Substring(0, 24);

        var tokenProvider = new TokenProvider(fabricTarget.Auth);
        var connFactory = new WarehouseConnectionFactory(fabricTarget.Warehouse, tokenProvider, NullLogger.Instance);
        var schemaManager = new WarehouseSchemaManager(connFactory, NullLogger.Instance);
        var loader = new WarehouseLoader(connFactory, NullLogger.Instance);
        var uploader = new OneLakeUploader(fabricTarget.Staging, tokenProvider, NullLogger.Instance);
        var csvWriter = new CsvGzipWriter();
        var cleanup = new CleanupConfig
        {
            DeleteLocalTempFiles = true,
            DeleteStagedFiles = true,
            DropTempTables = true
        };

        try
        {
            await schemaManager.EnsureTableAndSchemaAsync(
                targetSchema,
                targetTable,
                sourceColumns,
                primaryKey: ["Id"]);

            var seedRows = await ToListAsync(reader.ReadColumnsAsync(
                sourceSql: "SELECT Id, Name, UpdatedAt FROM dbo.Customers",
                columnNames: ["Id", "Name", "UpdatedAt"],
                whereClause: "[Id] = 2"));
            seedRows.Should().HaveCount(1);

            await LoadUpsertsToFabricAsync(
                seedRows,
                sourceColumns,
                streamName: targetTable,
                targetSchema: targetSchema,
                targetTable: targetTable,
                loader: loader,
                csvWriter: csvWriter,
                uploader: uploader,
                cleanup: cleanup);

            var fromVersion = await reader.GetChangeTrackingCurrentVersionAsync();
            fromVersion.Should().NotBeNull();

            await using (var conn = new SqlConnection(sourceConnectionString))
            {
                await conn.OpenAsync();
                await ExecuteNonQueryAsync(conn, @"
DELETE FROM dbo.Customers
WHERE Id = 2;
");
            }

            var toVersion = await reader.GetChangeTrackingCurrentVersionAsync();
            toVersion.Should().NotBeNull();

            var deletedKeys = await ToListAsync(
                reader.ReadChangeTrackingDeletedKeysAsync(
                    sourceTable: "dbo.Customers",
                    primaryKey: ["Id"],
                    lastSyncVersionExclusive: fromVersion!.Value,
                    syncToVersionInclusive: toVersion!.Value));
            deletedKeys.Select(r => Convert.ToInt32(r[0]!)).Should().Contain(2);

            await SoftDeleteKeysInFabricAsync(
                deletedKeys,
                streamName: targetTable,
                targetSchema: targetSchema,
                targetTable: targetTable,
                loader: loader,
                csvWriter: csvWriter,
                uploader: uploader,
                cleanup: cleanup);

            await using var verifyConn = await connFactory.OpenAsync();
            await using var verifyCmd = new SqlCommand($@"
SELECT [_pluck_update_op]
FROM [{targetSchema}].[{targetTable}]
WHERE [Id] = 2;
", verifyConn);
            var updateOp = await verifyCmd.ExecuteScalarAsync();
            (updateOp as string).Should().Be("D");
        }
        finally
        {
            await TryDropWarehouseTableAsync(connFactory, targetSchema, targetTable);
        }
    }

    private static async Task<(IntegrationSqlTarget? Target, string? SkipReason)> TryResolveSqlTargetAsync()
    {
        if (ShouldUseLocalSqlServer())
        {
            var localConnectionString = Environment.GetEnvironmentVariable(LocalSqlServerConnectionEnvVar);
            if (string.IsNullOrWhiteSpace(localConnectionString))
            {
                return (null,
                    $"Integration tests are configured to use local SQL Server, but {LocalSqlServerConnectionEnvVar} is not set.");
            }

            try
            {
                await using var conn = new SqlConnection(localConnectionString);
                await conn.OpenAsync();
                return (new IntegrationSqlTarget(localConnectionString, useConfiguredDatabase: true, static () => ValueTask.CompletedTask), null);
            }
            catch (Exception ex)
            {
                return (null,
                    $"Could not connect to local SQL Server using {LocalSqlServerConnectionEnvVar}: {ex.Message}");
            }
        }

        var (container, skipReason) = await TryStartContainerAsync();
        if (container is null)
            return (null, skipReason);

        return (new IntegrationSqlTarget(container.GetConnectionString(), useConfiguredDatabase: false, () => container.DisposeAsync()), null);
    }

    private static async Task<(MsSqlContainer? Container, string? SkipReason)> TryStartContainerAsync()
    {
        MsSqlContainer container;
        try
        {
            container = new MsSqlBuilder().Build();
        }
        catch (Exception ex)
        {
            return (null, $"Docker/Testcontainers is not available: {ex.Message}");
        }

        try
        {
            await container.StartAsync();
            return (container, null);
        }
        catch (Exception ex)
        {
            await container.DisposeAsync();
            return (null, $"Docker/Testcontainers could not start SQL Server container: {ex.Message}");
        }
    }

    private static async Task<string> InitializeTestDatabaseAsync(IntegrationSqlTarget sqlTarget)
    {
        var csb = new SqlConnectionStringBuilder(sqlTarget.ConnectionString);
        var hasConfiguredDb = !string.IsNullOrWhiteSpace(csb.InitialCatalog);
        var useConfiguredDb = sqlTarget.UseConfiguredDatabase && hasConfiguredDb;

        if (!useConfiguredDb)
        {
            await using var conn = new SqlConnection(sqlTarget.ConnectionString);
            await conn.OpenAsync();
            await ExecuteNonQueryAsync(conn, @"
IF DB_ID(N'PluckCtTest') IS NULL
BEGIN
    CREATE DATABASE [PluckCtTest];
END;
");
            csb.InitialCatalog = "PluckCtTest";
        }

        var testDbConnectionString = csb.ConnectionString;

        await using var testConn = new SqlConnection(testDbConnectionString);
        await testConn.OpenAsync();
        await ExecuteNonQueryAsync(testConn, @"
IF EXISTS (
    SELECT 1
    FROM sys.change_tracking_databases
    WHERE database_id = DB_ID()
)
BEGIN
    SELECT 1;
END
ELSE
BEGIN
    DECLARE @sql nvarchar(max) =
        N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) +
        N' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);';
    EXEC sp_executesql @sql;
END;
");
        await ExecuteNonQueryAsync(testConn, @"
IF OBJECT_ID(N'dbo.Customers', N'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.Customers;
END;

CREATE TABLE dbo.Customers (
    Id int NOT NULL PRIMARY KEY,
    Name nvarchar(100) NULL,
    UpdatedAt datetime2(0) NULL
);

ALTER TABLE dbo.Customers
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = OFF);

INSERT INTO dbo.Customers (Id, Name, UpdatedAt)
VALUES (1, N'Alice', SYSUTCDATETIME()),
       (2, N'Bob', SYSUTCDATETIME());
");

        return testDbConnectionString;
    }

    private static async Task<string> InitializeTestDatabaseWithoutChangeTrackingAsync(IntegrationSqlTarget sqlTarget)
    {
        var adminCsb = new SqlConnectionStringBuilder(sqlTarget.ConnectionString)
        {
            InitialCatalog = "master"
        };

        await using (var conn = new SqlConnection(adminCsb.ConnectionString))
        {
            await conn.OpenAsync();
            await ExecuteNonQueryAsync(conn, @"
IF DB_ID(N'PluckCtTestNoCt') IS NULL
BEGIN
    CREATE DATABASE [PluckCtTestNoCt];
END;
");
        }

        var testDbCsb = new SqlConnectionStringBuilder(sqlTarget.ConnectionString)
        {
            InitialCatalog = "PluckCtTestNoCt"
        };
        var testDbConnectionString = testDbCsb.ConnectionString;

        await using var testConn = new SqlConnection(testDbConnectionString);
        await testConn.OpenAsync();
        await ExecuteNonQueryAsync(testConn, @"
IF EXISTS (
    SELECT 1
    FROM sys.change_tracking_databases
    WHERE database_id = DB_ID()
)
BEGIN
    DECLARE @sql nvarchar(max) =
        N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) +
        N' SET CHANGE_TRACKING = OFF;';
    EXEC sp_executesql @sql;
END;
");
        await ExecuteNonQueryAsync(testConn, @"
IF OBJECT_ID(N'dbo.Customers', N'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.Customers;
END;

CREATE TABLE dbo.Customers (
    Id int NOT NULL PRIMARY KEY,
    Name nvarchar(100) NULL,
    UpdatedAt datetime2(0) NULL
);
");

        return testDbConnectionString;
    }

    private static async Task<string> InitializeTestDatabaseWithDatabaseChangeTrackingOnlyAsync(IntegrationSqlTarget sqlTarget)
    {
        var csb = new SqlConnectionStringBuilder(sqlTarget.ConnectionString);
        var hasConfiguredDb = !string.IsNullOrWhiteSpace(csb.InitialCatalog);
        var useConfiguredDb = sqlTarget.UseConfiguredDatabase && hasConfiguredDb;

        if (!useConfiguredDb)
        {
            await using var conn = new SqlConnection(sqlTarget.ConnectionString);
            await conn.OpenAsync();
            await ExecuteNonQueryAsync(conn, @"
IF DB_ID(N'PluckCtDbOnlyTest') IS NULL
BEGIN
    CREATE DATABASE [PluckCtDbOnlyTest];
END;
");
            csb.InitialCatalog = "PluckCtDbOnlyTest";
        }

        var testDbConnectionString = csb.ConnectionString;

        await using var testConn = new SqlConnection(testDbConnectionString);
        await testConn.OpenAsync();
        await ExecuteNonQueryAsync(testConn, @"
IF EXISTS (
    SELECT 1
    FROM sys.change_tracking_databases
    WHERE database_id = DB_ID()
)
BEGIN
    SELECT 1;
END
ELSE
BEGIN
    DECLARE @sql nvarchar(max) =
        N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) +
        N' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);';
    EXEC sp_executesql @sql;
END;
");
        await ExecuteNonQueryAsync(testConn, @"
IF OBJECT_ID(N'dbo.Customers', N'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.Customers;
END;

CREATE TABLE dbo.Customers (
    Id int NOT NULL PRIMARY KEY,
    Name nvarchar(100) NULL,
    UpdatedAt datetime2(0) NULL
);

INSERT INTO dbo.Customers (Id, Name, UpdatedAt)
VALUES (1, N'Alice', SYSUTCDATETIME()),
       (2, N'Bob', SYSUTCDATETIME());
");

        return testDbConnectionString;
    }

    private static async Task ExecuteNonQueryAsync(SqlConnection conn, string sql)
    {
        await using var cmd = new SqlCommand(sql, conn)
        {
            CommandTimeout = 120
        };
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<object?[]>> ToListAsync(IAsyncEnumerable<object?[]> source)
    {
        var result = new List<object?[]>();
        await foreach (var row in source)
            result.Add(row);
        return result;
    }

    private static async Task ApplyChangeTrackingToTargetAsync(
        SqlConnection conn,
        List<object?[]> upserts,
        List<object?[]> deletedKeys)
    {
        foreach (var row in upserts)
        {
            await using var upsertCmd = new SqlCommand(@"
UPDATE dbo.CustomersTarget
SET Name = @name,
    UpdatedAt = @updatedAt,
    _pluck_update_datetime = SYSUTCDATETIME(),
    _pluck_update_op = 'U'
WHERE Id = @id;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.CustomersTarget (Id, Name, UpdatedAt, _pluck_update_datetime, _pluck_update_op)
    VALUES (@id, @name, @updatedAt, SYSUTCDATETIME(), 'I');
END;
", conn);
            upsertCmd.Parameters.AddWithValue("@id", Convert.ToInt32(row[0]!));
            upsertCmd.Parameters.AddWithValue("@name", row[1] ?? DBNull.Value);
            upsertCmd.Parameters.AddWithValue("@updatedAt", row[2] ?? DBNull.Value);
            await upsertCmd.ExecuteNonQueryAsync();
        }

        foreach (var row in deletedKeys)
        {
            await using var deleteCmd = new SqlCommand(@"
UPDATE dbo.CustomersTarget
SET _pluck_update_datetime = SYSUTCDATETIME(),
    _pluck_update_op = 'D'
WHERE Id = @id
  AND ISNULL(_pluck_update_op, '') <> 'D';
", conn);
            deleteCmd.Parameters.AddWithValue("@id", Convert.ToInt32(row[0]!));
            await deleteCmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task LoadUpsertsToFabricAsync(
        List<object?[]> rows,
        List<SourceColumn> columns,
        string streamName,
        string targetSchema,
        string targetTable,
        WarehouseLoader loader,
        CsvGzipWriter csvWriter,
        OneLakeUploader uploader,
        CleanupConfig cleanup)
    {
        var runId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
        var relativeName = $"{streamName}/itest/{runId}/upserts.csv.gz";
        var localPath = Path.Combine(Path.GetTempPath(), "pluck-itest", Guid.NewGuid().ToString("N"), "upserts.csv.gz");
        Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);

        try
        {
            var writeResult = await csvWriter.WriteCsvGzAsync(localPath, columns, ToAsyncEnumerable(rows));
            var sourceUrl = await uploader.UploadAsync(localPath, relativeName);
            var tempTable = $"__tmp_{streamName}_{Guid.NewGuid():N}".Substring(0, 30);

            await loader.LoadAndMergeAsync(
                targetSchema: targetSchema,
                targetTable: targetTable,
                tempTable: tempTable,
                columns: columns,
                primaryKey: ["Id"],
                expectedRowCount: writeResult.RowCount,
                oneLakeDfsUrl: sourceUrl,
                stagingFileFormat: "csv.gz",
                cleanup: cleanup);

            if (cleanup.DeleteStagedFiles)
                await uploader.TryDeleteAsync(relativeName);
        }
        finally
        {
            if (cleanup.DeleteLocalTempFiles)
                TryDeleteLocalTempFileAndDirectory(localPath);
        }
    }

    private static async Task SoftDeleteKeysInFabricAsync(
        List<object?[]> deletedKeys,
        string streamName,
        string targetSchema,
        string targetTable,
        WarehouseLoader loader,
        CsvGzipWriter csvWriter,
        OneLakeUploader uploader,
        CleanupConfig cleanup)
    {
        var keyColumns = new List<SourceColumn> { new("Id", "int", false) };
        var runId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
        var relativeName = $"{streamName}/itest/{runId}/delete-keys.csv.gz";
        var localPath = Path.Combine(Path.GetTempPath(), "pluck-itest", Guid.NewGuid().ToString("N"), "delete-keys.csv.gz");
        Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);

        try
        {
            var writeResult = await csvWriter.WriteCsvGzAsync(localPath, keyColumns, ToAsyncEnumerable(deletedKeys));
            var sourceUrl = await uploader.UploadAsync(localPath, relativeName);
            var tempTable = $"__tmp_del_{streamName}_{Guid.NewGuid():N}".Substring(0, 30);

            await loader.SoftDeleteByKeysAsync(
                targetSchema: targetSchema,
                targetTable: targetTable,
                sourceKeysTempTable: tempTable,
                primaryKeyColumns: keyColumns,
                expectedSourceKeyRowCount: writeResult.RowCount,
                sourceKeysDfsUrl: sourceUrl,
                sourceKeysFileFormat: "csv.gz",
                cleanup: cleanup);

            if (cleanup.DeleteStagedFiles)
                await uploader.TryDeleteAsync(relativeName);
        }
        finally
        {
            if (cleanup.DeleteLocalTempFiles)
                TryDeleteLocalTempFileAndDirectory(localPath);
        }
    }

    private static async Task TryDropWarehouseTableAsync(WarehouseConnectionFactory factory, string schema, string table)
    {
        try
        {
            await using var conn = await factory.OpenAsync();
            await using var cmd = new SqlCommand($@"
IF OBJECT_ID(N'{schema}.{table}', N'U') IS NOT NULL
BEGIN
    DROP TABLE [{schema}].[{table}];
END;
", conn);
            await cmd.ExecuteNonQueryAsync();
        }
        catch
        {
            // Best effort cleanup.
        }
    }

    private static async IAsyncEnumerable<object?[]> ToAsyncEnumerable(List<object?[]> rows)
    {
        foreach (var row in rows)
        {
            yield return row;
            await Task.Yield();
        }
    }

    private static void TryDeleteLocalTempFileAndDirectory(string localPath)
    {
        try
        {
            if (File.Exists(localPath))
                File.Delete(localPath);

            var dir = Path.GetDirectoryName(localPath);
            if (!string.IsNullOrWhiteSpace(dir) && Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
        catch
        {
            // best effort
        }
    }

    private static bool ShouldUseLocalSqlServer()
    {
        var value = Environment.GetEnvironmentVariable(UseLocalSqlServerEnvVar);
        return bool.TryParse(value, out var parsed) && parsed;
    }

    private static (FabricIntegrationTarget? Target, string? SkipReason) TryResolveFabricTarget()
    {
        var useFabricWarehouse = Environment.GetEnvironmentVariable(UseFabricWarehouseEnvVar);
        if (!bool.TryParse(useFabricWarehouse, out var enabled) || !enabled)
            return (null, $"{UseFabricWarehouseEnvVar} is not enabled.");

        EnvironmentConfig? fileConfig = null;
        var connectionsPath = Environment.GetEnvironmentVariable(TestConnectionsFileEnvVar) ?? "connections.yaml";
        var resolvedConnectionsPath = ResolveExistingPath(connectionsPath);
        var envName = Environment.GetEnvironmentVariable(TestEnvironmentNameEnvVar) ?? "dev";
        if (resolvedConnectionsPath is not null)
        {
            try
            {
                var loader = new YamlLoader();
                var root = loader.Load<ConnectionsRoot>(resolvedConnectionsPath);
                root.Environments.TryGetValue(envName, out fileConfig);
            }
            catch (Exception ex)
            {
                return (null, $"Failed to load Fabric test config from {resolvedConnectionsPath}: {ex.Message}");
            }
        }

        var server = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricWarehouseServerEnvVar), fileConfig?.FabricWarehouse?.Server);
        var database = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricWarehouseDatabaseEnvVar), fileConfig?.FabricWarehouse?.Database);
        var targetSchema = FirstNonEmpty(
            Environment.GetEnvironmentVariable(FabricWarehouseTargetSchemaEnvVar),
            fileConfig?.FabricWarehouse?.TargetSchema,
            "dbo");
        var tenantId = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricTenantIdEnvVar), fileConfig?.Auth?.TenantId);
        var clientId = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricClientIdEnvVar), fileConfig?.Auth?.ClientId);
        var clientSecret = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricClientSecretEnvVar), fileConfig?.Auth?.ClientSecret);

        var dataLakeUrl = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricDataLakeUrlEnvVar), fileConfig?.OneLakeStaging?.DataLakeUrl);
        var workspaceId = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricWorkspaceIdEnvVar), fileConfig?.OneLakeStaging?.WorkspaceId);
        var lakehouseId = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricLakehouseIdEnvVar), fileConfig?.OneLakeStaging?.LakehouseId);
        var filesPath = FirstNonEmpty(Environment.GetEnvironmentVariable(FabricFilesPathEnvVar), fileConfig?.OneLakeStaging?.FilesPath);

        if (string.IsNullOrWhiteSpace(server) ||
            string.IsNullOrWhiteSpace(database) ||
            string.IsNullOrWhiteSpace(tenantId) ||
            string.IsNullOrWhiteSpace(clientId) ||
            string.IsNullOrWhiteSpace(clientSecret))
        {
            return (null,
                $"Missing Fabric config. Set env vars or provide {connectionsPath} env '{envName}' with fabricWarehouse + auth. Required keys: server, database, tenantId, clientId, clientSecret.");
        }

        var hasDataLakeUrl = !string.IsNullOrWhiteSpace(dataLakeUrl);
        var hasWorkspaceMode = !string.IsNullOrWhiteSpace(workspaceId) && !string.IsNullOrWhiteSpace(lakehouseId);
        if (!hasDataLakeUrl && !hasWorkspaceMode)
        {
            return (null,
                $"Missing staging config. Set {FabricDataLakeUrlEnvVar} or both {FabricWorkspaceIdEnvVar} + {FabricLakehouseIdEnvVar}, or configure oneLakeStaging in {connectionsPath} env '{envName}'.");
        }

        var target = new FabricIntegrationTarget(
            new FabricWarehouseConfig
            {
                Server = server!,
                Database = database!,
                TargetSchema = targetSchema
            },
            new OneLakeStagingConfig
            {
                DataLakeUrl = dataLakeUrl,
                WorkspaceId = workspaceId ?? "",
                LakehouseId = lakehouseId ?? "",
                FilesPath = string.IsNullOrWhiteSpace(filesPath) ? "staging/incremental-repl/itest" : filesPath!
            },
            new AuthConfig
            {
                TenantId = tenantId!,
                ClientId = clientId!,
                ClientSecret = clientSecret!
            });

        return (target, null);
    }

    private sealed class IntegrationSqlTarget(string connectionString, bool useConfiguredDatabase, Func<ValueTask> disposeAsync) : IAsyncDisposable
    {
        public string ConnectionString { get; } = connectionString;
        public bool UseConfiguredDatabase { get; } = useConfiguredDatabase;
        private readonly Func<ValueTask> _disposeAsync = disposeAsync;

        public async ValueTask DisposeAsync()
        {
            await _disposeAsync();
        }
    }

    private sealed record FabricIntegrationTarget(
        FabricWarehouseConfig Warehouse,
        OneLakeStagingConfig Staging,
        AuthConfig Auth);

    private static string? FirstNonEmpty(params string?[] values)
    {
        foreach (var value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
                return value;
        }

        return null;
    }

    private static string? ResolveExistingPath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;

        if (Path.IsPathRooted(path))
            return File.Exists(path) ? path : null;

        var fromCurrentDir = Path.GetFullPath(path, Directory.GetCurrentDirectory());
        if (File.Exists(fromCurrentDir))
            return fromCurrentDir;

        var fromBaseDir = Path.GetFullPath(path, AppContext.BaseDirectory);
        if (File.Exists(fromBaseDir))
            return fromBaseDir;

        var current = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (current is not null)
        {
            var candidate = Path.Combine(current.FullName, path);
            if (File.Exists(candidate))
                return candidate;
            current = current.Parent;
        }

        return null;
    }
}
