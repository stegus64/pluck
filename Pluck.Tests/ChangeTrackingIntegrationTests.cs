using FluentAssertions;
using Microsoft.Data.SqlClient;
using Pluck.Source;
using Testcontainers.MsSql;
using Xunit;

namespace Pluck.Tests;

public sealed class ChangeTrackingIntegrationTests
{
    private const string UseLocalSqlServerEnvVar = "PLUCK_ITEST_USE_LOCAL_SQLSERVER";
    private const string LocalSqlServerConnectionEnvVar = "PLUCK_ITEST_SQLSERVER";

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

    private static bool ShouldUseLocalSqlServer()
    {
        var value = Environment.GetEnvironmentVariable(UseLocalSqlServerEnvVar);
        return bool.TryParse(value, out var parsed) && parsed;
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
}
