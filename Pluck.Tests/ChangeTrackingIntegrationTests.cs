using FluentAssertions;
using Microsoft.Data.SqlClient;
using Pluck.Source;
using Testcontainers.MsSql;
using Xunit;

namespace Pluck.Tests;

public sealed class ChangeTrackingIntegrationTests
{
    [SkippableFact]
    [Trait("Category", "Integration")]
    public async Task SourceChunkReader_Should_Read_ChangeTracking_Upserts_And_Deletes()
    {
        var (sqlContainer, skipReason) = await TryStartContainerAsync();
        Skip.If(sqlContainer is null, skipReason);
        await using var _ = sqlContainer!;

        var testDbConnectionString = await InitializeTestDatabaseAsync(sqlContainer!.GetConnectionString());
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

    private static async Task<string> InitializeTestDatabaseAsync(string masterConnectionString)
    {
        await using (var conn = new SqlConnection(masterConnectionString))
        {
            await conn.OpenAsync();
            await ExecuteNonQueryAsync(conn, @"
IF DB_ID(N'PluckCtTest') IS NULL
BEGIN
    CREATE DATABASE [PluckCtTest];
END;
");
            await ExecuteNonQueryAsync(conn, @"
ALTER DATABASE [PluckCtTest]
SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
");
        }

        var csb = new SqlConnectionStringBuilder(masterConnectionString)
        {
            InitialCatalog = "PluckCtTest"
        };
        var testDbConnectionString = csb.ConnectionString;

        await using var testConn = new SqlConnection(testDbConnectionString);
        await testConn.OpenAsync();
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
}
