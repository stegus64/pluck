using FluentAssertions;
using Pluck.Source;
using Pluck.Target;

namespace Pluck.Tests;

public class MergeBuilderTests
{
    [Fact]
    public void BuildMergeSql_Should_Include_Metadata_Columns_And_Exclude_Pk_From_Update_Set()
    {
        var columns = new List<SourceColumn>
        {
            new("Id", "int", true),
            new("Name", "nvarchar(100)", true),
            new("UpdatedAt", "datetime2(0)", true),
        };

        var sql = MergeBuilder.BuildMergeSql(
            schema: "dbo",
            targetTable: "Customers",
            tempTable: "__tmp_Customers",
            columns: columns,
            primaryKey: ["Id"]);

        sql.Should().Contain("ON t.[Id] = s.[Id]");
        sql.Should().NotContain("t.[Id] = s.[Id],");
        sql.Should().Contain("t.[Name] = s.[Name]");
        sql.Should().Contain("t.[UpdatedAt] = s.[UpdatedAt]");
        sql.Should().Contain("t.[_pluck_update_datetime] = SYSUTCDATETIME()");
        sql.Should().Contain("t.[_pluck_update_op] = 'U'");
        sql.Should().Contain("INSERT ([Id], [Name], [UpdatedAt], [_pluck_update_datetime], [_pluck_update_op])");
        sql.Should().Contain("VALUES (s.[Id], s.[Name], s.[UpdatedAt], SYSUTCDATETIME(), 'I')");
    }

    [Fact]
    public void BuildMergeSql_Should_Use_Composite_Primary_Key_In_On_Clause()
    {
        var columns = new List<SourceColumn>
        {
            new("TenantId", "int", true),
            new("OrderId", "int", true),
            new("Amount", "decimal(18,2)", true),
        };

        var sql = MergeBuilder.BuildMergeSql(
            schema: "sales",
            targetTable: "Orders",
            tempTable: "__tmp_Orders",
            columns: columns,
            primaryKey: ["TenantId", "OrderId"]);

        sql.Should().Contain("ON t.[TenantId] = s.[TenantId] AND t.[OrderId] = s.[OrderId]");
        sql.Should().Contain("t.[Amount] = s.[Amount]");
    }
}
