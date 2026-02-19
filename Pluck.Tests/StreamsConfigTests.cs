using FluentAssertions;
using Pluck.Config;

namespace Pluck.Tests;

public class StreamsConfigTests
{
    [Fact]
    public void GetMaxParallelStreams_Should_Default_To_One()
    {
        var cfg = new StreamsConfig();

        cfg.GetMaxParallelStreams().Should().Be(1);
    }

    [Fact]
    public void GetMaxParallelStreams_Should_Throw_For_Invalid_Value()
    {
        var cfg = new StreamsConfig { MaxParallelStreams = 0 };

        var act = () => cfg.GetMaxParallelStreams();

        act.Should().Throw<Exception>().WithMessage("*maxParallelStreams*>= 1*");
    }

    [Fact]
    public void GetResolvedStreams_Should_Merge_Defaults_And_Apply_Stream_Tokens()
    {
        var cfg = new StreamsConfig
        {
            Defaults = new StreamConfig
            {
                SourceConnection = "landSql",
                SourceSql = "SELECT * FROM dbo.{stream_table}",
                TargetTable = "{stream_table}",
                PrimaryKey = ["DW_Rowid"],
                UpdateKey = "DW_Rowid",
                StagingFileFormat = "csv.gz",
            },
            Streams = new Dictionary<string, StreamConfig>(StringComparer.OrdinalIgnoreCase)
            {
                ["M3_MITMAS"] = new()
                {
                    ExcludeColumns = ["DW_Version"],
                    ChangeTracking = new ChangeTrackingConfig
                    {
                        Enabled = true,
                        SourceTable = "dbo.{stream_table}"
                    }
                }
            }
        };

        var stream = cfg.GetResolvedStreams().Single();

        stream.Name.Should().Be("M3_MITMAS");
        stream.SourceSql.Should().Be("SELECT * FROM dbo.M3_MITMAS");
        stream.TargetTable.Should().Be("M3_MITMAS");
        stream.ChangeTracking.Enabled.Should().BeTrue();
        stream.ChangeTracking.SourceTable.Should().Be("dbo.M3_MITMAS");
    }

    [Fact]
    public void GetResolvedStreams_Should_Throw_When_ChangeTracking_Enabled_Without_SourceTable()
    {
        var cfg = new StreamsConfig
        {
            Defaults = BaseDefaults(),
            Streams = new Dictionary<string, StreamConfig>(StringComparer.OrdinalIgnoreCase)
            {
                ["M3_MITMAS"] = new()
                {
                    ChangeTracking = new ChangeTrackingConfig { Enabled = true }
                }
            }
        };

        var act = () => cfg.GetResolvedStreams();

        act.Should().Throw<Exception>().WithMessage("*change_tracking.sourceTable*");
    }

    [Fact]
    public void GetResolvedStreams_Should_Throw_When_ChangeTracking_SourceTable_Is_Invalid()
    {
        var cfg = new StreamsConfig
        {
            Defaults = BaseDefaults(),
            Streams = new Dictionary<string, StreamConfig>(StringComparer.OrdinalIgnoreCase)
            {
                ["M3_MITMAS"] = new()
                {
                    ChangeTracking = new ChangeTrackingConfig
                    {
                        Enabled = true,
                        SourceTable = "dbo.M3_MITMAS; DROP TABLE x;"
                    }
                }
            }
        };

        var act = () => cfg.GetResolvedStreams();

        act.Should().Throw<Exception>().WithMessage("*Invalid stream setting 'change_tracking.sourceTable'*");
    }

    private static StreamConfig BaseDefaults() =>
        new()
        {
            SourceConnection = "landSql",
            SourceSql = "SELECT * FROM dbo.{stream_table}",
            TargetTable = "{stream_table}",
            PrimaryKey = ["DW_Rowid"],
            UpdateKey = "DW_Rowid",
            StagingFileFormat = "csv.gz"
        };
}
