namespace Pluck.Config;

public sealed class ConnectionsRoot
{
    public Dictionary<string, EnvironmentConfig> Environments { get; set; } = new();
}

public sealed class EnvironmentConfig
{
    public Dictionary<string, SourceSqlConfig> SourceConnections { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, DestinationConnectionConfig> DestinationConnections { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public FabricWarehouseConfig FabricWarehouse { get; set; } = new();
    public OneLakeStagingConfig OneLakeStaging { get; set; } = new();
    public AuthConfig Auth { get; set; } = new();
    public CleanupConfig Cleanup { get; set; } = new();

    public Dictionary<string, DestinationConnectionConfig> GetResolvedDestinationConnections()
    {
        if (DestinationConnections.Count > 0)
            return DestinationConnections;

        // Backward compatibility for older connections.yaml shape.
        if (!string.IsNullOrWhiteSpace(FabricWarehouse.Server) &&
            !string.IsNullOrWhiteSpace(FabricWarehouse.Database))
        {
            return new Dictionary<string, DestinationConnectionConfig>(StringComparer.OrdinalIgnoreCase)
            {
                ["fabric"] = new()
                {
                    Type = "fabricWarehouse",
                    FabricWarehouse = FabricWarehouse,
                    OneLakeStaging = OneLakeStaging,
                    Auth = Auth
                }
            };
        }

        return new Dictionary<string, DestinationConnectionConfig>(StringComparer.OrdinalIgnoreCase);
    }
}

public sealed class DestinationConnectionConfig
{
    public string Type { get; set; } = "fabricWarehouse";
    public FabricWarehouseConfig FabricWarehouse { get; set; } = new();
    public OneLakeStagingConfig OneLakeStaging { get; set; } = new();
    public AuthConfig Auth { get; set; } = new();
    public SqlServerDestinationConfig SqlServer { get; set; } = new();
}

public sealed class SourceSqlConfig
{
    public string ConnectionString { get; set; } = "";
    public int CommandTimeoutSeconds { get; set; } = 3600;
}

public sealed class FabricWarehouseConfig
{
    public string Server { get; set; } = "";   // host
    public string Database { get; set; } = "";
    public string? TargetSchema { get; set; }  // default schema
    public int CommandTimeoutSeconds { get; set; } = 3600;
}

public sealed class OneLakeStagingConfig
{
    // Optional full ADLS Gen2 DFS URL, e.g.
    // https://myaccount.dfs.core.windows.net/myfilesystem/staging/incremental-repl
    // If set, this is used for staging uploads/COPY paths.
    public string? DataLakeUrl { get; set; }
    public string WorkspaceId { get; set; } = "";
    public string LakehouseId { get; set; } = "";
    public string FilesPath { get; set; } = "staging/incremental-repl";
}

public sealed class AuthConfig
{
    public string TenantId { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string ClientSecret { get; set; } = "";
}

public sealed class CleanupConfig
{
    public bool DeleteLocalTempFiles { get; set; } = true;
    public bool DeleteStagedFiles { get; set; } = true;
    public bool DropTempTables { get; set; } = true;
}

public sealed class SqlServerDestinationConfig
{
    public string ConnectionString { get; set; } = "";
    public string? TargetSchema { get; set; }
    public int CommandTimeoutSeconds { get; set; } = 3600;
    public BulkLoadConfig BulkLoad { get; set; } = new();
}

public sealed class BulkLoadConfig
{
    public string Method { get; set; } = "SqlBulkCopy";
    public int BatchSize { get; set; } = 10000;
    public int BulkCopyTimeoutSeconds { get; set; } = 0;
}

