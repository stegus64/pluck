namespace FabricIncrementalReplicator.Config;

public sealed class ConnectionsRoot
{
    public Dictionary<string, EnvironmentConfig> Environments { get; set; } = new();
}

public sealed class EnvironmentConfig
{
    public Dictionary<string, SourceSqlConfig> SourceConnections { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public FabricWarehouseConfig FabricWarehouse { get; set; } = new();
    public OneLakeStagingConfig OneLakeStaging { get; set; } = new();
    public AuthConfig Auth { get; set; } = new();
    public CleanupConfig Cleanup { get; set; } = new();
    public SchemaDiscoveryConfig SchemaDiscovery { get; set; } = new();
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

public sealed class SchemaDiscoveryConfig
{
    // "DescribeFirstResultSet" | "FmtOnly"
    public string Mode { get; set; } = "DescribeFirstResultSet";
}
