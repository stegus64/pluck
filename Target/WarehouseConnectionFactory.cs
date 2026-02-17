using FabricIncrementalReplicator.Auth;
using FabricIncrementalReplicator.Config;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FabricIncrementalReplicator.Target;

public sealed class WarehouseConnectionFactory
{
    private readonly FabricWarehouseConfig _cfg;
    private readonly TokenProvider _tokenProvider;
    private readonly Microsoft.Extensions.Logging.ILogger<WarehouseConnectionFactory> _log;

    public WarehouseConnectionFactory(FabricWarehouseConfig cfg, TokenProvider tokenProvider, Microsoft.Extensions.Logging.ILogger<WarehouseConnectionFactory>? log = null)
    {
        _cfg = cfg;
        _tokenProvider = tokenProvider;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WarehouseConnectionFactory>.Instance;
    }

    public async Task<SqlConnection> OpenAsync(CancellationToken ct = default)
    {
        var cs = new SqlConnectionStringBuilder
        {
            DataSource = _cfg.Server,
            InitialCatalog = _cfg.Database,
            Encrypt = true,
            TrustServerCertificate = false
        }.ConnectionString;

        var conn = new SqlConnection(cs);

        // Fabric Warehouse uses Entra ID; use AAD token for "https://database.windows.net/.default"
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("Requesting AAD token for SQL");
        var token = await _tokenProvider.GetTokenAsync(new[] { "https://database.windows.net/.default" }, ct);
        conn.AccessToken = token;
        _log.LogDebug("Acquired token (masked)");
        await conn.OpenAsync(ct);
        sw.Stop();
        _log.LogDebug("SQL OpenAsync elapsed: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);
        return conn;
    }
}