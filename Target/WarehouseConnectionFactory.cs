using FabricIncrementalReplicator.Auth;
using FabricIncrementalReplicator.Config;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FabricIncrementalReplicator.Target;

public sealed class WarehouseConnectionFactory
{
    private readonly FabricWarehouseConfig _cfg;
    private readonly TokenProvider _tokenProvider;
    private readonly Microsoft.Extensions.Logging.ILogger _log;

    public WarehouseConnectionFactory(FabricWarehouseConfig cfg, TokenProvider tokenProvider, Microsoft.Extensions.Logging.ILogger? log = null)
    {
        _cfg = cfg;
        _tokenProvider = tokenProvider;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public int CommandTimeoutSeconds => _cfg.CommandTimeoutSeconds > 0 ? _cfg.CommandTimeoutSeconds : 3600;

    public async Task<SqlConnection> OpenAsync(string? logPrefix = null, CancellationToken ct = default)
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
        _log.LogDebug("{LogPrefix}Requesting AAD token for SQL", Prefix(logPrefix));
        var token = await _tokenProvider.GetTokenAsync(new[] { "https://database.windows.net/.default" }, ct);
        conn.AccessToken = token;
        _log.LogDebug("{LogPrefix}Acquired token (masked)", Prefix(logPrefix));
        await conn.OpenAsync(ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}SQL OpenAsync elapsed: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);
        return conn;
    }

    private static string Prefix(string? logPrefix) =>
        string.IsNullOrWhiteSpace(logPrefix) ? "[app] " : $"{logPrefix} ";
}
