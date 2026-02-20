using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Pluck.Config;

namespace Pluck.Target;

public sealed class SqlServerDestinationConnectionFactory
{
    private readonly SqlServerDestinationConfig _cfg;
    private readonly ILogger _log;

    public SqlServerDestinationConnectionFactory(SqlServerDestinationConfig cfg, ILogger? log = null)
    {
        _cfg = cfg;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public int CommandTimeoutSeconds => _cfg.CommandTimeoutSeconds > 0 ? _cfg.CommandTimeoutSeconds : 3600;
    public BulkLoadConfig BulkLoad => _cfg.BulkLoad;

    public async Task<SqlConnection> OpenAsync(string? logPrefix = null, CancellationToken ct = default)
    {
        var conn = new SqlConnection(_cfg.ConnectionString);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await conn.OpenAsync(ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}SQL Server destination OpenAsync elapsed: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);
        return conn;
    }

    private static string Prefix(string? logPrefix) =>
        string.IsNullOrWhiteSpace(logPrefix) ? "[app] " : $"{logPrefix} ";
}
