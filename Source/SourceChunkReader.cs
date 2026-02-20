using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Pluck.Util;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace Pluck.Source;

public sealed class SourceChunkReader
{
    private readonly string _sourceType;
    private readonly string _connString;
    private readonly int _commandTimeoutSeconds;
    private readonly ILogger _log;

    public SourceChunkReader(
        string connString,
        int commandTimeoutSeconds = 3600,
        ILogger? log = null,
        string sourceType = SourceProvider.SqlServer)
    {
        _sourceType = SourceProvider.NormalizeSourceType(sourceType);
        _connString = connString;
        _commandTimeoutSeconds = commandTimeoutSeconds > 0 ? commandTimeoutSeconds : 3600;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public async Task<(object? Min, object? Max)> GetMinMaxUpdateKeyAsync(string sourceSql, string updateKey, object? watermark, string? logPrefix = null)
    {
        var quotedUpdateKey = SourceProvider.QuoteIdentifier(_sourceType, updateKey);
        var watermarkPlaceholder = BuildParameterPlaceholder("watermark");
        var sql = $@"
WITH src AS (
    {sourceSql}
)
SELECT MIN({quotedUpdateKey}) AS MinVal, MAX({quotedUpdateKey}) AS MaxVal
FROM src
WHERE ({watermarkPlaceholder} IS NULL OR {quotedUpdateKey} > {watermarkPlaceholder});
";

        await using var conn = SourceProvider.CreateConnection(_sourceType, _connString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;
        AddParameter(cmd, "watermark", watermark ?? DBNull.Value);
        if (SourceProvider.UsesPositionalParameters(_sourceType))
            AddParameter(cmd, "watermark", watermark ?? DBNull.Value);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("{LogPrefix}GetMinMax execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL GetMinMax: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL GetMinMax Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await using var rdr = await cmd.ExecuteReaderAsync();
        sw.Stop();
        _log.LogDebug("{LogPrefix}GetMinMax execution time: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);
        if (!await rdr.ReadAsync())
            return (null, null);

        return (
            rdr.IsDBNull(0) ? null : rdr.GetValue(0),
            rdr.IsDBNull(1) ? null : rdr.GetValue(1));
    }

    public async IAsyncEnumerable<object?[]> ReadChunkByIntervalAsync(
        string sourceSql,
        List<SourceColumn> columns,
        string updateKey,
        object lowerBoundInclusive,
        object upperBoundExclusive,
        string? logPrefix = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var selectList = string.Join(", ", columns.Select(c => SourceProvider.QuoteIdentifier(_sourceType, c.Name)));
        var quotedUpdateKey = SourceProvider.QuoteIdentifier(_sourceType, updateKey);
        var lowerBoundPlaceholder = BuildParameterPlaceholder("lowerBound");
        var upperBoundPlaceholder = BuildParameterPlaceholder("upperBound");

        var sql = $@"
WITH src AS (
    {sourceSql}
)
SELECT {selectList}
FROM src
WHERE {quotedUpdateKey} >= {lowerBoundPlaceholder}
  AND {quotedUpdateKey} < {upperBoundPlaceholder};
";

        await using var conn = SourceProvider.CreateConnection(_sourceType, _connString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;
        AddParameter(cmd, "lowerBound", lowerBoundInclusive);
        AddParameter(cmd, "upperBound", upperBoundExclusive);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("{LogPrefix}ReadChunkByInterval execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL ReadChunkByInterval: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL ReadChunkByInterval Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}ReadChunkByInterval execution time: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);

        while (await rdr.ReadAsync(ct))
        {
            var values = new object?[columns.Count];
            for (var i = 0; i < columns.Count; i++)
                values[i] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);

            yield return values;
        }
    }

    public async IAsyncEnumerable<object?[]> ReadChunkFromLowerBoundAsync(
        string sourceSql,
        List<SourceColumn> columns,
        string updateKey,
        object lowerBoundInclusive,
        object maxInclusive,
        string? logPrefix = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var selectList = string.Join(", ", columns.Select(c => SourceProvider.QuoteIdentifier(_sourceType, c.Name)));
        var quotedUpdateKey = SourceProvider.QuoteIdentifier(_sourceType, updateKey);
        var lowerBoundPlaceholder = BuildParameterPlaceholder("lowerBound");
        var maxInclusivePlaceholder = BuildParameterPlaceholder("maxInclusive");

        var sql = $@"
WITH src AS (
    {sourceSql}
)
SELECT {selectList}
FROM src
WHERE {quotedUpdateKey} >= {lowerBoundPlaceholder}
  AND {quotedUpdateKey} <= {maxInclusivePlaceholder};
";

        await using var conn = SourceProvider.CreateConnection(_sourceType, _connString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;
        AddParameter(cmd, "lowerBound", lowerBoundInclusive);
        AddParameter(cmd, "maxInclusive", maxInclusive);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("{LogPrefix}ReadChunkFromLowerBound execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL ReadChunkFromLowerBound: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL ReadChunkFromLowerBound Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}ReadChunkFromLowerBound execution time: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);

        while (await rdr.ReadAsync(ct))
        {
            var values = new object?[columns.Count];
            for (var i = 0; i < columns.Count; i++)
                values[i] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);

            yield return values;
        }
    }

    public async IAsyncEnumerable<object?[]> ReadColumnsAsync(
        string sourceSql,
        List<string> columnNames,
        string? whereClause = null,
        string? logPrefix = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var selectList = string.Join(", ", columnNames.Select(c => SourceProvider.QuoteIdentifier(_sourceType, c)));
        var whereSql = string.IsNullOrWhiteSpace(whereClause) ? string.Empty : $"\nWHERE ({whereClause})";

        var sql = $@"
WITH src AS (
    {sourceSql}
)
SELECT {selectList}
FROM src{whereSql};
";

        await using var conn = SourceProvider.CreateConnection(_sourceType, _connString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("{LogPrefix}ReadColumns execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL ReadColumns: {Sql}", Prefix(logPrefix), sql);
        await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}ReadColumns execution time: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);

        while (await rdr.ReadAsync(ct))
        {
            var values = new object?[columnNames.Count];
            for (var i = 0; i < columnNames.Count; i++)
                values[i] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);

            yield return values;
        }
    }

    public async Task<long?> GetChangeTrackingCurrentVersionAsync(string? logPrefix = null, CancellationToken ct = default)
    {
        EnsureChangeTrackingSupported();

        const string sql = "SELECT CHANGE_TRACKING_CURRENT_VERSION();";
        await using var conn = SourceProvider.CreateConnection(_sourceType, _connString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;
        _log.LogDebug("{LogPrefix}GetChangeTrackingCurrentVersion execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL GetChangeTrackingCurrentVersion: {Sql}", Prefix(logPrefix), sql);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = await cmd.ExecuteScalarAsync(ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}GetChangeTrackingCurrentVersion execution time: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);
        return result is null or DBNull ? null : Convert.ToInt64(result);
    }

    public async Task<long?> GetChangeTrackingMinValidVersionAsync(string sourceTable, string? logPrefix = null, CancellationToken ct = default)
    {
        EnsureChangeTrackingSupported();

        var sql = $"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(N'{sourceTable}'));";
        await using var conn = SourceProvider.CreateConnection(_sourceType, _connString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;
        _log.LogDebug("{LogPrefix}GetChangeTrackingMinValidVersion execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL GetChangeTrackingMinValidVersion: {Sql}", Prefix(logPrefix), sql);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = await cmd.ExecuteScalarAsync(ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}GetChangeTrackingMinValidVersion execution time: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);
        return result is null or DBNull ? null : Convert.ToInt64(result);
    }

    public async IAsyncEnumerable<object?[]> ReadChangeTrackingUpsertsAsync(
        string sourceSql,
        string sourceTable,
        List<SourceColumn> columns,
        List<string> primaryKey,
        long lastSyncVersionExclusive,
        long syncToVersionInclusive,
        string? logPrefix = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        EnsureChangeTrackingSupported();

        var selectList = string.Join(", ", columns.Select(c => $"src.{SourceProvider.QuoteIdentifier(_sourceType, c.Name)}"));
        var pkJoin = string.Join(" AND ", primaryKey.Select(pk => $"src.{SourceProvider.QuoteIdentifier(_sourceType, pk)} = ct.{SourceProvider.QuoteIdentifier(_sourceType, pk)}"));
        var lastSyncPlaceholder = BuildParameterPlaceholder("lastSyncVersion");
        var syncToPlaceholder = BuildParameterPlaceholder("syncToVersion");

        var sql = $@"
WITH src AS (
    {sourceSql}
)
SELECT {selectList}
FROM src
INNER JOIN CHANGETABLE(CHANGES {sourceTable}, {lastSyncPlaceholder}) AS ct
    ON {pkJoin}
WHERE ct.SYS_CHANGE_OPERATION IN ('I', 'U')
  AND ct.SYS_CHANGE_VERSION <= {syncToPlaceholder};
";

        await using var conn = SourceProvider.CreateConnection(_sourceType, _connString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;
        AddParameter(cmd, "lastSyncVersion", lastSyncVersionExclusive);
        AddParameter(cmd, "syncToVersion", syncToVersionInclusive);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("{LogPrefix}ReadChangeTrackingUpserts execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL ReadChangeTrackingUpserts: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL ReadChangeTrackingUpserts Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}ReadChangeTrackingUpserts execution time: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);

        while (await rdr.ReadAsync(ct))
        {
            var values = new object?[columns.Count];
            for (var i = 0; i < columns.Count; i++)
                values[i] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);

            yield return values;
        }
    }

    public async IAsyncEnumerable<object?[]> ReadChangeTrackingDeletedKeysAsync(
        string sourceTable,
        List<string> primaryKey,
        long lastSyncVersionExclusive,
        long syncToVersionInclusive,
        string? logPrefix = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        EnsureChangeTrackingSupported();

        var selectList = string.Join(", ", primaryKey.Select(pk => $"ct.{SourceProvider.QuoteIdentifier(_sourceType, pk)}"));
        var lastSyncPlaceholder = BuildParameterPlaceholder("lastSyncVersion");
        var syncToPlaceholder = BuildParameterPlaceholder("syncToVersion");
        var sql = $@"
SELECT {selectList}
FROM CHANGETABLE(CHANGES {sourceTable}, {lastSyncPlaceholder}) AS ct
WHERE ct.SYS_CHANGE_OPERATION = 'D'
  AND ct.SYS_CHANGE_VERSION <= {syncToPlaceholder};
";

        await using var conn = SourceProvider.CreateConnection(_sourceType, _connString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;
        AddParameter(cmd, "lastSyncVersion", lastSyncVersionExclusive);
        AddParameter(cmd, "syncToVersion", syncToVersionInclusive);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("{LogPrefix}ReadChangeTrackingDeletedKeys execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL ReadChangeTrackingDeletedKeys: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL ReadChangeTrackingDeletedKeys Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        sw.Stop();
        _log.LogDebug("{LogPrefix}ReadChangeTrackingDeletedKeys execution time: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);

        while (await rdr.ReadAsync(ct))
        {
            var values = new object?[primaryKey.Count];
            for (var i = 0; i < primaryKey.Count; i++)
                values[i] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);

            yield return values;
        }
    }

    private string BuildParameterPlaceholder(string parameterName)
    {
        if (SourceProvider.UsesPositionalParameters(_sourceType))
            return "?";

        return "@" + parameterName;
    }

    private void AddParameter(DbCommand cmd, string name, object? value)
    {
        var parameterValue = value ?? DBNull.Value;
        if (SourceProvider.UsesPositionalParameters(_sourceType))
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            p.Value = parameterValue;
            cmd.Parameters.Add(p);
            return;
        }

        if (cmd is SqlCommand sqlCmd)
        {
            sqlCmd.Parameters.AddWithValue("@" + name, parameterValue);
            return;
        }

        var fallback = cmd.CreateParameter();
        fallback.ParameterName = "@" + name;
        fallback.Value = parameterValue;
        cmd.Parameters.Add(fallback);
    }

    private void EnsureChangeTrackingSupported()
    {
        if (SourceProvider.SupportsChangeTracking(_sourceType))
            return;

        throw new Exception($"Source type '{_sourceType}' does not support change_tracking. Use a sqlServer source connection.");
    }

    private static string Prefix(string? logPrefix)
    {
        return string.IsNullOrWhiteSpace(logPrefix) ? "[app] " : $"{logPrefix} ";
    }
}
