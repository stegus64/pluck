using FabricIncrementalReplicator.Auth;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using FabricIncrementalReplicator.Config;
using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Staging;
using FabricIncrementalReplicator.Target;
using FabricIncrementalReplicator.Util;
using System.Globalization;
using System.Text.RegularExpressions;

namespace FabricIncrementalReplicator;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        const string appPrefix = "[app]";
        // Configure logging
        var traceFlag = args.Any(a => a.Equals("--trace", StringComparison.OrdinalIgnoreCase));
        var debugFlag = args.Any(a => a.Equals("--debug", StringComparison.OrdinalIgnoreCase));
        var logLevelArg = traceFlag
            ? "TRACE"
            : debugFlag
            ? "DEBUG"
            : (GetArg(args, "--log-level") ?? "INFO");
        var minLogLevel = logLevelArg.ToUpperInvariant() switch
        {
            "TRACE" => Microsoft.Extensions.Logging.LogLevel.Trace,
            "ERROR" => Microsoft.Extensions.Logging.LogLevel.Error,
            "DEBUG" => Microsoft.Extensions.Logging.LogLevel.Debug,
            _ => Microsoft.Extensions.Logging.LogLevel.Information,
        };

        using var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole(options => { options.TimestampFormat = "HH:mm:ss "; options.SingleLine = true; });
            builder.SetMinimumLevel(minLogLevel);
        });

        var logger = loggerFactory.CreateLogger("App");

        try
        {
            var env = GetArg(args, "--env") ?? "dev";
            var connectionsPath = GetArg(args, "--connections-file") ?? "connections.yaml";
            var streamsPathArg = GetArg(args, "--streams-file");
            var streamsFilterArg = GetArg(args, "--streams");
            var streamsPath = streamsPathArg ?? "streams.yaml";

            var loader = new YamlLoader();
            var connectionsRoot = loader.Load<ConnectionsRoot>(connectionsPath);
            var streams = loader.Load<StreamsConfig>(streamsPath);

            if (!connectionsRoot.Environments.TryGetValue(env, out var envConfig))
                throw new Exception($"Environment '{env}' not found in {connectionsPath}.");

            var tokenProvider = new TokenProvider(envConfig.Auth);


            var testConnectionsFlag = args.Any(a => a.Equals("--test-connections", StringComparison.OrdinalIgnoreCase));

            var sourceSchemaReader = new SourceSchemaReader(
                envConfig.SourceSql.ConnectionString,
                envConfig.SchemaDiscovery,
                envConfig.SourceSql.CommandTimeoutSeconds,
                logger);
            var sourceChunkReader = new SourceChunkReader(
                envConfig.SourceSql.ConnectionString,
                envConfig.SourceSql.CommandTimeoutSeconds,
                logger);

            var uploader = new OneLakeUploader(envConfig.OneLakeStaging, tokenProvider, logger);
            var csvWriter = new CsvGzipWriter();
            var parquetWriter = new ParquetChunkWriter();

            var warehouseConnFactory = new WarehouseConnectionFactory(envConfig.FabricWarehouse, tokenProvider, logger);
            var schemaManager = new WarehouseSchemaManager(warehouseConnFactory, logger);
            var loaderTarget = new WarehouseLoader(warehouseConnFactory, logger);

            if (testConnectionsFlag)
            {
                logger.LogInformation("{LogPrefix} Running connection tests...", appPrefix);

                // 1) Test source SQL connection (open/close)
                try
                {
                    await using var conn = new SqlConnection(envConfig.SourceSql.ConnectionString);
                    await conn.OpenAsync();
                    logger.LogInformation("{LogPrefix} Source SQL: OK", appPrefix);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "{LogPrefix} Source SQL: FAILED", appPrefix);
                    return 2;
                }

                // 2) Test warehouse SQL connection (open/close)
                try
                {
                    using var conn = await warehouseConnFactory.OpenAsync(logPrefix: appPrefix);
                    logger.LogInformation("{LogPrefix} SQL Warehouse: OK", appPrefix);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "{LogPrefix} SQL Warehouse: FAILED", appPrefix);
                    return 3;
                }

                // 3) Test staging lake access
                try
                {
                    await uploader.TestConnectionAsync();
                    logger.LogInformation("{LogPrefix} Staging lake: OK", appPrefix);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "{LogPrefix} Staging lake: FAILED", appPrefix);
                    return 4;
                }

                logger.LogInformation("{LogPrefix} All connection checks passed.", appPrefix);
                return 0;
            }

            var resolvedStreams = streams.GetResolvedStreams();
            if (!string.IsNullOrWhiteSpace(streamsFilterArg))
            {
                var requestedStreams = streamsFilterArg
                    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .Where(s => !string.IsNullOrWhiteSpace(s))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                if (requestedStreams.Count == 0)
                    throw new Exception("Invalid --streams value. Provide one or more comma-separated stream names.");

                var requestedSet = new HashSet<string>(requestedStreams, StringComparer.OrdinalIgnoreCase);
                var availableSet = new HashSet<string>(resolvedStreams.Select(s => s.Name), StringComparer.OrdinalIgnoreCase);
                var missing = requestedStreams.Where(name => !availableSet.Contains(name)).ToList();
                if (missing.Count > 0)
                    throw new Exception($"Unknown stream(s) in --streams: {string.Join(", ", missing)}.");

                resolvedStreams = resolvedStreams
                    .Where(s => requestedSet.Contains(s.Name))
                    .ToList();

                logger.LogInformation(
                    "{LogPrefix} Stream filter enabled (--streams): {Streams}",
                    appPrefix,
                    string.Join(", ", requestedStreams));
            }

            var maxParallelStreams = streams.GetMaxParallelStreams();
            logger.LogInformation(
                "{LogPrefix} Stream execution mode: maxParallelStreams={MaxParallelStreams}, streamCount={StreamCount}",
                appPrefix,
                maxParallelStreams,
                resolvedStreams.Count);

            if (maxParallelStreams <= 1)
            {
                foreach (var stream in resolvedStreams)
                    await ProcessStreamAsync(stream);
            }
            else
            {
                await Parallel.ForEachAsync(
                    resolvedStreams,
                    new ParallelOptions { MaxDegreeOfParallelism = maxParallelStreams },
                    async (stream, _) => await ProcessStreamAsync(stream));
            }

            return 0;

            async Task ProcessStreamAsync(ResolvedStreamConfig stream)
            {
                var streamPrefix = $"[stream={stream.Name}]";
                logger.LogInformation("{LogPrefix} === Stream: {StreamName} ===", streamPrefix, stream.Name);
                var stagingFileFormat = NormalizeStagingFileFormat(stream.StagingFileFormat);
                logger.LogInformation("{LogPrefix} Stream staging file format: {StagingFileFormat}", streamPrefix, stagingFileFormat);

                var targetSchema = stream.TargetSchema ?? envConfig.FabricWarehouse.TargetSchema ?? "dbo";
                var targetTable = stream.TargetTable;

                // 1) Discover schema of the source query
                var sourceColumns = await sourceSchemaReader.DescribeQueryAsync(stream.SourceSql, streamPrefix);
                if (sourceColumns.Any(c => c.Name.Equals("_sg_update_datetime", StringComparison.OrdinalIgnoreCase) ||
                                           c.Name.Equals("_sg_update_op", StringComparison.OrdinalIgnoreCase)))
                {
                    throw new Exception(
                        $"Stream '{stream.Name}' source query includes reserved metadata column names " +
                        "('_sg_update_datetime' or '_sg_update_op').");
                }
                var excludedColumns = new HashSet<string>(stream.ExcludeColumns ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                if (excludedColumns.Count > 0)
                    logger.LogInformation("{LogPrefix} Excluded columns for stream {StreamName}: {ExcludedColumns}", streamPrefix, stream.Name, string.Join(", ", excludedColumns));
                if (excludedColumns.Contains(stream.UpdateKey))
                    throw new Exception($"Stream '{stream.Name}' excludes updateKey column '{stream.UpdateKey}', which is not allowed.");

                var excludedPrimaryKey = stream.PrimaryKey.FirstOrDefault(pk => excludedColumns.Contains(pk));
                if (!string.IsNullOrWhiteSpace(excludedPrimaryKey))
                    throw new Exception($"Stream '{stream.Name}' excludes primary key column '{excludedPrimaryKey}', which is not allowed.");

                sourceColumns = sourceColumns
                    .Where(c => !excludedColumns.Contains(c.Name))
                    .ToList();

                // Validate required columns exist
                SchemaValidator.EnsureContainsColumns(sourceColumns, stream.PrimaryKey, stream.UpdateKey);

                // 2) Ensure target table exists and schema is aligned (add new columns)
                await schemaManager.EnsureTableAndSchemaAsync(
                    targetSchema,
                    targetTable,
                    sourceColumns,
                    primaryKey: stream.PrimaryKey,
                    logPrefix: streamPrefix
                );

                // 3) Read watermark from target
                object? targetMax = await loaderTarget.GetMaxUpdateKeyAsync(targetSchema, targetTable, stream.UpdateKey, streamPrefix);
                logger.LogInformation("{LogPrefix} Target watermark (max {UpdateKey}) = {TargetMax}", streamPrefix, stream.UpdateKey, FormatLogValue(targetMax));

                // 4) Optional logging: source min/max after watermark
                var (srcMin, srcMax) = await sourceChunkReader.GetMinMaxUpdateKeyAsync(stream.SourceSql, stream.UpdateKey, targetMax, streamPrefix);
                logger.LogInformation(
                    "{LogPrefix} Source range after watermark: min={SourceMin}, max={SourceMax}",
                    streamPrefix,
                    FormatLogValue(srcMin),
                    FormatLogValue(srcMax));

                // 5) Chunk loop by update-key interval
                var chunkIndex = 0;
                if (srcMin is not null && srcMax is not null)
                {
                    var chunkInterval = ParseChunkInterval(stream.ChunkSize, stream.Name);
                    var ext = stagingFileFormat == "parquet" ? "parquet" : (stagingFileFormat == "csv" ? "csv" : "csv.gz");
                    Task<PreparedChunk?>? prepareNextTask = PrepareNextChunkAsync(srcMin, chunkIndex + 1);
                    string ChunkLogPrefix(int idx) => $"[stream={stream.Name} chunk={idx:D2}]";

                    while (prepareNextTask is not null)
                    {
                        var prepared = await prepareNextTask;
                        if (prepared is null)
                            break;

                        prepareNextTask = prepared.NextLowerBound is not null
                            ? PrepareNextChunkAsync(prepared.NextLowerBound, prepared.ChunkIndex + 1)
                            : null;

                        await ProcessPreparedChunkAsync(prepared);
                        chunkIndex = prepared.ChunkIndex;
                    }

                    async Task<PreparedChunk?> PrepareNextChunkAsync(object initialLowerBound, int nextChunkIndex)
                    {
                        var lowerBound = initialLowerBound;
                        while (CompareUpdateKey(lowerBound, srcMax) <= 0)
                        {
                            var upperBound = chunkInterval is null ? null : AddUpdateKeyInterval(lowerBound, chunkInterval);

                            var runId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
                            var fileName = $"{stream.Name}/run={runId}/chunk={nextChunkIndex:D6}.{ext}";
                            var localPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), $"chunk.{ext}");
                            Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);
                            var chunkPrefix = ChunkLogPrefix(nextChunkIndex);
                            logger.LogDebug("{LogPrefix} Next chunk local staging path: {LocalPath}", chunkPrefix, localPath);

                            var rowStream = chunkInterval is null
                                ? sourceChunkReader.ReadChunkFromLowerBoundAsync(
                                    stream.SourceSql,
                                    sourceColumns,
                                    stream.UpdateKey,
                                    lowerBound,
                                    srcMax,
                                    chunkPrefix)
                                : sourceChunkReader.ReadChunkByIntervalAsync(
                                    stream.SourceSql,
                                    sourceColumns,
                                    stream.UpdateKey,
                                    lowerBound,
                                    upperBound!,
                                    chunkPrefix);
                            var writeSw = System.Diagnostics.Stopwatch.StartNew();
                            var chunkWrite = stagingFileFormat == "parquet"
                                ? await parquetWriter.WriteParquetAsync(localPath, sourceColumns, rowStream)
                                : stagingFileFormat == "csv"
                                    ? await csvWriter.WriteCsvAsync(localPath, sourceColumns, rowStream)
                                    : await csvWriter.WriteCsvGzAsync(localPath, sourceColumns, rowStream);
                            writeSw.Stop();
                            logger.LogDebug("{LogPrefix} Local file written. Elapsed: {Elapsed}ms", chunkPrefix, writeSw.Elapsed.TotalMilliseconds);

                            if (chunkWrite.RowCount == 0)
                            {
                                if (chunkInterval is null)
                                {
                                    logger.LogDebug(
                                        "{LogPrefix} No rows in single-chunk range [{LowerBound}, {MaxInclusive}].",
                                        chunkPrefix,
                                        lowerBound,
                                        srcMax);
                                }
                                else
                                {
                                    logger.LogDebug(
                                        "{LogPrefix} No rows in interval chunk [{LowerBound}, {UpperBound}).",
                                        chunkPrefix,
                                        lowerBound,
                                        upperBound);
                                }

                                if (envConfig.Cleanup.DeleteLocalTempFiles)
                                    TryDeleteLocalTempFileAndDirectory(localPath, logger, chunkPrefix);
                                if (chunkInterval is null)
                                    return null;

                                lowerBound = upperBound!;
                                continue;
                            }

                            var nextLowerBound = chunkInterval is null ? null : upperBound!;
                            return new PreparedChunk(
                                ChunkIndex: nextChunkIndex,
                                LowerBound: lowerBound,
                                UpperBound: upperBound,
                                NextLowerBound: nextLowerBound,
                                LocalPath: localPath,
                                FileName: fileName,
                                RowCount: chunkWrite.RowCount,
                                WriteElapsed: writeSw.Elapsed);
                        }

                        return null;
                    }

                    async Task ProcessPreparedChunkAsync(PreparedChunk prepared)
                    {
                        var chunkPrefix = ChunkLogPrefix(prepared.ChunkIndex);
                        var uploadSw = System.Diagnostics.Stopwatch.StartNew();
                        var oneLakePath = await uploader.UploadAsync(prepared.LocalPath, prepared.FileName, chunkPrefix);
                        uploadSw.Stop();
                        var uploadedFileSizeKb = new FileInfo(prepared.LocalPath).Length / 1024d;

                        var tempTable = $"__tmp_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
                        var warehouseMetrics = await loaderTarget.LoadAndMergeAsync(
                            targetSchema: targetSchema,
                            targetTable: targetTable,
                            tempTable: tempTable,
                            columns: sourceColumns,
                            primaryKey: stream.PrimaryKey,
                            expectedRowCount: prepared.RowCount,
                            oneLakeDfsUrl: oneLakePath,
                            stagingFileFormat: stagingFileFormat,
                            cleanup: envConfig.Cleanup,
                            logPrefix: chunkPrefix
                        );

                        var totalSeconds =
                            prepared.WriteElapsed.TotalSeconds +
                            uploadSw.Elapsed.TotalSeconds +
                            warehouseMetrics.CopyIntoElapsed.TotalSeconds +
                            warehouseMetrics.MergeElapsed.TotalSeconds;
                        var rowsPerSecond = totalSeconds > 0
                            ? prepared.RowCount / totalSeconds
                            : 0d;

                        if (chunkInterval is null)
                        {
                            logger.LogInformation(
                                "{LogPrefix} Chunk {ChunkIndex}: rows={RowCount}, lowerBound={LowerBound}, maxInclusive={MaxInclusive}, fileSizeKb={FileSizeKb:F1}, writeMs={WriteMs:F0}, uploadMs={UploadMs:F0}, copyIntoMs={CopyIntoMs:F0}, mergeMs={MergeMs:F0}, avgRowsPerSec={RowsPerSec:F1}",
                                chunkPrefix,
                                prepared.ChunkIndex,
                                prepared.RowCount,
                                prepared.LowerBound,
                                srcMax,
                                uploadedFileSizeKb,
                                prepared.WriteElapsed.TotalMilliseconds,
                                uploadSw.Elapsed.TotalMilliseconds,
                                warehouseMetrics.CopyIntoElapsed.TotalMilliseconds,
                                warehouseMetrics.MergeElapsed.TotalMilliseconds,
                                rowsPerSecond);
                        }
                        else
                        {
                            logger.LogInformation(
                                "{LogPrefix} Chunk {ChunkIndex}: rows={RowCount}, lowerBound={LowerBound}, upperBoundExclusive={UpperBound}, fileSizeKb={FileSizeKb:F1}, writeMs={WriteMs:F0}, uploadMs={UploadMs:F0}, copyIntoMs={CopyIntoMs:F0}, mergeMs={MergeMs:F0}, avgRowsPerSec={RowsPerSec:F1}",
                                chunkPrefix,
                                prepared.ChunkIndex,
                                prepared.RowCount,
                                prepared.LowerBound,
                                prepared.UpperBound,
                                uploadedFileSizeKb,
                                prepared.WriteElapsed.TotalMilliseconds,
                                uploadSw.Elapsed.TotalMilliseconds,
                                warehouseMetrics.CopyIntoElapsed.TotalMilliseconds,
                                warehouseMetrics.MergeElapsed.TotalMilliseconds,
                                rowsPerSecond);
                        }

                        if (envConfig.Cleanup.DeleteLocalTempFiles)
                            TryDeleteLocalTempFileAndDirectory(prepared.LocalPath, logger, chunkPrefix);

                        if (envConfig.Cleanup.DeleteStagedFiles)
                            await uploader.TryDeleteAsync(prepared.FileName, chunkPrefix);
                    }
                }
                else
                {
                    logger.LogInformation("{LogPrefix} No more rows.", streamPrefix);
                }

                if (string.Equals(stream.DeleteDetection.Type, "subset", StringComparison.OrdinalIgnoreCase))
                {
                    var pkColumns = stream.PrimaryKey
                        .Select(pk => sourceColumns.Single(c => c.Name.Equals(pk, StringComparison.OrdinalIgnoreCase)))
                        .ToList();
                    var keyStream = sourceChunkReader.ReadColumnsAsync(stream.SourceSql, stream.PrimaryKey, stream.DeleteDetection.Where, streamPrefix);

                    var deleteRunId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
                    var keysFileName = $"{stream.Name}/run={deleteRunId}/delete-keys.csv.gz";
                    var keysLocalPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), "delete-keys.csv.gz");
                    Directory.CreateDirectory(Path.GetDirectoryName(keysLocalPath)!);

                    var writeKeysSw = System.Diagnostics.Stopwatch.StartNew();
                    var keyWrite = await csvWriter.WriteCsvGzAsync(keysLocalPath, pkColumns, keyStream);
                    writeKeysSw.Stop();

                    var uploadKeysSw = System.Diagnostics.Stopwatch.StartNew();
                    var keysDfsUrl = await uploader.UploadAsync(keysLocalPath, keysFileName, streamPrefix);
                    uploadKeysSw.Stop();

                    var keysTempTable = $"__tmp_delkeys_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
                    var deleteMetrics = await loaderTarget.SoftDeleteMissingRowsAsync(
                        targetSchema: targetSchema,
                        targetTable: targetTable,
                        sourceKeysTempTable: keysTempTable,
                        primaryKeyColumns: pkColumns,
                        sourceKeysDfsUrl: keysDfsUrl,
                        sourceKeysFileFormat: "csv.gz",
                        subsetWhere: stream.DeleteDetection.Where,
                        cleanup: envConfig.Cleanup,
                        logPrefix: streamPrefix);

                    logger.LogInformation(
                        "{LogPrefix} Delete detection ({Type}) stream {StreamName}: sourceKeys={SourceKeys}, subsetWhere={SubsetWhere}, keyWriteMs={KeyWriteMs:F0}, keyUploadMs={KeyUploadMs:F0}, keyCopyMs={KeyCopyMs:F0}, softDeleteMs={SoftDeleteMs:F0}, softDeletedRows={SoftDeletedRows}",
                        streamPrefix,
                        stream.DeleteDetection.Type,
                        stream.Name,
                        keyWrite.RowCount,
                        string.IsNullOrWhiteSpace(stream.DeleteDetection.Where) ? "<none>" : stream.DeleteDetection.Where,
                        writeKeysSw.Elapsed.TotalMilliseconds,
                        uploadKeysSw.Elapsed.TotalMilliseconds,
                        deleteMetrics.CopyIntoElapsed.TotalMilliseconds,
                        deleteMetrics.SoftDeleteElapsed.TotalMilliseconds,
                        deleteMetrics.AffectedRows);

                    if (envConfig.Cleanup.DeleteLocalTempFiles)
                        TryDeleteLocalTempFileAndDirectory(keysLocalPath, logger, streamPrefix);
                    if (envConfig.Cleanup.DeleteStagedFiles)
                        await uploader.TryDeleteAsync(keysFileName, streamPrefix);
                }

                logger.LogInformation("{LogPrefix} === Stream {StreamName} complete ===", streamPrefix, stream.Name);
            }
        }
        catch (SqlException ex)
        {
            // SQL errors: concise output without stack trace.
            logger.LogError("{LogPrefix} {ExceptionType}: {Message}", appPrefix, ex.GetType().Name, ex.Message);
            return 1;
        }
        catch (Exception ex)
        {
            // Non-SQL errors: full stack trace.
            logger.LogError(ex, "{LogPrefix} Unhandled exception during replication run.", appPrefix);
            return 1;
        }
    }

    private static string? GetArg(string[] args, string name)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].Equals(name, StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                return args[i + 1];
        }
        return null;
    }

    private static string NormalizeStagingFileFormat(string? value)
    {
        var normalized = (value ?? "csv.gz").Trim().ToLowerInvariant();
        return normalized switch
        {
            "csv" => "csv",
            "csv.gz" => "csv.gz",
            "gz" => "csv.gz",
            "parquet" => "parquet",
            "pq" => "parquet",
            _ => throw new Exception($"Unsupported staging file format '{value}'. Supported values: csv, csv.gz, parquet.")
        };
    }

    private static int CompareUpdateKey(object left, object right)
    {
        if (left.GetType() == right.GetType() && left is IComparable comparable)
            return comparable.CompareTo(right);

        if (IsNumericType(left) && IsNumericType(right))
            return Convert.ToDecimal(left).CompareTo(Convert.ToDecimal(right));

        throw new Exception($"Unsupported updateKey type comparison: '{left.GetType().Name}' vs '{right.GetType().Name}'.");
    }

    private static object AddUpdateKeyInterval(object value, ChunkInterval interval)
    {
        if (interval.Amount <= 0)
            throw new Exception("chunkSize must be > 0.");

        return value switch
        {
            byte v when interval.Unit == ChunkUnit.Number => checked((byte)(v + interval.Amount)),
            short v when interval.Unit == ChunkUnit.Number => checked((short)(v + interval.Amount)),
            int v when interval.Unit == ChunkUnit.Number => checked(v + interval.Amount),
            long v when interval.Unit == ChunkUnit.Number => checked(v + interval.Amount),
            sbyte v when interval.Unit == ChunkUnit.Number => checked((sbyte)(v + interval.Amount)),
            ushort v when interval.Unit == ChunkUnit.Number => checked((ushort)(v + interval.Amount)),
            uint v when interval.Unit == ChunkUnit.Number => checked(v + (uint)interval.Amount),
            ulong v when interval.Unit == ChunkUnit.Number => checked(v + (ulong)interval.Amount),
            float v when interval.Unit == ChunkUnit.Number => v + interval.Amount,
            double v when interval.Unit == ChunkUnit.Number => v + interval.Amount,
            decimal v when interval.Unit == ChunkUnit.Number => v + interval.Amount,
            DateTime v => AddDateTimeInterval(v, interval),
            DateTimeOffset v => AddDateTimeOffsetInterval(v, interval),
            _ => throw new Exception($"Unsupported chunking: updateKey type '{value.GetType().Name}' with chunkSize '{interval.Raw}'.")
        };
    }

    private static bool IsNumericType(object value)
    {
        return value is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;
    }

    private static object FormatLogValue(object? value)
    {
        if (value is null)
            return "NULL";

        return value switch
        {
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.fff"),
            DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss.fff zzz"),
            _ => value
        };
    }

    private static DateTime AddDateTimeInterval(DateTime value, ChunkInterval interval)
    {
        return interval.Unit switch
        {
            ChunkUnit.Day => value.AddDays(interval.Amount),
            ChunkUnit.Month => value.AddMonths(interval.Amount),
            ChunkUnit.Year => value.AddYears(interval.Amount),
            ChunkUnit.Hour => value.AddHours(interval.Amount),
            ChunkUnit.Number => throw new Exception($"chunkSize '{interval.Raw}' must include a unit for date/datetime update keys (supported: d, m, y, h)."),
            _ => throw new Exception($"Unsupported chunkSize unit in '{interval.Raw}'.")
        };
    }

    private static DateTimeOffset AddDateTimeOffsetInterval(DateTimeOffset value, ChunkInterval interval)
    {
        return interval.Unit switch
        {
            ChunkUnit.Day => value.AddDays(interval.Amount),
            ChunkUnit.Month => value.AddMonths(interval.Amount),
            ChunkUnit.Year => value.AddYears(interval.Amount),
            ChunkUnit.Hour => value.AddHours(interval.Amount),
            ChunkUnit.Number => throw new Exception($"chunkSize '{interval.Raw}' must include a unit for date/datetime update keys (supported: d, m, y, h)."),
            _ => throw new Exception($"Unsupported chunkSize unit in '{interval.Raw}'.")
        };
    }

    private static ChunkInterval? ParseChunkInterval(string? rawChunkSize, string streamName)
    {
        var raw = (rawChunkSize ?? string.Empty).Trim().ToLowerInvariant();
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var m = Regex.Match(raw, @"^(?<num>\d+)\s*(?<unit>[a-z]*)$");
        if (!m.Success)
            throw new Exception($"Invalid chunkSize '{rawChunkSize}' for stream '{streamName}'. Examples: 50000, 7d, 2m.");

        if (!int.TryParse(m.Groups["num"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var amount) || amount <= 0)
            throw new Exception($"Invalid chunkSize '{rawChunkSize}' for stream '{streamName}'. Amount must be a positive integer.");

        var unitText = m.Groups["unit"].Value;
        var unit = unitText switch
        {
            "" => ChunkUnit.Number,
            "d" => ChunkUnit.Day,
            "m" => ChunkUnit.Month,
            "y" => ChunkUnit.Year,
            "h" => ChunkUnit.Hour,
            _ => throw new Exception($"Unsupported chunkSize unit '{unitText}' for stream '{streamName}'. Supported units: d, m, y, h.")
        };

        return new ChunkInterval(raw, amount, unit);
    }

    private enum ChunkUnit
    {
        Number,
        Hour,
        Day,
        Month,
        Year
    }

    private sealed record ChunkInterval(string Raw, int Amount, ChunkUnit Unit);
    private sealed record PreparedChunk(
        int ChunkIndex,
        object LowerBound,
        object? UpperBound,
        object? NextLowerBound,
        string LocalPath,
        string FileName,
        int RowCount,
        TimeSpan WriteElapsed);

    private static void TryDeleteLocalTempFileAndDirectory(string localPath, ILogger logger, string logPrefix)
    {
        try
        {
            if (File.Exists(localPath))
                File.Delete(localPath);

            var dir = Path.GetDirectoryName(localPath);
            if (!string.IsNullOrWhiteSpace(dir) && Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "{LogPrefix} Failed to cleanup local temp file/directory: {LocalPath}", logPrefix, localPath);
        }
    }
}
