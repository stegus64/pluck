using FabricIncrementalReplicator.Auth;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using FabricIncrementalReplicator.Config;
using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Staging;
using FabricIncrementalReplicator.Target;
using FabricIncrementalReplicator.Util;

namespace FabricIncrementalReplicator;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            var env = GetArg(args, "--env") ?? "dev";
            var connectionsPath = GetArg(args, "--connections") ?? "connections.yaml";
            var streamsPath = GetArg(args, "--streams") ?? "streams.yaml";

            var loader = new YamlLoader();
            var connectionsRoot = loader.Load<ConnectionsRoot>(connectionsPath);
            var streams = loader.Load<StreamsConfig>(streamsPath);

            if (!connectionsRoot.Environments.TryGetValue(env, out var envConfig))
                throw new Exception($"Environment '{env}' not found in {connectionsPath}.");

            var tokenProvider = new TokenProvider(envConfig.Auth);

            // Configure logging
            var debugFlag = args.Any(a => a.Equals("--debug", StringComparison.OrdinalIgnoreCase));
            var logLevelArg = debugFlag
                ? "DEBUG"
                : (GetArg(args, "--log-level") ?? "INFO");
            var minLogLevel = logLevelArg.ToUpperInvariant() switch
            {
                "ERROR" => Microsoft.Extensions.Logging.LogLevel.Error,
                "DEBUG" => Microsoft.Extensions.Logging.LogLevel.Debug,
                _ => Microsoft.Extensions.Logging.LogLevel.Information,
            };

            using var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole(options => { options.TimestampFormat = "HH:mm:ss "; options.SingleLine = true; });
                builder.SetMinimumLevel(minLogLevel);
            });

            var logger = loggerFactory.CreateLogger("Program");

            var testConnectionsFlag = args.Any(a => a.Equals("--test-connections", StringComparison.OrdinalIgnoreCase));

            var sourceSchemaReader = new SourceSchemaReader(
                envConfig.SourceSql.ConnectionString,
                envConfig.SchemaDiscovery,
                envConfig.SourceSql.CommandTimeoutSeconds,
                loggerFactory.CreateLogger<SourceSchemaReader>());
            var sourceChunkReader = new SourceChunkReader(
                envConfig.SourceSql.ConnectionString,
                envConfig.SourceSql.CommandTimeoutSeconds,
                loggerFactory.CreateLogger<SourceChunkReader>());

            var uploader = new OneLakeUploader(envConfig.OneLakeStaging, tokenProvider, loggerFactory.CreateLogger<OneLakeUploader>());
            var csvWriter = new CsvGzipWriter();
            var parquetWriter = new ParquetChunkWriter();

            var warehouseConnFactory = new WarehouseConnectionFactory(envConfig.FabricWarehouse, tokenProvider, loggerFactory.CreateLogger<WarehouseConnectionFactory>());
            var schemaManager = new WarehouseSchemaManager(warehouseConnFactory, loggerFactory.CreateLogger<WarehouseSchemaManager>());
            var loaderTarget = new WarehouseLoader(warehouseConnFactory, loggerFactory.CreateLogger<WarehouseLoader>());

            if (testConnectionsFlag)
            {
                logger.LogInformation("Running connection tests...");

                // 1) Test source SQL connection (open/close)
                try
                {
                    await using var conn = new SqlConnection(envConfig.SourceSql.ConnectionString);
                    await conn.OpenAsync();
                    logger.LogInformation("Source SQL: OK");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Source SQL: FAILED");
                    return 2;
                }

                // 2) Test warehouse SQL connection (open/close)
                try
                {
                    using var conn = await warehouseConnFactory.OpenAsync();
                    logger.LogInformation("SQL Warehouse: OK");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "SQL Warehouse: FAILED");
                    return 3;
                }

                // 3) Test staging lake access
                try
                {
                    await uploader.TestConnectionAsync();
                    logger.LogInformation("Staging lake: OK");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Staging lake: FAILED");
                    return 4;
                }

                logger.LogInformation("All connection checks passed.");
                return 0;
            }

            foreach (var stream in streams.GetResolvedStreams())
            {
                logger.LogInformation("=== Stream: {StreamName} ===", stream.Name);
                var stagingFileFormat = NormalizeStagingFileFormat(stream.StagingFileFormat);
                logger.LogInformation("Stream staging file format: {StagingFileFormat}", stagingFileFormat);

                var targetSchema = stream.TargetSchema ?? envConfig.FabricWarehouse.TargetSchema ?? "dbo";
                var targetTable = stream.TargetTable;

                // 1) Discover schema of the source query
                var sourceColumns = await sourceSchemaReader.DescribeQueryAsync(stream.SourceSql);
                var excludedColumns = new HashSet<string>(stream.ExcludeColumns ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                if (excludedColumns.Count > 0)
                    logger.LogInformation("Excluded columns for stream {StreamName}: {ExcludedColumns}", stream.Name, string.Join(", ", excludedColumns));
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
                    primaryKey: stream.PrimaryKey
                );

                // 3) Read watermark from target
                object? targetMax = await loaderTarget.GetMaxUpdateKeyAsync(targetSchema, targetTable, stream.UpdateKey);
                logger.LogInformation("Target watermark (max {UpdateKey}) = {TargetMax}", stream.UpdateKey, targetMax ?? "NULL");

                // 4) Optional logging: source min/max after watermark
                var (srcMin, srcMax) = await sourceChunkReader.GetMinMaxUpdateKeyAsync(stream.SourceSql, stream.UpdateKey, targetMax);
                logger.LogInformation("Source range after watermark: min={SourceMin}, max={SourceMax}", srcMin ?? "NULL", srcMax ?? "NULL");

                // 5) Chunk loop by update-key interval
                var chunkIndex = 0;
                if (srcMin is null || srcMax is null)
                {
                    logger.LogInformation("No more rows.");
                    logger.LogInformation("=== Stream {StreamName} complete ===", stream.Name);
                    continue;
                }

                if (stream.ChunkSize <= 0)
                    throw new Exception($"Invalid chunkSize for stream '{stream.Name}'. Value must be > 0.");

                var lowerBound = srcMin;
                while (CompareUpdateKey(lowerBound, srcMax) <= 0)
                {
                    var upperBound = AddUpdateKeyInterval(lowerBound, stream.ChunkSize);

                    // Local temp file
                    var runId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
                    var ext = stagingFileFormat == "parquet" ? "parquet" : (stagingFileFormat == "csv" ? "csv" : "csv.gz");
                    var fileName = $"{stream.Name}/run={runId}/chunk={(chunkIndex + 1):D6}.{ext}";

                    var localPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), $"chunk.{ext}");
                    Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);
                    logger.LogDebug("Next chunk local staging path: {LocalPath}", localPath);

                    var rowStream = sourceChunkReader.ReadChunkByIntervalAsync(
                        stream.SourceSql,
                        sourceColumns,
                        stream.UpdateKey,
                        lowerBound,
                        upperBound
                    );
                    var writeSw = System.Diagnostics.Stopwatch.StartNew();
                    var chunkWrite = stagingFileFormat == "parquet"
                        ? await parquetWriter.WriteParquetAsync(localPath, sourceColumns, rowStream)
                        : stagingFileFormat == "csv"
                            ? await csvWriter.WriteCsvAsync(localPath, sourceColumns, rowStream)
                            : await csvWriter.WriteCsvGzAsync(localPath, sourceColumns, rowStream);
                    writeSw.Stop();

                    if (chunkWrite.RowCount == 0)
                    {
                        logger.LogDebug(
                            "No rows in interval chunk [{LowerBound}, {UpperBound}) for stream {StreamName}.",
                            lowerBound,
                            upperBound,
                            stream.Name);
                        if (envConfig.Cleanup.DeleteLocalTempFiles && File.Exists(localPath))
                            File.Delete(localPath);
                        lowerBound = upperBound;
                        continue;
                    }

                    chunkIndex++;

                    // Upload to staging lake path...
                    var uploadSw = System.Diagnostics.Stopwatch.StartNew();
                    var oneLakePath = await uploader.UploadAsync(localPath, fileName);
                    uploadSw.Stop();
                    var uploadedFileSizeKb = new FileInfo(localPath).Length / 1024d;

                    // Temp table name
                    var tempTable = $"__tmp_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";

                    // Load into temp table & merge
                    var warehouseMetrics = await loaderTarget.LoadAndMergeAsync(
                        targetSchema: targetSchema,
                        targetTable: targetTable,
                        tempTable: tempTable,
                        columns: sourceColumns,
                        primaryKey: stream.PrimaryKey,
                        expectedRowCount: chunkWrite.RowCount,
                        oneLakeDfsUrl: oneLakePath,
                        stagingFileFormat: stagingFileFormat,
                        cleanup: envConfig.Cleanup
                    );

                    var totalSeconds =
                        writeSw.Elapsed.TotalSeconds +
                        uploadSw.Elapsed.TotalSeconds +
                        warehouseMetrics.CopyIntoElapsed.TotalSeconds +
                        warehouseMetrics.MergeElapsed.TotalSeconds;
                    var rowsPerSecond = totalSeconds > 0
                        ? chunkWrite.RowCount / totalSeconds
                        : 0d;

                    logger.LogInformation(
                        "Chunk {ChunkIndex}: rows={RowCount}, lowerBound={LowerBound}, upperBoundExclusive={UpperBound}, fileSizeKb={FileSizeKb:F1}, writeMs={WriteMs:F0}, uploadMs={UploadMs:F0}, copyIntoMs={CopyIntoMs:F0}, mergeMs={MergeMs:F0}, avgRowsPerSec={RowsPerSec:F1}",
                        chunkIndex,
                        chunkWrite.RowCount,
                        lowerBound,
                        upperBound,
                        uploadedFileSizeKb,
                        writeSw.Elapsed.TotalMilliseconds,
                        uploadSw.Elapsed.TotalMilliseconds,
                        warehouseMetrics.CopyIntoElapsed.TotalMilliseconds,
                        warehouseMetrics.MergeElapsed.TotalMilliseconds,
                        rowsPerSecond);

                    // Cleanup files
                    if (envConfig.Cleanup.DeleteLocalTempFiles && File.Exists(localPath))
                        File.Delete(localPath);

                    if (envConfig.Cleanup.DeleteStagedFiles)
                        await uploader.TryDeleteAsync(fileName);

                    lowerBound = upperBound;
                }

                logger.LogInformation("=== Stream {StreamName} complete ===", stream.Name);
            }

            return 0;
        }
        catch (Exception ex)
        {
            using var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole(options => { options.TimestampFormat = "HH:mm:ss "; options.SingleLine = false; });
                builder.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Critical);
            });
            var logger = loggerFactory.CreateLogger("Program");
            logger.LogCritical(ex, "Unhandled exception during replication run.");
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

    private static object AddUpdateKeyInterval(object value, int interval)
    {
        return value switch
        {
            byte v => checked((byte)(v + interval)),
            short v => checked((short)(v + interval)),
            int v => checked(v + interval),
            long v => checked(v + interval),
            sbyte v => checked((sbyte)(v + interval)),
            ushort v => checked((ushort)(v + interval)),
            uint v => checked(v + (uint)interval),
            ulong v => checked(v + (ulong)interval),
            float v => v + interval,
            double v => v + interval,
            decimal v => v + interval,
            _ => throw new Exception($"Unsupported updateKey type '{value.GetType().Name}' for interval chunking. Use a numeric updateKey.")
        };
    }

    private static bool IsNumericType(object value)
    {
        return value is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;
    }
}
