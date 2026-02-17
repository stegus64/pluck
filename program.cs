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

            var sourceSchemaReader = new SourceSchemaReader(envConfig.SourceSql.ConnectionString, envConfig.SchemaDiscovery, loggerFactory.CreateLogger<SourceSchemaReader>());
            var sourceChunkReader = new SourceChunkReader(envConfig.SourceSql.ConnectionString, loggerFactory.CreateLogger<SourceChunkReader>());

            var uploader = new OneLakeUploader(envConfig.OneLakeStaging, tokenProvider, loggerFactory.CreateLogger<OneLakeUploader>());
            var csvWriter = new CsvGzipWriter();

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

            foreach (var stream in streams.Streams)
            {
                logger.LogInformation("=== Stream: {StreamName} ===", stream.Name);

                var targetSchema = stream.TargetSchema ?? envConfig.FabricWarehouse.TargetSchema ?? "dbo";
                var targetTable = stream.TargetTable;

                // 1) Discover schema of the source query
                var sourceColumns = await sourceSchemaReader.DescribeQueryAsync(stream.SourceSql);

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

                // 5) Chunk loop (TOP N ordered)
                var chunkIndex = 0;
                object? watermark = targetMax;

                while (true)
                {
                    // Local temp file
                    var runId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
                    var fileName = $"{stream.Name}/run={runId}/chunk={(chunkIndex + 1):D6}.csv.gz";

                    var localPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), "chunk.csv.gz");
                    Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);
                    logger.LogDebug("Next chunk local csv.gz path: {LocalPath}", localPath);

                    var updateKeyIndex = sourceColumns.FindIndex(c => c.Name.Equals(stream.UpdateKey, StringComparison.OrdinalIgnoreCase));
                    if (updateKeyIndex < 0)
                        throw new Exception($"Update key '{stream.UpdateKey}' was not found in source column list for stream '{stream.Name}'.");
                    var rowStream = sourceChunkReader.ReadNextChunkStreamAsync(
                        stream.SourceSql,
                        sourceColumns,
                        stream.UpdateKey,
                        stream.PrimaryKey,
                        watermark,
                        stream.ChunkSize
                    );
                    var chunkWrite = await csvWriter.WriteCsvGzAsync(localPath, sourceColumns, rowStream, updateKeyIndex);

                    if (chunkWrite.RowCount == 0)
                    {
                        logger.LogInformation("No more rows.");
                        if (envConfig.Cleanup.DeleteLocalTempFiles && File.Exists(localPath))
                            File.Delete(localPath);
                        break;
                    }

                    chunkIndex++;
                    logger.LogInformation("Chunk {ChunkIndex}: rows={RowCount}", chunkIndex, chunkWrite.RowCount);
                    watermark = chunkWrite.MaxUpdateKey;

                    // Upload to staging lake path...
                    var oneLakePath = await uploader.UploadAsync(localPath, fileName);

                    // Temp table name
                    var tempTable = $"__tmp_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";

                    // Load into temp table & merge
                    await loaderTarget.LoadAndMergeAsync(
                        targetSchema: targetSchema,
                        targetTable: targetTable,
                        tempTable: tempTable,
                        columns: sourceColumns,
                        primaryKey: stream.PrimaryKey,
                        expectedRowCount: chunkWrite.RowCount,
                        oneLakeDfsUrl: oneLakePath,
                        cleanup: envConfig.Cleanup
                    );

                    // Cleanup files
                    if (envConfig.Cleanup.DeleteLocalTempFiles && File.Exists(localPath))
                        File.Delete(localPath);

                    if (envConfig.Cleanup.DeleteStagedFiles)
                        await uploader.TryDeleteAsync(fileName);

                    logger.LogInformation("Chunk {ChunkIndex} done. New watermark={Watermark}", chunkIndex, watermark);
                }

                logger.LogInformation("=== Stream {StreamName} complete ===", stream.Name);
            }

            return 0;
        }
        catch (Exception ex)
        {
            using var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole(options => { options.TimestampFormat = "HH:mm:ss "; options.SingleLine = true; });
                builder.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Error);
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
}
