using FabricIncrementalReplicator.Auth;
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

            var testConnectionsFlag = args.Any(a => a.Equals("--test-connections", StringComparison.OrdinalIgnoreCase));

            var sourceSchemaReader = new SourceSchemaReader(envConfig.SourceSql.ConnectionString, envConfig.SchemaDiscovery);
            var sourceChunkReader = new SourceChunkReader(envConfig.SourceSql.ConnectionString);

            var uploader = new OneLakeUploader(envConfig.OneLakeStaging, tokenProvider);
            var csvWriter = new CsvGzipWriter();

            var warehouseConnFactory = new WarehouseConnectionFactory(envConfig.FabricWarehouse, tokenProvider);
            var schemaManager = new WarehouseSchemaManager(warehouseConnFactory);
            var loaderTarget = new WarehouseLoader(warehouseConnFactory);

            if (testConnectionsFlag)
            {
                Console.WriteLine("Running connection tests...");

                // 1) Test SQL connection (open/close)
                try
                {
                    using var conn = await warehouseConnFactory.OpenAsync();
                    Console.WriteLine("SQL Warehouse: OK");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"SQL Warehouse: FAILED - {ex.Message}");
                    return 2;
                }

                // 2) Test OneLake access
                try
                {
                    await uploader.TestConnectionAsync();
                    Console.WriteLine("OneLake staging: OK");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"OneLake staging: FAILED - {ex.Message}");
                    return 3;
                }

                Console.WriteLine("All connection checks passed.");
                return 0;
            }

            foreach (var stream in streams.Streams)
            {
                Console.WriteLine($"=== Stream: {stream.Name} ===");

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
                Console.WriteLine($"Target watermark (max {stream.UpdateKey}) = {targetMax ?? "NULL"}");

                // 4) Optional logging: source min/max after watermark
                var (srcMin, srcMax) = await sourceChunkReader.GetMinMaxUpdateKeyAsync(stream.SourceSql, stream.UpdateKey, targetMax);
                Console.WriteLine($"Source range after watermark: min={srcMin ?? "NULL"}, max={srcMax ?? "NULL"}");

                // 5) Chunk loop (TOP N ordered)
                var chunkIndex = 0;
                object? watermark = targetMax;

                while (true)
                {
                    var chunkRows = await sourceChunkReader.ReadNextChunkAsync(
                        stream.SourceSql,
                        sourceColumns,
                        stream.UpdateKey,
                        stream.PrimaryKey,
                        watermark,
                        stream.ChunkSize
                    );

                    if (chunkRows.Count == 0)
                    {
                        Console.WriteLine("No more rows.");
                        break;
                    }

                    chunkIndex++;
                    Console.WriteLine($"Chunk {chunkIndex}: rows={chunkRows.Count}");

                    // Determine new watermark = max(updateKey) in this chunk
                    watermark = SourceChunkReader.GetMaxValue(chunkRows, stream.UpdateKey);

                    // Local temp file
                    var runId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
                    var fileName = $"{stream.Name}/run={runId}/chunk={chunkIndex:D6}.csv.gz";

                    var localPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), "chunk.csv.gz");
                    Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);

                    await csvWriter.WriteCsvGzAsync(localPath, sourceColumns, chunkRows);

                    // Upload to OneLake Files/...
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
                        oneLakeDfsUrl: oneLakePath,
                        cleanup: envConfig.Cleanup
                    );

                    // Cleanup files
                    if (envConfig.Cleanup.DeleteLocalTempFiles && File.Exists(localPath))
                        File.Delete(localPath);

                    if (envConfig.Cleanup.DeleteStagedFiles)
                        await uploader.TryDeleteAsync(fileName);

                    Console.WriteLine($"Chunk {chunkIndex} done. New watermark={watermark}");
                }

                Console.WriteLine($"=== Stream {stream.Name} complete ===");
            }

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
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