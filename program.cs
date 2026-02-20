using Pluck.Auth;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using Pluck.Config;
using Pluck.Source;
using Pluck.Staging;
using Pluck.Target;
using Pluck.Util;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;

namespace Pluck;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        return await RunApplicationAsync(args);
    }

    private static async Task<int> RunApplicationAsync(string[] args)
    {
        if (!TryParseArgs(args, out var parsedArgs, out var parseError))
        {
            Console.Error.WriteLine(parseError);
            Console.Error.WriteLine("Use --help to see valid options.");
            return 1;
        }

        if (parsedArgs.Help)
        {
            PrintHelp();
            return 0;
        }

        const string appPrefix = "[app]";
        // Configure logging
        var traceFlag = parsedArgs.Trace;
        var debugFlag = parsedArgs.Debug;
        var logLevelArg = traceFlag
            ? "TRACE"
            : debugFlag
            ? "DEBUG"
            : (parsedArgs.LogLevel ?? "INFO");
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
            var env = parsedArgs.Env ?? "dev";
            var connectionsPath = parsedArgs.ConnectionsFile ?? "connections.yaml";
            var streamsPathArg = parsedArgs.StreamsFile;
            var streamsFilterArg = parsedArgs.Streams;
            var streamsPath = streamsPathArg ?? "streams.yaml";
            logger.LogInformation("{LogPrefix} Selected environment: {Environment}", appPrefix, env);

            var loader = new YamlLoader();
            var connectionsRoot = loader.Load<ConnectionsRoot>(connectionsPath);
            var streams = loader.Load<StreamsConfig>(streamsPath);

            if (!connectionsRoot.Environments.TryGetValue(env, out var envConfig))
                throw new Exception($"Environment '{env}' not found in {connectionsPath}.");
            if (envConfig.SourceConnections is null || envConfig.SourceConnections.Count == 0)
                throw new Exception($"Environment '{env}' must define at least one source connection under 'sourceConnections' in {connectionsPath}.");
            var destinationConnections = envConfig.GetResolvedDestinationConnections();
            if (destinationConnections.Count == 0)
                throw new Exception($"Environment '{env}' must define at least one destination under 'destinationConnections' in {connectionsPath}.");

            var testConnectionsFlag = parsedArgs.TestConnections;
            var runDeleteDetection = parsedArgs.DeleteDetection;
            var failFast = parsedArgs.FailFast;

            var csvWriter = new CsvGzipWriter();
            var parquetWriter = new ParquetChunkWriter();

            if (testConnectionsFlag)
                return await RunConnectionTestsAsync(envConfig, destinationConnections, logger, appPrefix);

            var resolvedStreams = ResolveStreams(streams, destinationConnections, streamsFilterArg, logger, appPrefix);

            return await ExecuteReplicationAsync(
                streams,
                resolvedStreams,
                envConfig,
                destinationConnections,
                runDeleteDetection,
                failFast,
                csvWriter,
                parquetWriter,
                logger,
                appPrefix);
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

    private static async Task<int> ExecuteReplicationAsync(
        StreamsConfig streams,
        List<ResolvedStreamConfig> resolvedStreams,
        EnvironmentConfig envConfig,
        Dictionary<string, DestinationConnectionConfig> destinationConnections,
        bool runDeleteDetection,
        bool failFast,
        CsvGzipWriter csvWriter,
        ParquetChunkWriter parquetWriter,
        ILogger logger,
        string appPrefix)
    {
        foreach (var stream in resolvedStreams)
            LogResolvedStreamConfig(stream, logger);

        var maxParallelStreams = streams.GetMaxParallelStreams();
        logger.LogInformation(
            "{LogPrefix} Stream execution mode: maxParallelStreams={MaxParallelStreams}, streamCount={StreamCount}",
            appPrefix,
            maxParallelStreams,
            resolvedStreams.Count);
        logger.LogInformation(
            "{LogPrefix} Delete detection runtime switch (--delete_detection): {Enabled}",
            appPrefix,
            runDeleteDetection);
        logger.LogInformation(
            "{LogPrefix} Fail-fast runtime switch (--failfast): {Enabled}",
            appPrefix,
            failFast);

        var failedStreams = new ConcurrentBag<string>();
        using var failFastCts = new CancellationTokenSource();

        if (maxParallelStreams <= 1)
        {
            foreach (var stream in resolvedStreams)
            {
                if (failFastCts.IsCancellationRequested)
                    break;

                await ExecuteStreamWithHandlingAsync(stream, failFastCts.Token, failFast, logger, appPrefix, failedStreams, failFastCts, envConfig, destinationConnections, runDeleteDetection, csvWriter, parquetWriter);
            }
        }
        else
        {
            try
            {
                await Parallel.ForEachAsync(
                    resolvedStreams,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxParallelStreams,
                        CancellationToken = failFastCts.Token
                    },
                    async (stream, cancellationToken) => await ExecuteStreamWithHandlingAsync(stream, failFastCts.Token, failFast, logger, appPrefix, failedStreams, failFastCts, envConfig, destinationConnections, runDeleteDetection, csvWriter, parquetWriter));
            }
            catch (OperationCanceledException) when (failFast && failFastCts.IsCancellationRequested)
            {
                logger.LogInformation("{LogPrefix} Stream execution canceled due to --failfast.", appPrefix);
            }
        }

        if (!failedStreams.IsEmpty)
        {
            var failed = failedStreams
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(name => name)
                .ToList();

            logger.LogError(
                "{LogPrefix} Replication run completed with failed streams ({FailedCount}): {FailedStreams}",
                appPrefix,
                failed.Count,
                string.Join(", ", failed));
            return 1;
        }

        return 0;

        }

    private static async Task ProcessStreamAsync(ResolvedStreamConfig stream, EnvironmentConfig envConfig, Dictionary<string, DestinationConnectionConfig> destinationConnections, bool runDeleteDetection, CsvGzipWriter csvWriter, ParquetChunkWriter parquetWriter, ILogger logger, string appPrefix)
    {
        var streamPrefix = $"[stream={stream.Name}]";
        if (!envConfig.SourceConnections.TryGetValue(stream.SourceConnection, out var sourceCfg))
            throw new Exception(
                $"Stream '{stream.Name}' references unknown sourceConnection '{stream.SourceConnection}'. " +
                $"Defined sourceConnections: {string.Join(", ", envConfig.SourceConnections.Keys.OrderBy(x => x))}.");
        if (!destinationConnections.TryGetValue(stream.DestinationConnection, out var destinationCfg))
            throw new Exception(
                $"Stream '{stream.Name}' references unknown destinationConnection '{stream.DestinationConnection}'. " +
                $"Defined destinationConnections: {string.Join(", ", destinationConnections.Keys.OrderBy(x => x))}.");
        var destinationType = NormalizeDestinationType(destinationCfg.Type);

        var sourceSchemaReader = new SourceSchemaReader(
            sourceCfg.ConnectionString,
            sourceCfg.CommandTimeoutSeconds,
            logger);
        var sourceChunkReader = new SourceChunkReader(
            sourceCfg.ConnectionString,
            sourceCfg.CommandTimeoutSeconds,
            logger);
        var sourceCsb = new SqlConnectionStringBuilder(sourceCfg.ConnectionString);
        logger.LogDebug(
            "{LogPrefix} Source connection details: sourceConnection={SourceConnection}; server={Server}; database={Database}",
            streamPrefix,
            stream.SourceConnection,
            sourceCsb.DataSource,
            sourceCsb.InitialCatalog);

        logger.LogInformation("{LogPrefix} === Stream: {StreamName} ===", streamPrefix, stream.Name);
        var stagingFileFormat = NormalizeStagingFileFormat(stream.StagingFileFormat);
        logger.LogInformation("{LogPrefix} Stream staging file format: {StagingFileFormat}", streamPrefix, stagingFileFormat);
        logger.LogInformation(
            "{LogPrefix} Stream destination: destinationConnection={DestinationConnection}, destinationType={DestinationType}",
            streamPrefix,
            stream.DestinationConnection,
            destinationType);

        OneLakeUploader? uploader = null;
        WarehouseSchemaManager? fabricSchemaManager = null;
        WarehouseLoader? fabricLoader = null;
        SqlServerDestinationSchemaManager? sqlServerSchemaManager = null;
        SqlServerDestinationLoader? sqlServerLoader = null;
        string? destinationDefaultSchema = null;

        if (destinationType == "fabricWarehouse")
        {
            var tokenProvider = new TokenProvider(destinationCfg.Auth);
            uploader = new OneLakeUploader(destinationCfg.OneLakeStaging, tokenProvider, logger);
            var warehouseConnFactory = new WarehouseConnectionFactory(destinationCfg.FabricWarehouse, tokenProvider, logger);
            fabricSchemaManager = new WarehouseSchemaManager(warehouseConnFactory, logger);
            fabricLoader = new WarehouseLoader(warehouseConnFactory, logger);
            destinationDefaultSchema = destinationCfg.FabricWarehouse.TargetSchema;
        }
        else if (destinationType == "sqlServer")
        {
            var sqlServerFactory = new SqlServerDestinationConnectionFactory(destinationCfg.SqlServer, logger);
            sqlServerSchemaManager = new SqlServerDestinationSchemaManager(sqlServerFactory, logger);
            sqlServerLoader = new SqlServerDestinationLoader(sqlServerFactory, logger);
            destinationDefaultSchema = destinationCfg.SqlServer.TargetSchema;
        }
        else
        {
            throw new Exception(
                $"Unsupported destination type '{destinationCfg.Type}' for destinationConnection '{stream.DestinationConnection}'. " +
                "Supported values: fabricWarehouse, sqlServer.");
        }

        var targetSchema = stream.TargetSchema ?? destinationDefaultSchema ?? "dbo";
        var targetTable = stream.TargetTable;
        var sqlServerBufferChunksToCsv =
            destinationType == "sqlServer" &&
            stream.SqlServer.BufferChunksToCsvBeforeBulkCopy == true;
        if (destinationType == "sqlServer")
        {
            logger.LogInformation(
                "{LogPrefix} SQL Server destination chunk buffering: bufferChunksToCsvBeforeBulkCopy={Enabled}",
                streamPrefix,
                sqlServerBufferChunksToCsv);
        }

        // 1) Discover schema of the source query
        var sourceColumns = await sourceSchemaReader.DescribeQueryAsync(stream.SourceSql, streamPrefix);
        if (sourceColumns.Any(c => c.Name.Equals("_pluck_update_datetime", StringComparison.OrdinalIgnoreCase) ||
                                   c.Name.Equals("_pluck_update_op", StringComparison.OrdinalIgnoreCase)))
        {
            throw new Exception(
                $"Stream '{stream.Name}' source query includes reserved metadata column names " +
                "('_pluck_update_datetime' or '_pluck_update_op').");
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

        // 2) Ensure target table exists and schema is aligned (add new columns if necessary)
        await EnsureTargetTable(stream, streamPrefix, destinationType, fabricSchemaManager, sqlServerSchemaManager, targetSchema, targetTable, sourceColumns);

        var changeTrackingEnabled = stream.ChangeTracking.Enabled == true;
        long? ctVersionToPersistAfterStandard = null;
        if (changeTrackingEnabled)
        {
            var (ok, ctVersionToPersist) = await ProcessChangeTrackingMaybe(
                stream,
                envConfig,
                runDeleteDetection,
                csvWriter,
                parquetWriter,
                logger,
                appPrefix,
                streamPrefix,
                destinationType,
                sourceChunkReader,
                stagingFileFormat,
                uploader,
                fabricLoader,
                sqlServerLoader,
                targetSchema,
                targetTable,
                sourceColumns);
            ctVersionToPersistAfterStandard = ctVersionToPersist;
            if (ok)
            {
                return;
            }
            // If change tracking was not done due to missing prior state, we continue with standard sync. After standard sync completes, the change tracking version will be initialized to the current version, so next run will pick up changes since this run's snapshot.
        }

        // 3) Read watermark from target
        object? targetMax = destinationType == "fabricWarehouse"
            ? await fabricLoader!.GetMaxUpdateKeyAsync(targetSchema, targetTable, stream.UpdateKey, streamPrefix)
            : await sqlServerLoader!.GetMaxUpdateKeyAsync(targetSchema, targetTable, stream.UpdateKey, streamPrefix);
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
            Task<PreparedChunk?>? prepareNextTask = PrepareNextChunkAsync(srcMin, chunkIndex + 1, envConfig, csvWriter, parquetWriter, logger, stream, destinationType, sourceChunkReader, stagingFileFormat, sqlServerBufferChunksToCsv, sourceColumns, srcMax, chunkInterval);

            while (prepareNextTask is not null)
            {
                var prepared = await prepareNextTask;
                if (prepared is null)
                    break;

                prepareNextTask = prepared.NextLowerBound is not null
                    ? PrepareNextChunkAsync(prepared.NextLowerBound, prepared.ChunkIndex + 1, envConfig, csvWriter, parquetWriter, logger, stream, destinationType, sourceChunkReader, stagingFileFormat, sqlServerBufferChunksToCsv, sourceColumns, srcMax, chunkInterval)
                    : null;

                await ProcessPreparedChunkAsync(prepared, stream, envConfig, logger, destinationType, stagingFileFormat, uploader, fabricLoader, sqlServerLoader, targetSchema, targetTable, sourceColumns, srcMax, chunkInterval);
                chunkIndex = prepared.ChunkIndex;
            }
        }
        else
        {
            logger.LogInformation("{LogPrefix} No more rows.", streamPrefix);
        }

        if (runDeleteDetection && string.Equals(stream.DeleteDetection.Type, "subset", StringComparison.OrdinalIgnoreCase))
        {
            await ProcessDeleteDetection(stream, envConfig, csvWriter, logger, streamPrefix, destinationType, sourceChunkReader, uploader, fabricLoader, sqlServerLoader, targetSchema, targetTable, sourceColumns);
        }
        else if (!runDeleteDetection && string.Equals(stream.DeleteDetection.Type, "subset", StringComparison.OrdinalIgnoreCase))
        {
            logger.LogInformation(
                "{LogPrefix} Delete detection configured but skipped because --delete_detection was not specified.",
                streamPrefix);
        }

        if (ctVersionToPersistAfterStandard.HasValue)
        {
            if (destinationType == "fabricWarehouse")
                await fabricLoader!.UpsertStreamChangeTrackingVersionAsync(stream.Name, ctVersionToPersistAfterStandard.Value, streamPrefix);
            else
                await sqlServerLoader!.UpsertStreamChangeTrackingVersionAsync(stream.Name, ctVersionToPersistAfterStandard.Value, streamPrefix);
            logger.LogInformation(
                "{LogPrefix} Change tracking stream state initialized: stream={StreamName}, lastSyncVersion={Version}",
                appPrefix,
                stream.Name,
                ctVersionToPersistAfterStandard.Value);
        }

        logger.LogInformation("{LogPrefix} === Stream {StreamName} complete ===", streamPrefix, stream.Name);
    }

    private static async Task ProcessDeleteDetection(ResolvedStreamConfig stream, EnvironmentConfig envConfig, CsvGzipWriter csvWriter, ILogger logger, string streamPrefix, string destinationType, SourceChunkReader sourceChunkReader, OneLakeUploader uploader, WarehouseLoader fabricLoader, SqlServerDestinationLoader sqlServerLoader, string targetSchema, string targetTable, List<SourceColumn> sourceColumns)
    {
        var pkColumns = stream.PrimaryKey
            .Select(pk => sourceColumns.Single(c => c.Name.Equals(pk, StringComparison.OrdinalIgnoreCase)))
            .ToList();
        var keyStream = sourceChunkReader.ReadColumnsAsync(stream.SourceSql, stream.PrimaryKey, stream.DeleteDetection.Where, streamPrefix);

        if (destinationType == "fabricWarehouse")
        {
            var deleteRunId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
            var keysFileName = $"{stream.Name}/run={deleteRunId}/delete-keys.csv.gz";
            var keysLocalPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), "delete-keys.csv.gz");
            Directory.CreateDirectory(Path.GetDirectoryName(keysLocalPath)!);
            logger.LogDebug("{LogPrefix} Delete detection keys local staging path: {LocalPath}", streamPrefix, keysLocalPath);

            var writeKeysSw = System.Diagnostics.Stopwatch.StartNew();
            var keyWrite = await csvWriter.WriteCsvGzAsync(keysLocalPath, pkColumns, keyStream);
            writeKeysSw.Stop();
            logger.LogDebug(
                "{LogPrefix} Delete detection keys file write finished. Rows={RowCount}, ElapsedMs={ElapsedMs:F0}",
                streamPrefix,
                keyWrite.RowCount,
                writeKeysSw.Elapsed.TotalMilliseconds);

            var uploadKeysSw = System.Diagnostics.Stopwatch.StartNew();
            var keysDfsUrl = await uploader!.UploadAsync(keysLocalPath, keysFileName, streamPrefix);
            uploadKeysSw.Stop();

            var keysTempTable = $"__tmp_delkeys_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
            var deleteMetrics = await fabricLoader!.SoftDeleteMissingRowsAsync(
                targetSchema: targetSchema,
                targetTable: targetTable,
                sourceKeysTempTable: keysTempTable,
                primaryKeyColumns: pkColumns,
                expectedSourceKeyRowCount: keyWrite.RowCount,
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
        else
        {
            var keysTempTable = $"__tmp_delkeys_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
            var deleteMetrics = await sqlServerLoader!.SoftDeleteMissingRowsAsync(
                targetSchema: targetSchema,
                targetTable: targetTable,
                sourceKeysTempTable: keysTempTable,
                primaryKeyColumns: pkColumns,
                sourceKeys: keyStream,
                subsetWhere: stream.DeleteDetection.Where,
                cleanup: envConfig.Cleanup,
                logPrefix: streamPrefix);

            logger.LogInformation(
                "{LogPrefix} Delete detection ({Type}) stream {StreamName}: sourceKeys={SourceKeys}, subsetWhere={SubsetWhere}, keyBulkCopyMs={KeyBulkCopyMs:F0}, softDeleteMs={SoftDeleteMs:F0}, softDeletedRows={SoftDeletedRows}",
                streamPrefix,
                stream.DeleteDetection.Type,
                stream.Name,
                deleteMetrics.SourceKeysCopied,
                string.IsNullOrWhiteSpace(stream.DeleteDetection.Where) ? "<none>" : stream.DeleteDetection.Where,
                deleteMetrics.BulkCopyElapsed.TotalMilliseconds,
                deleteMetrics.SoftDeleteElapsed.TotalMilliseconds,
                deleteMetrics.AffectedRows);
        }
    }

    private static async Task ProcessPreparedChunkAsync(PreparedChunk prepared, ResolvedStreamConfig stream, EnvironmentConfig envConfig, ILogger logger, string? destinationType, string? stagingFileFormat, OneLakeUploader? uploader, WarehouseLoader? fabricLoader, SqlServerDestinationLoader? sqlServerLoader, string? targetSchema, string? targetTable, List<SourceColumn>? sourceColumns, object? srcMax, ChunkInterval? chunkInterval)
    {
        var chunkPrefix = ChunkLogPrefix(prepared.ChunkIndex, stream);
        if (destinationType == "fabricWarehouse")
        {
            var uploadSw = System.Diagnostics.Stopwatch.StartNew();
            var oneLakePath = await uploader!.UploadAsync(prepared.LocalPath!, prepared.FileName!, chunkPrefix);
            uploadSw.Stop();
            var uploadedFileSizeKb = new FileInfo(prepared.LocalPath!).Length / 1024d;

            var tempTable = $"__tmp_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
            var warehouseMetrics = await fabricLoader!.LoadAndMergeAsync(
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
                TryDeleteLocalTempFileAndDirectory(prepared.LocalPath!, logger, chunkPrefix);

            if (envConfig.Cleanup.DeleteStagedFiles)
                await uploader.TryDeleteAsync(prepared.FileName!, chunkPrefix);
        }
        else
        {
            var tempTable = $"__tmp_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
            SqlServerDestinationChunkMetrics sqlMetrics;
            if (prepared.LocalPath is not null && prepared.RowStream is null)
            {
                sqlMetrics = await sqlServerLoader!.LoadAndMergeFromCsvAsync(
                    targetSchema: targetSchema,
                    targetTable: targetTable,
                    tempTable: tempTable,
                    columns: sourceColumns,
                    primaryKey: stream.PrimaryKey,
                    csvPath: prepared.LocalPath,
                    expectedRowCount: prepared.RowCount,
                    cleanup: envConfig.Cleanup,
                    logPrefix: chunkPrefix);
            }
            else
            {
                sqlMetrics = await sqlServerLoader!.LoadAndMergeAsync(
                    targetSchema: targetSchema,
                    targetTable: targetTable,
                    tempTable: tempTable,
                    columns: sourceColumns,
                    primaryKey: stream.PrimaryKey,
                    rows: prepared.RowStream!,
                    cleanup: envConfig.Cleanup,
                    logPrefix: chunkPrefix);
            }

            var totalSeconds =
                prepared.WriteElapsed.TotalSeconds +
                sqlMetrics.BulkCopyElapsed.TotalSeconds +
                sqlMetrics.MergeElapsed.TotalSeconds;
            var effectiveRowCount = sqlMetrics.RowsCopied;
            var rowsPerSecond = totalSeconds > 0
                ? effectiveRowCount / totalSeconds
                : 0d;

            if (chunkInterval is null)
            {
                logger.LogInformation(
                    "{LogPrefix} Chunk {ChunkIndex}: rows={RowCount}, lowerBound={LowerBound}, maxInclusive={MaxInclusive}, materializeMs={MaterializeMs:F0}, bulkCopyMs={BulkCopyMs:F0}, mergeMs={MergeMs:F0}, avgRowsPerSec={RowsPerSec:F1}",
                    chunkPrefix,
                    prepared.ChunkIndex,
                    effectiveRowCount,
                    prepared.LowerBound,
                    srcMax,
                    prepared.WriteElapsed.TotalMilliseconds,
                    sqlMetrics.BulkCopyElapsed.TotalMilliseconds,
                    sqlMetrics.MergeElapsed.TotalMilliseconds,
                    rowsPerSecond);
            }
            else
            {
                logger.LogInformation(
                    "{LogPrefix} Chunk {ChunkIndex}: rows={RowCount}, lowerBound={LowerBound}, upperBoundExclusive={UpperBound}, preLoadMs={PreLoadMs:F0}, bulkCopyMs={BulkCopyMs:F0}, mergeMs={MergeMs:F0}, avgRowsPerSec={RowsPerSec:F1}",
                    chunkPrefix,
                    prepared.ChunkIndex,
                    effectiveRowCount,
                    prepared.LowerBound,
                    prepared.UpperBound,
                    prepared.WriteElapsed.TotalMilliseconds,
                    sqlMetrics.BulkCopyElapsed.TotalMilliseconds,
                    sqlMetrics.MergeElapsed.TotalMilliseconds,
                    rowsPerSecond);
            }

            if (envConfig.Cleanup.DeleteLocalTempFiles && prepared.LocalPath is not null)
                TryDeleteLocalTempFileAndDirectory(prepared.LocalPath, logger, chunkPrefix);
        }
    }

    private static async Task EnsureTargetTable(ResolvedStreamConfig stream, string streamPrefix, string destinationType, WarehouseSchemaManager fabricSchemaManager, SqlServerDestinationSchemaManager sqlServerSchemaManager, string targetSchema, string targetTable, List<SourceColumn> sourceColumns)
    {
        if (destinationType == "fabricWarehouse")
        {
            await fabricSchemaManager!.EnsureTableAndSchemaAsync(
                targetSchema,
                targetTable,
                sourceColumns,
                primaryKey: stream.PrimaryKey,
                logPrefix: streamPrefix);
        }
        else
        {
            await sqlServerSchemaManager!.EnsureTableAndSchemaAsync(
                targetSchema,
                targetTable,
                sourceColumns,
                primaryKey: stream.PrimaryKey,
                createClusteredColumnstoreOnCreate: stream.SqlServer.CreateClusteredColumnstoreOnCreate == true,
                logPrefix: streamPrefix);
        }
    }

    // Returns true if Change tracking was successful. False if fallback to standard sync is needed (first run with CT enabled, no prior state).
    private static async Task<(bool Handled, long? CtVersionToPersistAfterStandard)> ProcessChangeTrackingMaybe(
        ResolvedStreamConfig stream,
        EnvironmentConfig envConfig,
        bool runDeleteDetection,
        CsvGzipWriter csvWriter,
        ParquetChunkWriter parquetWriter,
        ILogger logger,
        string appPrefix,
        string streamPrefix,
        string destinationType,
        SourceChunkReader sourceChunkReader,
        string stagingFileFormat,
        OneLakeUploader uploader,
        WarehouseLoader fabricLoader,
        SqlServerDestinationLoader sqlServerLoader,
        string targetSchema,
        string targetTable,
        List<SourceColumn> sourceColumns)
    {
        var ctSourceTable = stream.ChangeTracking.SourceTable!;
        var ctSyncToVersion = await sourceChunkReader.GetChangeTrackingCurrentVersionAsync(streamPrefix);
        if (ctSyncToVersion is null)
            throw new Exception($"Stream '{stream.Name}' is configured for change tracking, but CHANGE_TRACKING_CURRENT_VERSION() returned NULL.");

        var ctStreamState = destinationType == "fabricWarehouse"
            ? await fabricLoader!.GetStreamStateAsync(stream.Name, streamPrefix)
            : await sqlServerLoader!.GetStreamStateAsync(stream.Name, streamPrefix);
        if (ctStreamState?.ChangeTrackingVersion is long ctLastSyncVersion)
        {
            await ProcessChangeTracking(stream, envConfig, runDeleteDetection, csvWriter, parquetWriter, logger, appPrefix, streamPrefix, destinationType, sourceChunkReader, stagingFileFormat, uploader, fabricLoader, sqlServerLoader, targetSchema, targetTable, sourceColumns, ctSourceTable, ctSyncToVersion, ctLastSyncVersion);
            return (true, null);
        }

        logger.LogWarning(
            "{LogPrefix} Change tracking enabled but no prior stream state exists. Falling back to standard sync for this run and initializing change tracking version afterward. stream={StreamName}, sourceTable={SourceTable}, initVersion={InitVersion}",
            appPrefix,
            stream.Name,
            ctSourceTable,
            ctSyncToVersion.Value);
        return (false, ctSyncToVersion.Value);
    }

    private static async Task ProcessChangeTracking(ResolvedStreamConfig stream, EnvironmentConfig envConfig, bool runDeleteDetection, CsvGzipWriter csvWriter, ParquetChunkWriter parquetWriter, ILogger logger, string appPrefix, string streamPrefix, string destinationType, SourceChunkReader sourceChunkReader, string stagingFileFormat, OneLakeUploader uploader, WarehouseLoader fabricLoader, SqlServerDestinationLoader sqlServerLoader, string targetSchema, string targetTable, List<SourceColumn> sourceColumns, string ctSourceTable, long? ctSyncToVersion, long ctLastSyncVersion)
    {
        var ctMinValidVersion = await sourceChunkReader.GetChangeTrackingMinValidVersionAsync(ctSourceTable, streamPrefix);
        if (ctMinValidVersion is null)
            throw new Exception(
                $"Stream '{stream.Name}' has change_tracking enabled but source table '{ctSourceTable}' is not valid for CHANGE_TRACKING_MIN_VALID_VERSION. " +
                "Verify that SQL Server Change Tracking is enabled for the database and source table.");

        if (ctLastSyncVersion < ctMinValidVersion.Value)
        {
            throw new Exception(
                $"Stream '{stream.Name}' has stale change tracking state: lastSyncVersion={ctLastSyncVersion}, minValidVersion={ctMinValidVersion.Value}. " +
                "A re-initialization run is required (remove stream state row and run a full/non-CT sync once).");
        }

        logger.LogInformation(
            "{LogPrefix} Change tracking mode enabled. sourceTable={SourceTable}, fromVersion={FromVersion}, toVersion={ToVersion}, minValidVersion={MinValidVersion}",
            streamPrefix,
            ctSourceTable,
            ctLastSyncVersion,
            ctSyncToVersion.Value,
            ctMinValidVersion.Value);

        var pkColumns = stream.PrimaryKey
            .Select(pk => sourceColumns.Single(c => c.Name.Equals(pk, StringComparison.OrdinalIgnoreCase)))
            .ToList();

        if (destinationType == "fabricWarehouse")
        {
            // Upserts from CHANGETABLE (I/U) joined to current source rows.
            var ext = stagingFileFormat == "parquet" ? "parquet" : (stagingFileFormat == "csv" ? "csv" : "csv.gz");
            var upsertRunId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
            var upsertFileName = $"{stream.Name}/run={upsertRunId}/ct-upserts.{ext}";
            var upsertLocalPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), $"ct-upserts.{ext}");
            Directory.CreateDirectory(Path.GetDirectoryName(upsertLocalPath)!);

            var upsertRows = sourceChunkReader.ReadChangeTrackingUpsertsAsync(
                stream.SourceSql,
                ctSourceTable,
                sourceColumns,
                stream.PrimaryKey,
                ctLastSyncVersion,
                ctSyncToVersion.Value,
                streamPrefix);
            var upsertWriteSw = System.Diagnostics.Stopwatch.StartNew();
            var upsertWrite = stagingFileFormat == "parquet"
                ? await parquetWriter.WriteParquetAsync(upsertLocalPath, sourceColumns, upsertRows)
                : stagingFileFormat == "csv"
                    ? await csvWriter.WriteCsvAsync(upsertLocalPath, sourceColumns, upsertRows)
                    : await csvWriter.WriteCsvGzAsync(upsertLocalPath, sourceColumns, upsertRows);
            upsertWriteSw.Stop();

            if (upsertWrite.RowCount > 0)
            {
                var upsertUploadSw = System.Diagnostics.Stopwatch.StartNew();
                var upsertsDfsUrl = await uploader!.UploadAsync(upsertLocalPath, upsertFileName, streamPrefix);
                upsertUploadSw.Stop();

                var upsertTempTable = $"__tmp_ct_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
                var upsertMetrics = await fabricLoader!.LoadAndMergeAsync(
                    targetSchema: targetSchema,
                    targetTable: targetTable,
                    tempTable: upsertTempTable,
                    columns: sourceColumns,
                    primaryKey: stream.PrimaryKey,
                    expectedRowCount: upsertWrite.RowCount,
                    oneLakeDfsUrl: upsertsDfsUrl,
                    stagingFileFormat: stagingFileFormat,
                    cleanup: envConfig.Cleanup,
                    logPrefix: streamPrefix
                );

                logger.LogInformation(
                    "{LogPrefix} Change tracking upserts: rows={RowCount}, writeMs={WriteMs:F0}, uploadMs={UploadMs:F0}, copyIntoMs={CopyIntoMs:F0}, mergeMs={MergeMs:F0}",
                    streamPrefix,
                    upsertWrite.RowCount,
                    upsertWriteSw.Elapsed.TotalMilliseconds,
                    upsertUploadSw.Elapsed.TotalMilliseconds,
                    upsertMetrics.CopyIntoElapsed.TotalMilliseconds,
                    upsertMetrics.MergeElapsed.TotalMilliseconds);

                if (envConfig.Cleanup.DeleteStagedFiles)
                    await uploader.TryDeleteAsync(upsertFileName, streamPrefix);
            }
            else
            {
                logger.LogInformation("{LogPrefix} Change tracking upserts: no changed insert/update rows.", streamPrefix);
            }

            if (envConfig.Cleanup.DeleteLocalTempFiles)
                TryDeleteLocalTempFileAndDirectory(upsertLocalPath, logger, streamPrefix);

            var deleteRunId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
            var deleteKeysFileName = $"{stream.Name}/run={deleteRunId}/ct-delete-keys.csv.gz";
            var deleteKeysLocalPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), "ct-delete-keys.csv.gz");
            Directory.CreateDirectory(Path.GetDirectoryName(deleteKeysLocalPath)!);

            var deleteKeysRows = sourceChunkReader.ReadChangeTrackingDeletedKeysAsync(
                ctSourceTable,
                stream.PrimaryKey,
                ctLastSyncVersion,
                ctSyncToVersion.Value,
                streamPrefix);
            var deleteWriteSw = System.Diagnostics.Stopwatch.StartNew();
            var deleteKeyWrite = await csvWriter.WriteCsvGzAsync(deleteKeysLocalPath, pkColumns, deleteKeysRows);
            deleteWriteSw.Stop();

            if (deleteKeyWrite.RowCount > 0)
            {
                var deleteUploadSw = System.Diagnostics.Stopwatch.StartNew();
                var deleteKeysDfsUrl = await uploader!.UploadAsync(deleteKeysLocalPath, deleteKeysFileName, streamPrefix);
                deleteUploadSw.Stop();

                var deleteKeysTempTable = $"__tmp_ct_delkeys_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
                var deleteMetrics = await fabricLoader!.SoftDeleteByKeysAsync(
                    targetSchema: targetSchema,
                    targetTable: targetTable,
                    sourceKeysTempTable: deleteKeysTempTable,
                    primaryKeyColumns: pkColumns,
                    expectedSourceKeyRowCount: deleteKeyWrite.RowCount,
                    sourceKeysDfsUrl: deleteKeysDfsUrl,
                    sourceKeysFileFormat: "csv.gz",
                    cleanup: envConfig.Cleanup,
                    logPrefix: streamPrefix);

                logger.LogInformation(
                    "{LogPrefix} Change tracking deletes: sourceDeleteKeys={SourceDeleteKeys}, keyWriteMs={KeyWriteMs:F0}, keyUploadMs={KeyUploadMs:F0}, keyCopyMs={KeyCopyMs:F0}, softDeleteMs={SoftDeleteMs:F0}, softDeletedRows={SoftDeletedRows}",
                    streamPrefix,
                    deleteKeyWrite.RowCount,
                    deleteWriteSw.Elapsed.TotalMilliseconds,
                    deleteUploadSw.Elapsed.TotalMilliseconds,
                    deleteMetrics.CopyIntoElapsed.TotalMilliseconds,
                    deleteMetrics.SoftDeleteElapsed.TotalMilliseconds,
                    deleteMetrics.AffectedRows);

                if (envConfig.Cleanup.DeleteStagedFiles)
                    await uploader.TryDeleteAsync(deleteKeysFileName, streamPrefix);
            }
            else
            {
                logger.LogInformation("{LogPrefix} Change tracking deletes: no deleted rows.", streamPrefix);
            }

            if (envConfig.Cleanup.DeleteLocalTempFiles)
                TryDeleteLocalTempFileAndDirectory(deleteKeysLocalPath, logger, streamPrefix);
        }
        else
        {
            var upsertRows = sourceChunkReader.ReadChangeTrackingUpsertsAsync(
                stream.SourceSql,
                ctSourceTable,
                sourceColumns,
                stream.PrimaryKey,
                ctLastSyncVersion,
                ctSyncToVersion.Value,
                streamPrefix);
            var upsertTempTable = $"__tmp_ct_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
            var upsertMetrics = await sqlServerLoader!.LoadAndMergeAsync(
                targetSchema: targetSchema,
                targetTable: targetTable,
                tempTable: upsertTempTable,
                columns: sourceColumns,
                primaryKey: stream.PrimaryKey,
                rows: upsertRows,
                cleanup: envConfig.Cleanup,
                logPrefix: streamPrefix);

            if (upsertMetrics.RowsCopied > 0)
            {
                logger.LogInformation(
                    "{LogPrefix} Change tracking upserts: rows={RowCount}, bulkCopyMs={BulkCopyMs:F0}, mergeMs={MergeMs:F0}",
                    streamPrefix,
                    upsertMetrics.RowsCopied,
                    upsertMetrics.BulkCopyElapsed.TotalMilliseconds,
                    upsertMetrics.MergeElapsed.TotalMilliseconds);
            }
            else
            {
                logger.LogInformation("{LogPrefix} Change tracking upserts: no changed insert/update rows.", streamPrefix);
            }

            var deleteKeysRows = sourceChunkReader.ReadChangeTrackingDeletedKeysAsync(
                ctSourceTable,
                stream.PrimaryKey,
                ctLastSyncVersion,
                ctSyncToVersion.Value,
                streamPrefix);
            var deleteKeysTempTable = $"__tmp_ct_delkeys_{SqlName.SafeIdentifier(stream.Name)}_{Guid.NewGuid():N}";
            var deleteMetrics = await sqlServerLoader!.SoftDeleteByKeysAsync(
                targetSchema: targetSchema,
                targetTable: targetTable,
                sourceKeysTempTable: deleteKeysTempTable,
                primaryKeyColumns: pkColumns,
                sourceKeys: deleteKeysRows,
                cleanup: envConfig.Cleanup,
                logPrefix: streamPrefix);

            if (deleteMetrics.SourceKeysCopied > 0)
            {
                logger.LogInformation(
                    "{LogPrefix} Change tracking deletes: sourceDeleteKeys={SourceDeleteKeys}, bulkCopyMs={BulkCopyMs:F0}, softDeleteMs={SoftDeleteMs:F0}, softDeletedRows={SoftDeletedRows}",
                    streamPrefix,
                    deleteMetrics.SourceKeysCopied,
                    deleteMetrics.BulkCopyElapsed.TotalMilliseconds,
                    deleteMetrics.SoftDeleteElapsed.TotalMilliseconds,
                    deleteMetrics.AffectedRows);
            }
            else
            {
                logger.LogInformation("{LogPrefix} Change tracking deletes: no deleted rows.", streamPrefix);
            }
        }

        if (destinationType == "fabricWarehouse")
            await fabricLoader!.UpsertStreamChangeTrackingVersionAsync(stream.Name, ctSyncToVersion.Value, streamPrefix);
        else
            await sqlServerLoader!.UpsertStreamChangeTrackingVersionAsync(stream.Name, ctSyncToVersion.Value, streamPrefix);
        logger.LogInformation(
            "{LogPrefix} Change tracking stream state updated: stream={StreamName}, lastSyncVersion={Version}",
            appPrefix,
            stream.Name,
            ctSyncToVersion.Value);

        if (runDeleteDetection && string.Equals(stream.DeleteDetection.Type, "subset", StringComparison.OrdinalIgnoreCase))
        {
            logger.LogInformation(
                "{LogPrefix} Delete detection configured but skipped because change_tracking is enabled.",
                streamPrefix);
        }

        logger.LogInformation("{LogPrefix} === Stream {StreamName} complete ===", streamPrefix, stream.Name);
    }

    private static async Task ExecuteStreamWithHandlingAsync(ResolvedStreamConfig stream, CancellationToken cancellationToken, bool failFast, ILogger logger, string appPrefix, ConcurrentBag<string>? failedStreams, CancellationTokenSource? failFastCts, EnvironmentConfig envConfig, Dictionary<string, DestinationConnectionConfig> destinationConnections, bool runDeleteDetection, CsvGzipWriter csvWriter, ParquetChunkWriter parquetWriter)
    {
        if (cancellationToken.IsCancellationRequested)
            return;

        try
        {
            await ProcessStreamAsync(stream, envConfig, destinationConnections,  runDeleteDetection,  csvWriter,  parquetWriter,  logger,  appPrefix);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(
                "{LogPrefix} Stream execution canceled due to fail-fast cancellation.",
                $"[stream={stream.Name}]");
        }
        catch (Exception ex)
        {
            failedStreams.Add(stream.Name);
            logger.LogError(ex, "{LogPrefix} Stream failed.", $"[stream={stream.Name}]");

            if (failFast && !failFastCts.IsCancellationRequested)
            {
                logger.LogError(
                    "{LogPrefix} --failfast enabled; canceling remaining streams after failure in stream {StreamName}.",
                    appPrefix,
                    stream.Name);
                failFastCts.Cancel();
            }
        }
    }

    private static string ChunkLogPrefix(int idx, ResolvedStreamConfig stream) => $"[stream={stream.Name} chunk={idx:D2}]";

    private static async Task<PreparedChunk?> PrepareNextChunkAsync(object initialLowerBound, int nextChunkIndex, EnvironmentConfig envConfig, CsvGzipWriter csvWriter, ParquetChunkWriter parquetWriter, ILogger logger, ResolvedStreamConfig stream, string? destinationType, SourceChunkReader? sourceChunkReader, string? stagingFileFormat, bool sqlServerBufferChunksToCsv, List<SourceColumn>? sourceColumns, object? srcMax, ChunkInterval? chunkInterval)
    {
        var lowerBound = initialLowerBound;
        while (CompareUpdateKey(lowerBound, srcMax) <= 0)
        {
            var upperBound = chunkInterval is null ? null : AddUpdateKeyInterval(lowerBound, chunkInterval);
            var chunkPrefix = ChunkLogPrefix(nextChunkIndex, stream);

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

            if (destinationType == "fabricWarehouse")
            {
                var ext = stagingFileFormat == "parquet" ? "parquet" : (stagingFileFormat == "csv" ? "csv" : "csv.gz");
                var runId = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff");
                var fileName = $"{stream.Name}/run={runId}/chunk={nextChunkIndex:D6}.{ext}";
                var localPath = Path.Combine(Path.GetTempPath(), "fabric-incr-repl", Guid.NewGuid().ToString("N"), $"chunk.{ext}");
                Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);
                logger.LogDebug("{LogPrefix} Next chunk local staging path: {LocalPath}", chunkPrefix, localPath);

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

                var nextLowerBoundForFabric = chunkInterval is null ? null : upperBound!;
                return new PreparedChunk(
                    ChunkIndex: nextChunkIndex,
                    LowerBound: lowerBound,
                    UpperBound: upperBound,
                    NextLowerBound: nextLowerBoundForFabric,
                    RowCount: chunkWrite.RowCount,
                    WriteElapsed: writeSw.Elapsed,
                    LocalPath: localPath,
                    FileName: fileName,
                    RowStream: null);
            }

            if (destinationType == "sqlServer" && sqlServerBufferChunksToCsv)
            {
                var localPath = Path.Combine(Path.GetTempPath(), "pluck-sqlserver-bulkcopy", Guid.NewGuid().ToString("N"), "chunk.csv");
                Directory.CreateDirectory(Path.GetDirectoryName(localPath)!);
                logger.LogDebug("{LogPrefix} Next chunk local csv spool path: {LocalPath}", chunkPrefix, localPath);

                var writeSw = System.Diagnostics.Stopwatch.StartNew();
                var chunkWrite = await csvWriter.WriteCsvAsync(localPath, sourceColumns, rowStream);
                writeSw.Stop();
                logger.LogDebug("{LogPrefix} Chunk csv spool file written. Elapsed: {Elapsed}ms", chunkPrefix, writeSw.Elapsed.TotalMilliseconds);

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

                var nextLowerBoundForSqlServerCsv = chunkInterval is null ? null : upperBound!;
                return new PreparedChunk(
                    ChunkIndex: nextChunkIndex,
                    LowerBound: lowerBound,
                    UpperBound: upperBound,
                    NextLowerBound: nextLowerBoundForSqlServerCsv,
                    RowCount: chunkWrite.RowCount,
                    WriteElapsed: writeSw.Elapsed,
                    LocalPath: localPath,
                    FileName: null,
                    RowStream: null);
            }

            var nextLowerBoundForSqlServer = chunkInterval is null ? null : upperBound!;
            return new PreparedChunk(
                ChunkIndex: nextChunkIndex,
                LowerBound: lowerBound,
                UpperBound: upperBound,
                NextLowerBound: nextLowerBoundForSqlServer,
                RowCount: 0,
                WriteElapsed: TimeSpan.Zero,
                LocalPath: null,
                FileName: null,
                RowStream: rowStream);
        }

        return null;
    }

    private static async Task<int> RunConnectionTestsAsync(
        EnvironmentConfig envConfig,
        Dictionary<string, DestinationConnectionConfig> destinationConnections,
        ILogger logger,
        string appPrefix)
    {
        logger.LogInformation("{LogPrefix} Running connection tests...", appPrefix);

        foreach (var (sourceName, sourceCfg) in envConfig.SourceConnections)
        {
            try
            {
                await using var conn = new SqlConnection(sourceCfg.ConnectionString);
                await conn.OpenAsync();
                logger.LogInformation("{LogPrefix} Source SQL ({SourceConnection}): OK", appPrefix, sourceName);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "{LogPrefix} Source SQL ({SourceConnection}): FAILED", appPrefix, sourceName);
                return 2;
            }
        }

        foreach (var (destinationName, destinationCfg) in destinationConnections)
        {
            var destinationType = NormalizeDestinationType(destinationCfg.Type);
            try
            {
                if (destinationType == "fabricWarehouse")
                {
                    var tokenProvider = new TokenProvider(destinationCfg.Auth);
                    using var conn = await new WarehouseConnectionFactory(destinationCfg.FabricWarehouse, tokenProvider, logger)
                        .OpenAsync(logPrefix: appPrefix);
                    await new OneLakeUploader(destinationCfg.OneLakeStaging, tokenProvider, logger).TestConnectionAsync();
                }
                else if (destinationType == "sqlServer")
                {
                    using var conn = await new SqlServerDestinationConnectionFactory(destinationCfg.SqlServer, logger)
                        .OpenAsync(logPrefix: appPrefix);
                }
                else
                {
                    throw new Exception(
                        $"Unsupported destination type '{destinationCfg.Type}' for destinationConnection '{destinationName}'. Supported values: fabricWarehouse, sqlServer.");
                }

                logger.LogInformation(
                    "{LogPrefix} Destination ({DestinationConnection}, type={DestinationType}): OK",
                    appPrefix,
                    destinationName,
                    destinationType);
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "{LogPrefix} Destination ({DestinationConnection}, type={DestinationType}): FAILED",
                    appPrefix,
                    destinationName,
                    destinationType);
                return 3;
            }
        }

        logger.LogInformation("{LogPrefix} All connection checks passed.", appPrefix);
        return 0;
    }

    private static List<ResolvedStreamConfig> ResolveStreams(
        StreamsConfig streamsConfig,
        Dictionary<string, DestinationConnectionConfig> destinationConnections,
        string? streamsFilterArg,
        ILogger logger,
        string appPrefix)
    {
        var resolvedStreams = streamsConfig.GetResolvedStreams();
        if (resolvedStreams.Any(s => string.IsNullOrWhiteSpace(s.DestinationConnection)))
        {
            if (destinationConnections.Count == 1)
            {
                var implicitDestination = destinationConnections.Keys.Single();
                foreach (var stream in resolvedStreams.Where(s => string.IsNullOrWhiteSpace(s.DestinationConnection)))
                    stream.DestinationConnection = implicitDestination;
            }
            else
            {
                throw new Exception(
                    "One or more streams are missing destinationConnection and multiple destinationConnections are defined. " +
                    "Set defaults.destinationConnection (or per-stream destinationConnection) in streams.yaml.");
            }
        }

        if (string.IsNullOrWhiteSpace(streamsFilterArg))
            return resolvedStreams;

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
        return resolvedStreams;
    }

    private static void LogResolvedStreamConfig(ResolvedStreamConfig stream, ILogger logger)
    {
        var streamPrefix = $"[stream={stream.Name}]";
        logger.LogDebug(
            "{LogPrefix} Resolved stream config: sourceConnection={SourceConnection}; destinationConnection={DestinationConnection}; sourceSql={SourceSql}; targetSchema={TargetSchema}; targetTable={TargetTable}; primaryKey=[{PrimaryKey}]; excludeColumns=[{ExcludeColumns}]; updateKey={UpdateKey}; chunkSize={ChunkSize}; stagingFileFormat={StagingFileFormat}; sqlServerBufferChunksToCsvBeforeBulkCopy={SqlServerBufferChunksToCsvBeforeBulkCopy}; sqlServerCreateClusteredColumnstoreOnCreate={SqlServerCreateClusteredColumnstoreOnCreate}; deleteDetectionType={DeleteDetectionType}; deleteDetectionWhere={DeleteDetectionWhere}; changeTrackingEnabled={ChangeTrackingEnabled}; changeTrackingSourceTable={ChangeTrackingSourceTable}",
            streamPrefix,
            stream.SourceConnection,
            stream.DestinationConnection,
            stream.SourceSql,
            stream.TargetSchema ?? "<env-default>",
            stream.TargetTable,
            string.Join(", ", stream.PrimaryKey),
            stream.ExcludeColumns.Count > 0 ? string.Join(", ", stream.ExcludeColumns) : "<none>",
            stream.UpdateKey,
            stream.ChunkSize ?? "<none>",
            stream.StagingFileFormat,
            stream.SqlServer.BufferChunksToCsvBeforeBulkCopy == true,
            stream.SqlServer.CreateClusteredColumnstoreOnCreate == true,
            stream.DeleteDetection.Type,
            string.IsNullOrWhiteSpace(stream.DeleteDetection.Where) ? "<none>" : stream.DeleteDetection.Where,
            stream.ChangeTracking.Enabled == true,
            stream.ChangeTracking.SourceTable ?? "<none>");
    }

    private static bool TryParseArgs(string[] args, out ParsedArgs parsed, out string? error)
    {
        parsed = new ParsedArgs();
        error = null;

        var valueOptions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "--env",
            "--connections-file",
            "--streams-file",
            "--streams",
            "--log-level"
        };
        var flagOptions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "--help",
            "-h",
            "--test-connections",
            "--delete_detection",
            "--failfast",
            "--debug",
            "--trace"
        };
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < args.Length; i++)
        {
            var token = args[i];
            if (!token.StartsWith("-", StringComparison.Ordinal))
            {
                error = $"Unexpected positional argument '{token}'.";
                return false;
            }

            string option;
            string? valueInline = null;
            var eqIdx = token.IndexOf('=');
            if (eqIdx > 0)
            {
                option = token[..eqIdx];
                valueInline = token[(eqIdx + 1)..];
            }
            else
            {
                option = token;
            }

            if (!valueOptions.Contains(option) && !flagOptions.Contains(option))
            {
                error = $"Unknown parameter '{option}'.";
                return false;
            }

            if (seen.Contains(option))
            {
                error = $"Parameter '{option}' was specified more than once.";
                return false;
            }
            seen.Add(option);

            if (flagOptions.Contains(option))
            {
                if (valueInline is not null)
                {
                    error = $"Parameter '{option}' does not accept a value.";
                    return false;
                }

                if (option.Equals("--help", StringComparison.OrdinalIgnoreCase) || option.Equals("-h", StringComparison.OrdinalIgnoreCase))
                    parsed.Help = true;
                else if (option.Equals("--test-connections", StringComparison.OrdinalIgnoreCase))
                    parsed.TestConnections = true;
                else if (option.Equals("--delete_detection", StringComparison.OrdinalIgnoreCase))
                    parsed.DeleteDetection = true;
                else if (option.Equals("--failfast", StringComparison.OrdinalIgnoreCase))
                    parsed.FailFast = true;
                else if (option.Equals("--debug", StringComparison.OrdinalIgnoreCase))
                    parsed.Debug = true;
                else if (option.Equals("--trace", StringComparison.OrdinalIgnoreCase))
                    parsed.Trace = true;

                continue;
            }

            string value;
            if (valueInline is not null)
            {
                value = valueInline;
            }
            else
            {
                if (i + 1 >= args.Length)
                {
                    error = $"Parameter '{option}' requires a value.";
                    return false;
                }

                var next = args[++i];
                if (next.StartsWith("-", StringComparison.Ordinal))
                {
                    error = $"Parameter '{option}' requires a value.";
                    return false;
                }
                value = next;
            }

            if (string.IsNullOrWhiteSpace(value))
            {
                error = $"Parameter '{option}' requires a non-empty value.";
                return false;
            }

            if (option.Equals("--env", StringComparison.OrdinalIgnoreCase))
                parsed.Env = value;
            else if (option.Equals("--connections-file", StringComparison.OrdinalIgnoreCase))
                parsed.ConnectionsFile = value;
            else if (option.Equals("--streams-file", StringComparison.OrdinalIgnoreCase))
                parsed.StreamsFile = value;
            else if (option.Equals("--streams", StringComparison.OrdinalIgnoreCase))
                parsed.Streams = value;
            else if (option.Equals("--log-level", StringComparison.OrdinalIgnoreCase))
                parsed.LogLevel = value;
        }

        return true;
    }

    private static void PrintHelp()
    {
        // Keep this list in sync with runtime argument handling in Main when adding/removing parameters.
        Console.WriteLine("Usage:");
        Console.WriteLine("  pluck [options]");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --help, -h");
        Console.WriteLine("      Show this help text and exit.");
        Console.WriteLine();
        Console.WriteLine("  --env <name>");
        Console.WriteLine("      Environment key from connections file. Default: dev");
        Console.WriteLine();
        Console.WriteLine("  --connections-file <path>");
        Console.WriteLine("      Path to connections yaml. Default: connections.yaml");
        Console.WriteLine();
        Console.WriteLine("  --streams-file <path>");
        Console.WriteLine("      Path to streams yaml. Default: streams.yaml");
        Console.WriteLine();
        Console.WriteLine("  --streams <name1,name2,...>");
        Console.WriteLine("      Run only the specified comma-separated stream names.");
        Console.WriteLine();
        Console.WriteLine("  --test-connections");
        Console.WriteLine("      Validate source SQL, warehouse SQL, and staging connections, then exit.");
        Console.WriteLine();
        Console.WriteLine("  --delete_detection");
        Console.WriteLine("      Enable delete detection step for streams configured with delete_detection.type: subset.");
        Console.WriteLine();
        Console.WriteLine("  --failfast");
        Console.WriteLine("      Stop scheduling remaining streams after the first stream failure.");
        Console.WriteLine();
        Console.WriteLine("  --log-level <INFO|DEBUG|TRACE|ERROR>");
        Console.WriteLine("      Set minimum log level. Default: INFO");
        Console.WriteLine();
        Console.WriteLine("  --debug");
        Console.WriteLine("      Shortcut for --log-level DEBUG.");
        Console.WriteLine();
        Console.WriteLine("  --trace");
        Console.WriteLine("      Shortcut for --log-level TRACE. Overrides --debug and --log-level.");
    }

    private sealed class ParsedArgs
    {
        public bool Help { get; set; }
        public bool TestConnections { get; set; }
        public bool DeleteDetection { get; set; }
        public bool FailFast { get; set; }
        public bool Debug { get; set; }
        public bool Trace { get; set; }
        public string? Env { get; set; }
        public string? ConnectionsFile { get; set; }
        public string? StreamsFile { get; set; }
        public string? Streams { get; set; }
        public string? LogLevel { get; set; }
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

    private static string NormalizeDestinationType(string? value)
    {
        var normalized = (value ?? "fabricWarehouse").Trim().ToLowerInvariant();
        return normalized switch
        {
            "fabricwarehouse" => "fabricWarehouse",
            "fabric" => "fabricWarehouse",
            "sqlserver" => "sqlServer",
            "sql_server" => "sqlServer",
            "sql" => "sqlServer",
            _ => value ?? "fabricWarehouse"
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
        int RowCount,
        TimeSpan WriteElapsed,
        string? LocalPath,
        string? FileName,
        IAsyncEnumerable<object?[]>? RowStream);

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
