using System.Globalization;
using FabricIncrementalReplicator.Source;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace FabricIncrementalReplicator.Staging;

public sealed class ParquetChunkWriter
{
    private const int RowGroupSize = 50_000;

    private enum ColumnKind
    {
        String,
        Boolean,
        Int16,
        Int32,
        Int64,
        Float,
        Double,
        Decimal,
        DateTime
    }

    private sealed record ColumnPlan(string Name, ColumnKind Kind, DataField Field, List<object?> Buffer);

    public async Task<ChunkWriteResult> WriteParquetAsync(
        string path,
        List<SourceColumn> columns,
        IAsyncEnumerable<object?[]> rows,
        int updateKeyIndex,
        CancellationToken ct = default)
    {
        var plans = columns
            .Select(c =>
            {
                var kind = GetColumnKind(c.SqlServerTypeName);
                return new ColumnPlan(c.Name, kind, BuildField(c.Name, kind), new List<object?>(RowGroupSize));
            })
            .ToList();

        var schema = new ParquetSchema(plans.Select(p => (Field)p.Field).ToArray());

        var rowCount = 0;
        object? maxUpdateKey = null;
        FileStream? fs = null;
        ParquetWriter? parquetWriter = null;

        try
        {
            await foreach (var row in rows.WithCancellation(ct))
            {
                if (parquetWriter is null)
                {
                    fs = File.Create(path);
                    parquetWriter = await ParquetWriter.CreateAsync(schema, fs, cancellationToken: ct);
                }

                for (var i = 0; i < plans.Count; i++)
                    plans[i].Buffer.Add(row[i]);

                rowCount++;

                if (updateKeyIndex >= 0 && updateKeyIndex < row.Length && row[updateKeyIndex] is not null)
                    maxUpdateKey = row[updateKeyIndex];

                if (plans[0].Buffer.Count >= RowGroupSize)
                    await FlushRowGroupAsync(parquetWriter, plans, ct);
            }

            if (parquetWriter is not null && plans[0].Buffer.Count > 0)
                await FlushRowGroupAsync(parquetWriter, plans, ct);
        }
        finally
        {
            parquetWriter?.Dispose();
            if (fs is not null)
                await fs.DisposeAsync();
        }

        return new ChunkWriteResult(rowCount, maxUpdateKey);
    }

    private static ColumnKind GetColumnKind(string sqlServerTypeName)
    {
        var t = sqlServerTypeName.Trim().ToLowerInvariant();
        var p = t.IndexOf('(');
        if (p > 0)
            t = t[..p];

        return t switch
        {
            "bit" => ColumnKind.Boolean,
            "smallint" => ColumnKind.Int16,
            "int" => ColumnKind.Int32,
            "bigint" => ColumnKind.Int64,
            "real" => ColumnKind.Float,
            "float" => ColumnKind.Double,
            "decimal" => ColumnKind.Decimal,
            "numeric" => ColumnKind.Decimal,
            "money" => ColumnKind.Decimal,
            "smallmoney" => ColumnKind.Decimal,
            "datetime" => ColumnKind.DateTime,
            "datetime2" => ColumnKind.DateTime,
            "smalldatetime" => ColumnKind.DateTime,
            "date" => ColumnKind.DateTime,
            _ => ColumnKind.String
        };
    }

    private static DataField BuildField(string name, ColumnKind kind)
    {
        return kind switch
        {
            ColumnKind.Boolean => new DataField<bool?>(name),
            ColumnKind.Int16 => new DataField<short?>(name),
            ColumnKind.Int32 => new DataField<int?>(name),
            ColumnKind.Int64 => new DataField<long?>(name),
            ColumnKind.Float => new DataField<float?>(name),
            ColumnKind.Double => new DataField<double?>(name),
            ColumnKind.Decimal => new DataField<decimal?>(name),
            ColumnKind.DateTime => new DataField<DateTime?>(name),
            _ => new DataField<string>(name),
        };
    }

    private static async Task FlushRowGroupAsync(ParquetWriter writer, List<ColumnPlan> plans, CancellationToken ct)
    {
        using var rowGroup = writer.CreateRowGroup();

        foreach (var plan in plans)
        {
            var col = BuildDataColumn(plan);
            await rowGroup.WriteColumnAsync(col, ct);
            plan.Buffer.Clear();
        }
    }

    private static DataColumn BuildDataColumn(ColumnPlan plan)
    {
        var values = plan.Buffer;

        return plan.Kind switch
        {
            ColumnKind.Boolean => new DataColumn(plan.Field, values.Select(v => v is null ? (bool?)null : Convert.ToBoolean(v, CultureInfo.InvariantCulture)).ToArray()),
            ColumnKind.Int16 => new DataColumn(plan.Field, values.Select(v => v is null ? (short?)null : Convert.ToInt16(v, CultureInfo.InvariantCulture)).ToArray()),
            ColumnKind.Int32 => new DataColumn(plan.Field, values.Select(v => v is null ? (int?)null : Convert.ToInt32(v, CultureInfo.InvariantCulture)).ToArray()),
            ColumnKind.Int64 => new DataColumn(plan.Field, values.Select(v => v is null ? (long?)null : Convert.ToInt64(v, CultureInfo.InvariantCulture)).ToArray()),
            ColumnKind.Float => new DataColumn(plan.Field, values.Select(v => v is null ? (float?)null : Convert.ToSingle(v, CultureInfo.InvariantCulture)).ToArray()),
            ColumnKind.Double => new DataColumn(plan.Field, values.Select(v => v is null ? (double?)null : Convert.ToDouble(v, CultureInfo.InvariantCulture)).ToArray()),
            ColumnKind.Decimal => new DataColumn(plan.Field, values.Select(v => v is null ? (decimal?)null : Convert.ToDecimal(v, CultureInfo.InvariantCulture)).ToArray()),
            ColumnKind.DateTime => new DataColumn(plan.Field, values.Select(ToDateTime).ToArray()),
            _ => new DataColumn(plan.Field, values.Select(v => v is null ? null : Convert.ToString(v, CultureInfo.InvariantCulture)).ToArray()),
        };
    }

    private static DateTime? ToDateTime(object? value)
    {
        if (value is null)
            return null;

        if (value is DateTime dt)
            return dt;

        if (value is DateTimeOffset dto)
            return dto.UtcDateTime;

        return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
    }
}
