using System.Data;
using Pluck.Source;

namespace Pluck.Target;

internal sealed class AsyncEnumerableObjectDataReader : IDataReader
{
    private readonly List<SourceColumn> _columns;
    private readonly Dictionary<string, int> _ordinals;
    private readonly IAsyncEnumerator<object?[]> _enumerator;
    private object?[]? _current;
    private bool _isClosed;

    public AsyncEnumerableObjectDataReader(List<SourceColumn> columns, IAsyncEnumerable<object?[]> rows)
    {
        _columns = columns;
        _ordinals = columns
            .Select((c, i) => new { c.Name, Index = i })
            .ToDictionary(x => x.Name, x => x.Index, StringComparer.OrdinalIgnoreCase);
        _enumerator = rows.GetAsyncEnumerator();
    }

    public int RowsRead { get; private set; }

    public bool Read()
    {
        EnsureOpen();
        var hasRow = _enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult();
        if (!hasRow)
            return false;

        _current = _enumerator.Current;
        RowsRead++;
        return true;
    }

    public int FieldCount => _columns.Count;
    public object this[int i] => GetValue(i);
    public object this[string name] => GetValue(GetOrdinal(name));
    public int Depth => 0;
    public bool IsClosed => _isClosed;
    public int RecordsAffected => -1;

    public void Close() => Dispose();
    public DataTable? GetSchemaTable() => null;
    public bool NextResult() => false;

    public string GetName(int i) => _columns[i].Name;
    public string GetDataTypeName(int i) => "object";
    public Type GetFieldType(int i) => typeof(object);
    public object GetValue(int i)
    {
        EnsureCurrentRow();
        return _current![i] ?? DBNull.Value;
    }

    public int GetValues(object[] values)
    {
        EnsureCurrentRow();
        var count = Math.Min(values.Length, _columns.Count);
        for (var i = 0; i < count; i++)
            values[i] = _current![i] ?? DBNull.Value;
        return count;
    }

    public int GetOrdinal(string name)
    {
        if (_ordinals.TryGetValue(name, out var idx))
            return idx;
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public bool GetBoolean(int i) => Convert.ToBoolean(GetValue(i));
    public byte GetByte(int i) => Convert.ToByte(GetValue(i));
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
    public char GetChar(int i) => Convert.ToChar(GetValue(i));
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
    public Guid GetGuid(int i) => (Guid)GetValue(i);
    public short GetInt16(int i) => Convert.ToInt16(GetValue(i));
    public int GetInt32(int i) => Convert.ToInt32(GetValue(i));
    public long GetInt64(int i) => Convert.ToInt64(GetValue(i));
    public float GetFloat(int i) => Convert.ToSingle(GetValue(i));
    public double GetDouble(int i) => Convert.ToDouble(GetValue(i));
    public string GetString(int i) => Convert.ToString(GetValue(i)) ?? string.Empty;
    public decimal GetDecimal(int i) => Convert.ToDecimal(GetValue(i));
    public DateTime GetDateTime(int i) => Convert.ToDateTime(GetValue(i));
    public IDataReader GetData(int i) => throw new NotSupportedException();
    public bool IsDBNull(int i)
    {
        EnsureCurrentRow();
        return _current![i] is null or DBNull;
    }

    public void Dispose()
    {
        if (_isClosed)
            return;

        _isClosed = true;
        _enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private void EnsureOpen()
    {
        if (_isClosed)
            throw new ObjectDisposedException(nameof(AsyncEnumerableObjectDataReader));
    }

    private void EnsureCurrentRow()
    {
        EnsureOpen();
        if (_current is null)
            throw new InvalidOperationException("No current row. Call Read() first.");
    }
}
