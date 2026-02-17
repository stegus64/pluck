namespace FabricIncrementalReplicator.Util;

public static class SqlName
{
    public static string SafeIdentifier(string s)
    {
        var chars = s.Select(ch => char.IsLetterOrDigit(ch) ? ch : '_').ToArray();
        var res = new string(chars);
        if (string.IsNullOrWhiteSpace(res)) res = "x";
        return res;
    }
}