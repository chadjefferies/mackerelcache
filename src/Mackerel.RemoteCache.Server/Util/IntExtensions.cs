namespace Mackerel.RemoteCache.Server.Util
{
    internal static class IntExtensions
    {
        public static string ToMBString(this long bytes)
        {
            return $"{ByteSize.ToUnit(bytes, ByteSize.Unit.MB)} MB";
        }
    }
}
