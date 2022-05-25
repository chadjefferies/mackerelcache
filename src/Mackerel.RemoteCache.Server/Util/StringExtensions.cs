using System.IO;
using System.Text.RegularExpressions;

namespace Mackerel.RemoteCache.Server.Util
{
    internal static class StringExtensions
    {
        private static readonly char[] _invalidFileNameChars = Path.GetInvalidFileNameChars();

        public static Regex FromGlobPattern(this string pattern)
        {
            return new Regex(Regex.Escape(pattern).Replace(@"\*", ".*").Replace(@"\?", "."));
        }

        public static bool IsValidPartitionKey(this string value)
        {
            if (value.IndexOfAny(_invalidFileNameChars) >= 0)
            {
                return false;
            }

            return true;
        }
    }
}
