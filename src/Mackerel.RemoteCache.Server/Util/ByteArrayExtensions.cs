using System.Runtime.CompilerServices;
using Google.Protobuf;
using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Util
{
    internal static class ByteArrayExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Empty(this CacheKey value)
        {
            if (value.Key?.Length > 0)
            {
                return false;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Empty(this CacheValue value)
        {
            return value is null || value.Value.Length == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Empty(this ByteString value)
        {
            return value is null || value.Length == 0;
        }
    }
}
