using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Mackerel.RemoteCache.Client.Util;

namespace Mackerel.RemoteCache.Client.Routing
{
    /// <summary>
    /// Maps a key to a node using a consistent hashing algorithm.
    /// </summary>
    public class ConsistentHashFunction : IHashFunction
    {
        private readonly ICacheNodeChannel[] _channels;

        public ConsistentHashFunction(ICacheConnection connection)
        {
            _channels = connection.GetNodes().ToArray();
        }

        public ICacheNodeChannel Hash(string routeKey)
        {
            var sp = MemoryMarshal.Cast<char, byte>(routeKey.AsSpan());
            var hash = MurmurHash3.Hash128(sp);
            var idx = JumpConsistentHash(hash, _channels.Length);
            return _channels[idx];
        }

        // https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
        // NOTE: doesn't gracefully handle the case where servers are removed, only added
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long JumpConsistentHash(ulong key, int numBuckets)
        {
            long b = -1;
            long j = 0;
            while (j < numBuckets)
            {
                b = j;
                key = key * 2862933555777941757UL + 1;
                j = (long)((b + 1) * ((1L << 31) / ((double)(key >> 33) + 1)));
            }
            return b;
        }
    }
}