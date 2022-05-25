using System;
using System.Collections.Generic;
using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Eviction
{
    public class NoEvictionPolicy : IEvictionPolicy
    {
        public void Dispose() { }

        public ReadOnlySpan<KeyValuePair<CacheKey, CacheValue>> GetItems(MemoryStorePartition partition, int count, DateTime accessTime)
        {
            return ReadOnlySpan<KeyValuePair<CacheKey, CacheValue>>.Empty;
        }
    }
}
