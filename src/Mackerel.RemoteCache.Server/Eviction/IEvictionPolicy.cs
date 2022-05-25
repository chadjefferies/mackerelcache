using System;
using System.Collections.Generic;
using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Eviction
{
    public interface IEvictionPolicy : IDisposable
    {
        ReadOnlySpan<KeyValuePair<CacheKey, CacheValue>> GetItems(MemoryStorePartition partition, int count, DateTime accessTime);
    }
}
