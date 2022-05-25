using System;
using System.Collections.Generic;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Util;

namespace Mackerel.RemoteCache.Server.Watch
{
    public class WatchPredicate : IEquatable<WatchPredicate>
    {
        public WatcherChannel Owner { get; }
        public CacheKey WatchId { get; }
        public CacheKey PartitionKey { get; }
        public CacheKey Key { get; }
        public WatchFilterFlags Filter { get; }

        public WatchPredicate(
            WatcherChannel owner,
            CacheKey watchId,
            CacheKey partitionKey,
            CacheKey key,
            IReadOnlyList<WatchEventType> filters)
        {
            Owner = owner;
            WatchId = watchId;
            PartitionKey = partitionKey;
            Key = key;
            for (int i = 0; i < filters.Count; i++)
            {
                Filter |= filters[i].ToWatchFilterFlags();
            }
        }

        public bool IsMatch(CacheKey key, WatchFilterFlags eventType)
        {
            // no need to check partition since a predicate is always stored at the partition level

            if (Filter == WatchFilterFlags.None || Filter.HasFlag(eventType))
            {
                if (!Key.Empty())
                {
                    return key == Key;
                }
                else
                {
                    // key is null, watching the entire partition
                    return true;
                }
            }

            return false;
        }

        public bool Equals(WatchPredicate other)
        {
            return other.Owner == Owner &&
                other.WatchId == WatchId;
        }

        public override bool Equals(object obj)
        {
            if (obj is WatchPredicate k) return Equals(k);
            return Equals((WatchPredicate)obj);
        }

        public override int GetHashCode() => WatchId.GetHashCode();

        public static bool operator ==(WatchPredicate x, WatchPredicate y) => x.Equals(y);

        public static bool operator !=(WatchPredicate x, WatchPredicate y) => !(x == y);
    }
}
