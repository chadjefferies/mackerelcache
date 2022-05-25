using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Runtime;

namespace Mackerel.RemoteCache.Server.Statistics
{
    public class PartitionStatistics
    {
        private readonly PartitionMetadata _meta;

        private long _lastHit;
        private int _hits;
        private int _misses;
        private int _evictions;
        private int _expirations;
        private int _items;
        private long _totalCacheSize;

        public long LastHit => Volatile.Read(ref _lastHit);
        public int TotalHits => _hits;
        public int TotalMisses => _misses;
        public int TotalEvictionCount => _evictions;
        public int TotalExpiredCount => _expirations;
        public int CurrentItemCount => _items;
        public long TotalCacheSize => _totalCacheSize;

        public RuntimeStatistics Global { get; }

        public PartitionStatistics(PartitionMetadata meta, RuntimeStatistics runtimeStats, DateTime accessTime)
        {
            _meta = meta;
            _lastHit = accessTime.Ticks;
            Global = runtimeStats;
        }

        public void IncrementEvictions()
        {
            Interlocked.Increment(ref _evictions);
            Global.IncrementEvictions();
        }

        public void IncrementEvictionsBy(int i)
        {
            Global.IncrementEvictionCountBy(i);
            Interlocked.Add(ref _evictions, i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementExpirations()
        {
            Interlocked.Increment(ref _expirations);
            Global.IncrementExpirations();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementExpirationsBy(int i)
        {
            Global.IncrementExpirationCountBy(i);
            Interlocked.Add(ref _expirations, i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementCount()
        {
            Interlocked.Increment(ref _items);
            Global.IncrementCount();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DecrementCount()
        {
            Interlocked.Decrement(ref _items);
            Global.DecrementCount();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DecrementCountBy(int i)
        {
            Global.DecrementCountBy(i);
            Interlocked.Add(ref _items, -i);
        }

        public void ResetCount()
        {
            Global.DecrementCountBy(_items);
            Interlocked.Exchange(ref _items, 0);
        }

        public void SetEvictedTime(DateTime accessTime, long evictedTime)
        {
            Global.SetEvictedTime(accessTime.Ticks - evictedTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementHits(DateTime accessTime)
        {
            Volatile.Write(ref _lastHit, accessTime.Ticks);
            Interlocked.Increment(ref _hits);
            Global.IncrementGetHits();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementHitsBy(int i, DateTime accessTime)
        {
            Volatile.Write(ref _lastHit, accessTime.Ticks);
            Interlocked.Add(ref _hits, i);
            Global.IncrementGetHitsBy(i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementMisses()
        {
            Interlocked.Increment(ref _misses);
            Global.IncrementGetMisses();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementMissesBy(int i)
        {
            Interlocked.Add(ref _misses, i);
            Global.IncrementGetMissesBy(i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AdjustSize(int oldValSize, int newValSize)
        {
            if (oldValSize > newValSize)
            {
                DecrementSize(oldValSize - newValSize);
            }
            else if (newValSize > oldValSize)
            {
                IncrementSize(newValSize - oldValSize);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementSize(int i)
        {
            Interlocked.Add(ref _totalCacheSize, i);
            Global.IncrementSize(i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DecrementSize(int i)
        {
            Interlocked.Add(ref _totalCacheSize, -i);
            Global.DecrementSize(i);
        }

        public void ResetSize()
        {
            Global.DecrementSize(_totalCacheSize);
            Interlocked.Exchange(ref _totalCacheSize, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ShouldEvict()
        {
            if (_meta.IsUnboundedCache)
            {
                // if a partition is unbounded, we use the global stats to 
                // gauge eviction. This leads to much less predictable behavior, 
                // but needed since partitions that are implicitly created default to unbounded
                return Global.ShouldEvict();
            }
            else
            {
                return _totalCacheSize > _meta.MaxCacheSize;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasCapacity(int requestedAddition, EvictionPolicy policy)
        {
            if (policy == EvictionPolicy.NoEviction)
            {
                if (!_meta.IsUnboundedCache)
                {
                    return (_totalCacheSize + requestedAddition) <= _meta.MaxCacheSize;
                }
            }

            return true;
        }
    }
}
