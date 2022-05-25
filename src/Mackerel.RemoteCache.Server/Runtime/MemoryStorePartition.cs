using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Eviction;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Util;
using Mackerel.RemoteCache.Server.Watch;

namespace Mackerel.RemoteCache.Server.Runtime
{
    /// <summary>
    /// A single partition within the cache. A cache key must only be unique within a partition.
    /// <remarks>
    /// A partition represents a logical grouping of keys and a unit of parallelism within the cache.
    /// </remarks>
    /// </summary>
    public class MemoryStorePartition : IDisposable, IEnumerable<KeyValuePair<CacheKey, CacheValue>>
    {
        // KVPartition

        private readonly Dictionary<CacheKey, CacheValue> _data;
        private List<WatchPredicate> _watchPredicates;

        public IEvictionPolicy EvictionPolicy { get; private set; }
        public PartitionStatistics Stats { get; }
        public PartitionMetadata Metadata { get; }
        public CacheServerOptions Conf { get; }
        internal ReaderWriterLockWrapper Lock { get; }

        internal MemoryStorePartition(
            CacheServerOptions conf,
            PartitionStatistics stats,
            PartitionMetadata metadata,
            IEvictionPolicy evictionPolicy)
        {
            Conf = conf;
            Stats = stats;
            Metadata = metadata;
            Lock = new ReaderWriterLockWrapper();
            EvictionPolicy = evictionPolicy;
            _data = new Dictionary<CacheKey, CacheValue>(1000);
        }

        public CacheValue Get(CacheKey key, DateTime accessTime, bool shouldTouch, bool logHitRate)
        {
            CacheValue cacheValue;
            bool exists = false;
            using (Lock.EnterReadLock())
            {
                exists = _data.TryGetValue(key, out cacheValue);
            }

            if (exists)
            {
                if (cacheValue.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
                {
                    Delete(key, accessTime);
                }
                else
                {
                    if (!Metadata.IsAbsoluteExpiration && shouldTouch)
                    {
                        cacheValue.Touch(accessTime.Ticks);
                    }
                    if (logHitRate) Stats.IncrementHits(accessTime);
                    return cacheValue;
                }
            }

            if (logHitRate) Stats.IncrementMisses();
            return default;
        }

        public CacheDataBlock<CacheEntry> Get(IReadOnlyList<string> keys, DateTime accessTime, bool shouldTouch, bool logHitRate)
        {
            var dataBlock = new CacheDataBlock<CacheEntry>(keys.Count);
            if (keys.Count > 0)
            {
                CacheDataBlock<CacheKey> expiredKeys = default;
                try
                {
                    int expiredCount = 0, hitCount = 0, missCount = 0;
                    using (Lock.EnterReadLock())
                    {
                        for (int i = 0; i < keys.Count; i++)
                        {
                            CacheKey key = keys[i];
                            if (!key.Empty() && _data.TryGetValue(key, out var cacheValue))
                            {
                                if (cacheValue.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
                                {
                                    if (expiredCount == 0)
                                    {
                                        expiredKeys = new CacheDataBlock<CacheKey>(keys.Count);
                                    }

                                    expiredKeys[expiredCount] = key;
                                    expiredCount++;
                                    missCount++;
                                }
                                else
                                {
                                    dataBlock[hitCount].Key = key.Key;
                                    dataBlock[hitCount].Value = cacheValue;
                                    hitCount++;

                                    if (!Metadata.IsAbsoluteExpiration && shouldTouch)
                                    {
                                        cacheValue.Touch(accessTime.Ticks);
                                    }
                                }
                            }
                            else
                            {
                                missCount++;
                            }
                        }
                    }

                    if (hitCount < keys.Count)
                    {
                        dataBlock = dataBlock.Slice(0, hitCount);
                    }

                    if (expiredCount > 0)
                    {
                        Expire(expiredKeys.Data.Slice(0, expiredCount), accessTime);
                    }

                    if (missCount > 0 && logHitRate)
                    {
                        Stats.IncrementMissesBy(missCount);
                    }

                    if (hitCount > 0 && logHitRate)
                    {
                        Stats.IncrementHitsBy(hitCount, accessTime);
                    }
                }
                finally
                {
                    expiredKeys.Dispose();
                }
            }

            return dataBlock;
        }

        public WriteResult Put(CacheKey key, ByteString value, DateTime accessTime)
        {
            WriteResult result;
            var cacheValue = new CacheValue(value, accessTime.Ticks);
            using (Lock.EnterWriteLock())
            {
                if (!ValidatePut(key, cacheValue, accessTime, out result, out var sizeDelta))
                {
                    return result;
                }
                if (!Stats.HasCapacity(sizeDelta, Metadata.EvictionPolicy))
                {
                    return WriteResult.InsufficientCapacity;
                }

                PutWithNoLock(key, cacheValue);
            }

            return result;
        }

        public WriteResult Put(IReadOnlyDictionary<string, ByteString> items, DateTime accessTime)
        {
            using var dataBlock = new CacheDataBlock<CacheEntry>(items.Count);
            using (Lock.EnterWriteLock())
            {
                int keys = 0, totalSizeDelta = 0;
                foreach (var item in items)
                {
                    CacheKey key = item.Key;
                    var value = new CacheValue(item.Value, accessTime.Ticks);
                    if (!ValidatePut(key, value, accessTime, out var result, out var sizeDelta))
                    {
                        return result;
                    }
                    dataBlock[keys].Key = key;
                    dataBlock[keys].Value = value;
                    keys++;
                    totalSizeDelta += sizeDelta;
                }
                if (!Stats.HasCapacity(totalSizeDelta, Metadata.EvictionPolicy))
                {
                    return WriteResult.InsufficientCapacity;
                }

                for (int i = 0; i < keys; i++)
                {
                    PutWithNoLock(dataBlock[i].Key, dataBlock[i].Value);
                }
            }

            return WriteResult.Success;
        }

        private void PutWithNoLock(CacheKey key, CacheValue value)
        {
            if (_data.TryGetValue(key, out var existingValue))
            {
                if (Metadata.IsAbsoluteExpiration)
                {
                    // if absolute expiration, preserve initial access time
                    value.Touch(existingValue.AccessTime);
                }

                // blind update
                _data[key] = value;
                Stats.AdjustSize(existingValue.Value.Length, value.Value.Length);
                WriteWatch(key, value.Value, WatchFilterFlags.Write);
            }
            else
            {
                // add
                _data.Add(key, value);
                Stats.IncrementCount();
                Stats.IncrementSize(key.Key.Length + value.Value.Length);
                WriteWatch(key, value.Value, WatchFilterFlags.Write);
            }
        }

        private bool ValidatePut(CacheKey key, CacheValue value, DateTime accessTime, out WriteResult result, out int sizeDelta)
        {
            sizeDelta = 0;
            if (!ValidateWrite(key, value, out result))
            {
                return false;
            }

            if (_data.TryGetValue(key, out var existingValue))
            {
                if (existingValue.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
                {
                    ExpireWithNoLock(key, accessTime);
                    sizeDelta = key.Key.Length + value.Value.Length;
                }
                else
                {
                    sizeDelta = value.Value.Length - existingValue.Value.Length;
                }
            }
            else
            {
                sizeDelta = key.Key.Length + value.Value.Length;
            }

            result = default;
            return true;
        }

        public WriteResult PutIfNotExists(CacheKey key, ByteString value, DateTime accessTime)
        {
            WriteResult result;
            var cacheValue = new CacheValue(value, accessTime.Ticks);
            using (Lock.EnterWriteLock())
            {
                if (!ValidatePutIfNotExists(key, cacheValue, accessTime, out result))
                {
                    return result;
                }
                if (!Stats.HasCapacity(key.Key.Length + cacheValue.Value.Length, Metadata.EvictionPolicy))
                {
                    return WriteResult.InsufficientCapacity;
                }

                PutIfNotExistsWithNoLock(key, cacheValue);
            }

            return result;
        }

        public WriteResult PutIfNotExists(IReadOnlyDictionary<string, ByteString> items, DateTime accessTime)
        {
            using var dataBlock = new CacheDataBlock<CacheEntry>(items.Count);
            using (Lock.EnterWriteLock())
            {
                int keys = 0, totalSizeDelta = 0;
                foreach (var item in items)
                {
                    CacheKey key = item.Key;
                    var value = new CacheValue(item.Value, accessTime.Ticks);
                    if (!ValidatePutIfNotExists(key, value, accessTime, out var result))
                    {
                        return result;
                    }
                    dataBlock[keys].Key = key;
                    dataBlock[keys].Value = value;
                    keys++;
                    totalSizeDelta += key.Key.Length + value.Value.Length;
                }
                if (!Stats.HasCapacity(totalSizeDelta, Metadata.EvictionPolicy))
                {
                    return WriteResult.InsufficientCapacity;
                }

                for (int i = 0; i < keys; i++)
                {
                    PutIfNotExistsWithNoLock(dataBlock[i].Key, dataBlock[i].Value);
                }
            }

            return WriteResult.Success;
        }

        private void PutIfNotExistsWithNoLock(CacheKey key, CacheValue value)
        {
            _data[key] = value;
            Stats.IncrementCount();
            Stats.IncrementSize(key.Key.Length + value.Value.Length);
            WriteWatch(key, value.Value, WatchFilterFlags.Write);
        }

        private bool ValidatePutIfNotExists(CacheKey key, CacheValue value, DateTime accessTime, out WriteResult result)
        {
            if (!ValidateWrite(key, value, out result))
            {
                return false;
            }

            if (_data.TryGetValue(key, out var existingValue))
            {
                if (existingValue.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
                {
                    ExpireWithNoLock(key, accessTime);
                }
                else
                {
                    result = WriteResult.KeyAlreadyExists;
                    return false;
                }
            }

            result = default;
            return true;
        }

        public WriteResult PutIfExists(CacheKey key, ByteString value, DateTime accessTime)
        {
            WriteResult result;
            var cacheValue = new CacheValue(value, accessTime.Ticks);
            using (Lock.EnterWriteLock())
            {
                if (!ValidatePutIfExists(key, cacheValue, accessTime, out result, out var sizeDelta))
                {
                    return result;
                }
                if (!Stats.HasCapacity(sizeDelta, Metadata.EvictionPolicy))
                {
                    return WriteResult.InsufficientCapacity;
                }

                PutIfExistsWithNoLock(key, cacheValue);
            }

            return result;
        }

        public WriteResult PutIfExists(IReadOnlyDictionary<string, ByteString> items, DateTime accessTime)
        {
            using var dataBlock = new CacheDataBlock<CacheEntry>(items.Count);
            using (Lock.EnterWriteLock())
            {
                int keys = 0, totalSizeDelta = 0;
                foreach (var item in items)
                {
                    CacheKey key = item.Key;
                    var value = new CacheValue(item.Value, accessTime.Ticks);
                    if (!ValidatePutIfExists(key, value, accessTime, out var result, out var sizeDelta))
                    {
                        return result;
                    }
                    dataBlock[keys].Key = key;
                    dataBlock[keys].Value = value;
                    keys++;
                    totalSizeDelta += sizeDelta;
                }
                if (!Stats.HasCapacity(totalSizeDelta, Metadata.EvictionPolicy))
                {
                    return WriteResult.InsufficientCapacity;
                }

                for (int i = 0; i < keys; i++)
                {
                    PutIfExistsWithNoLock(dataBlock[i].Key, dataBlock[i].Value);
                }
            }

            return WriteResult.Success;
        }

        private void PutIfExistsWithNoLock(CacheKey key, CacheValue value)
        {
            if (_data.TryGetValue(key, out var existingValue))
            {
                if (Metadata.IsAbsoluteExpiration)
                {
                    // if absolute expiration, preserve initial access time
                    value.Touch(existingValue.AccessTime);
                }

                _data[key] = value;
                Stats.AdjustSize(existingValue.Value.Length, value.Value.Length);
                WriteWatch(key, value.Value, WatchFilterFlags.Write);
            }
            // else
            // not possible
        }

        private bool ValidatePutIfExists(CacheKey key, CacheValue value, DateTime accessTime, out WriteResult result, out int sizeDelta)
        {
            sizeDelta = 0;
            if (!ValidateWrite(key, value, out result))
            {
                return false;
            }

            if (!_data.TryGetValue(key, out var existingValue))
            {
                result = WriteResult.KeyDoesNotExist;
                return false;
            }
            else if (existingValue.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
            {
                ExpireWithNoLock(key, accessTime);
                result = WriteResult.KeyDoesNotExist;
                return false;
            }
            else
            {
                sizeDelta = value.Value.Length - existingValue.Value.Length;
            }

            result = default;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ValidateWrite(CacheKey key, CacheValue value, out WriteResult result)
        {
            if (!ValidateKeyWrite(key, out result))
            {
                return false;
            }

            if (value.Empty())
            {
                result = WriteResult.MissingValue;
                return false;
            }

            if (value.Value.Length > Conf.MaxBytesPerValue)
            {
                result = WriteResult.ValueTooLarge;
                return false;
            }

            result = default;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ValidateKeyWrite(CacheKey key, out WriteResult result)
        {
            if (key.Empty())
            {
                result = WriteResult.MissingKey;
                return false;
            }

            if (key.Key.Length > Conf.MaxBytesPerKey)
            {
                result = WriteResult.KeyTooLarge;
                return false;
            }

            result = default;
            return true;
        }

        public WriteResult Delete(CacheKey key, DateTime accessTime)
        {
            using (Lock.EnterWriteLock())
            {
                if (!ValidateDelete(key, out var result))
                {
                    return result;
                }

                var delResult = DeleteWithNoLock(key, accessTime);
                switch (delResult)
                {
                    case DeleteResult.Success:
                        Stats.DecrementCount();
                        return WriteResult.Success;
                    case DeleteResult.Expired:
                        Stats.DecrementCount();
                        Stats.IncrementExpirations();
                        return WriteResult.KeyDoesNotExist;
                    case DeleteResult.KeyDoesNotExist:
                        return WriteResult.KeyDoesNotExist;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public int Delete(IReadOnlyList<string> keys, DateTime accessTime)
        {
            int deleted = 0, expired = 0;
            using (Lock.EnterWriteLock())
            {
                for (int i = 0; i < keys.Count; i++)
                {
                    if (!ValidateDelete(keys[i], out _))
                    {
                        return 0;
                    }
                }

                for (int i = 0; i < keys.Count; i++)
                {
                    var result = DeleteWithNoLock(keys[i], accessTime);
                    if (result == DeleteResult.Success)
                    {
                        deleted++;
                    }
                    else if (result == DeleteResult.Expired)
                    {
                        expired++;
                    }
                }
            }

            Stats.IncrementExpirationsBy(expired);
            Stats.DecrementCountBy(deleted + expired);
            return deleted;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ValidateDelete(CacheKey key, out WriteResult result)
        {
            if (key.Empty())
            {
                result = WriteResult.MissingKey;
                return false;
            }

            result = default;
            return true;
        }

        private DeleteResult DeleteWithNoLock(CacheKey key, DateTime accessTime)
        {
            var result = DeleteResult.KeyDoesNotExist;
            if (_data.Remove(key, out var valueToRemove))
            {
                Stats.DecrementSize(key.Key.Length + valueToRemove.Value.Length);
                if (valueToRemove.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
                {
                    WriteWatch(key, valueToRemove.Value, WatchFilterFlags.Expire);
                    return DeleteResult.Expired;
                }
                else
                {
                    WriteWatch(key, valueToRemove.Value, WatchFilterFlags.Delete);
                    return DeleteResult.Success;
                }
            }

            return result;
        }

        public void Expire(ReadOnlySpan<CacheKey> keys, DateTime accessTime)
        {
            int expired = 0;
            using (Lock.EnterWriteLock())
            {
                for (int i = 0; i < keys.Length; i++)
                {
                    if (_data.TryGetValue(keys[i], out var cacheValue))
                    {
                        if (cacheValue.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
                        {
                            var result = DeleteWithNoLock(keys[i], accessTime);
                            expired++;
                        }
                    }
                }
            }

            Stats.IncrementExpirationsBy(expired);
            Stats.DecrementCountBy(expired);
        }

        private void ExpireWithNoLock(CacheKey key, DateTime accessTime)
        {
            if (DeleteWithNoLock(key, accessTime) == DeleteResult.Expired)
            {
                Stats.IncrementExpirations();
            }
            Stats.DecrementCount();
        }

        public void Flush()
        {
            using (Lock.EnterWriteLock())
            {
                if (_data.Count > 0)
                {
                    if (_watchPredicates?.Count > 0)
                    {
                        foreach (var item in _data)
                        {
                            WriteWatch(item.Key, item.Value.Value, WatchFilterFlags.Delete);
                        }
                    }

                    _data.Clear();
                    Stats.ResetCount();
                    Stats.ResetSize();
                }
            };
        }

        public bool EvictKeyWithNoLock(CacheKey key, DateTime accessTime)
        {
            if (_data.Remove(key, out var valueToRemove))
            {
                if (valueToRemove.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
                {
                    Stats.IncrementExpirations();
                    WriteWatch(key, valueToRemove.Value, WatchFilterFlags.Expire);
                }
                else
                {
                    Stats.IncrementEvictions();
                    Stats.SetEvictedTime(accessTime, valueToRemove.AccessTime);
                    WriteWatch(key, valueToRemove.Value, WatchFilterFlags.Evict);
                }

                Stats.DecrementCount();
                Stats.DecrementSize(key.Key.Length + valueToRemove.Value.Length);
                return true;
            }

            return false;
        }

        public int Evict(int count, DateTime accessTime)
        {
            int evicted = 0;
            using (Lock.EnterWriteLock())
            {
                int i = 0;
                var itemsToEvict = EvictionPolicy.GetItems(this, count, accessTime);
                while (i < itemsToEvict.Length && Stats.ShouldEvict())
                {
                    if (EvictKeyWithNoLock(itemsToEvict[i].Key, accessTime))
                    {
                        evicted++;
                    }
                    i++;
                }
            }

            return evicted;
        }

        private void WriteWatch(CacheKey key, byte[] value, WatchFilterFlags eventType)
        {
            if (_watchPredicates?.Count > 0)
            {
                // the more watches, the slower this gets
                for (int i = 0; i < _watchPredicates.Count; i++)
                {
                    if (_watchPredicates[i].IsMatch(key, eventType))
                    {
                        var watchEvent = new ChangeEvent(_watchPredicates[i].WatchId, _watchPredicates[i].PartitionKey, key, value, eventType);
                        if (!_watchPredicates[i].Owner.TryWrite(watchEvent))
                        {
                            // lazy removal of a zombie predicate
                            _watchPredicates.RemoveAt(i);
                        }
                    }
                }
            }
        }

        public bool PutWatchPredicate(WatcherChannel watcher, CacheKey watchId, CacheKey partitionKey, CacheKey key, IReadOnlyList<WatchEventType> filters)
        {
            using (Lock.EnterWriteLock())
            {
                _watchPredicates ??= new List<WatchPredicate>();

                var watchPredicate = new WatchPredicate(watcher, watchId, partitionKey, key, filters);
                // the more watches, the slower this gets
                // adding watches doesn't need to be super performant though
                int i = _watchPredicates.FindIndex(x => x == watchPredicate);
                if (i >= 0)
                {
                    _watchPredicates[i] = watchPredicate;
                    return false;
                }
                else
                {
                    _watchPredicates.Add(watchPredicate);
                    return true;
                }
            }
        }

        public bool DeleteWatchPredicate(WatcherChannel watcher, CacheKey watchId)
        {
            using (Lock.EnterWriteLock())
            {
                if (_watchPredicates?.Count > 0)
                {
                    int i = _watchPredicates.FindIndex(x =>
                        x.Owner == watcher && x.WatchId == watchId);
                    if (i >= 0)
                    {
                        _watchPredicates.RemoveAt(i);
                        return true;
                    }
                }
            }

            return false;
        }

        public void Dispose()
        {
            using (Lock.EnterWriteLock())
            {
                _data.Clear();
                EvictionPolicy.Dispose();
            }

            Lock.Dispose();
        }

        public IEnumerator<KeyValuePair<CacheKey, CacheValue>> GetEnumerator() => _data.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _data.GetEnumerator();

        public void SetEvictionPolicy(EvictionPolicy policy)
        {
            Metadata.SetEvictionPolicy(policy);

            using (Lock.EnterWriteLock())
            {
                EvictionPolicy.Dispose();
                EvictionPolicy = EvictionPolicyFactory.GetEvictionPolicy(Metadata.EvictionPolicy);
            }
        }

        public WriteResult TryIncrementValue(CacheKey key, long value, DateTime accessTime, out long result)
        {
            using (Lock.EnterWriteLock())
            {
                if (ValidateKeyWrite(key, out var writeResult))
                {
                    if (_data.TryGetValue(key, out var existingValue))
                    {
                        if (existingValue.IsExpired(accessTime.Ticks, Metadata.ExpirationTicks))
                        {
                            ExpireWithNoLock(key, accessTime);
                        }
                        else
                        {
                            if (existingValue.TryIncrement(value, out result))
                            {
                                if (!Metadata.IsAbsoluteExpiration)
                                {
                                    existingValue.Touch(accessTime.Ticks);
                                }

                                Stats.IncrementHits(accessTime);
                                WriteWatch(key, existingValue.Value, WatchFilterFlags.Write);
                                return WriteResult.Success;
                            }

                            return WriteResult.InvalidTypeOperation;
                        }
                    }

                    if (Stats.HasCapacity(key.Key.Length + CacheValue.LONG_SIZE, Metadata.EvictionPolicy))
                    {
                        result = value;
                        var cacheValue = new CacheValue(result, accessTime.Ticks);
                        // add
                        _data.Add(key, cacheValue);
                        Stats.IncrementCount();
                        Stats.IncrementSize(key.Key.Length + cacheValue.Value.Length);
                        Stats.IncrementMisses();

                        WriteWatch(key, cacheValue.Value, WatchFilterFlags.Write);
                        return WriteResult.Success;
                    }
                    else
                    {
                        writeResult = WriteResult.InsufficientCapacity;
                    }
                }

                result = 0;
                return writeResult;
            }
        }

        public bool HasWatchers() => _watchPredicates?.Count > 0;

        private enum DeleteResult
        {
            Success = 0,
            Expired = 1,
            KeyDoesNotExist = 2
        }
    }
}