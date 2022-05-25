using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Eviction;
using Mackerel.RemoteCache.Server.Persistence;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Util;
using Mackerel.RemoteCache.Server.Watch;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Server.Runtime
{
    /// <summary>
    /// A collection of cache partitions. The entry point for all cache operations. 
    /// </summary>
    public class MemoryStore : IDisposable, IEnumerable<KeyValuePair<CacheKey, MemoryStorePartition>>
    {
        private readonly ILogger<MemoryStore> _logger;

        // KVStore

        private readonly IPartitionStorage _partitionStorage;
        private readonly ConcurrentDictionary<CacheKey, MemoryStorePartition> _partitions;
        private readonly SemaphoreSlim _semaphore;

        public RuntimeStatistics Stats { get; }
        public CacheServerOptions Conf { get; }

        public MemoryStore(
            ILogger<MemoryStore> logger,
            IOptions<CacheServerOptions> conf,
            RuntimeStatistics stats,
            IPartitionStorage partitionStorage)
        {
            _logger = logger;
            Conf = conf.Value;
            Stats = stats;
            _partitions = new ConcurrentDictionary<CacheKey, MemoryStorePartition>(10, 1000);
            _partitionStorage = partitionStorage;
            _semaphore = new SemaphoreSlim(1, 1);
        }

        #region Partition Operations

        public CacheValue Get(string partitionKey, CacheKey key, DateTime accessTime)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)
                || key.Empty()
                || !_partitions.TryGetValue(partitionKey, out var partition))
            {
                Stats.IncrementGetMisses();
                return default;
            }

            return partition.Get(key, accessTime, true, true);
        }

        public CacheDataBlock<CacheEntry> Get(string partitionKey, IReadOnlyList<string> keys, DateTime accessTime)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)
                || !_partitions.TryGetValue(partitionKey, out var partition))
            {
                if (keys.Count > 0)
                    Stats.IncrementGetMissesBy(keys.Count);
                else
                    Stats.IncrementGetMisses();

                return CacheDataBlock<CacheEntry>.Empty();
            }

            return partition.Get(keys, accessTime, true, true);
        }

        public WriteResult Touch(string partitionKey, CacheKey key, DateTime accessTime)
        {
            var value = Get(partitionKey, key, accessTime);
            if (value is null)
            {
                return WriteResult.KeyDoesNotExist;
            }
            return WriteResult.Success;
        }

        public int Touch(string partitionKey, IReadOnlyList<string> keys, DateTime accessTime)
        {
            using var data = Get(partitionKey, keys, accessTime);
            return data.Data.Length;
        }

        public long Ttl(string partitionKey, CacheKey key, DateTime accessTime)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)
                || key.Empty()
                || !_partitions.TryGetValue(partitionKey, out var partition))
            {
                return CacheValue.TTL_NOT_FOUND;
            }

            var cacheValue = partition.Get(key, accessTime, false, true);
            return cacheValue is null
                ? CacheValue.TTL_NOT_FOUND
                : cacheValue.Ttl(accessTime.Ticks, partition.Metadata.ExpirationTicks);
        }

        public IReadOnlyList<KeyValuePair<CacheKey, long>> Ttl(string partitionKey, IReadOnlyList<string> keys, DateTime accessTime)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)
                || !_partitions.TryGetValue(partitionKey, out var partition))
            {
                if (keys.Count > 0)
                    Stats.IncrementGetMissesBy(keys.Count);
                else
                    Stats.IncrementGetMisses();

                return Array.Empty<KeyValuePair<CacheKey, long>>();
            }

            using var dataBlock = partition.Get(keys, accessTime, false, true);
            var data = new List<KeyValuePair<CacheKey, long>>(keys.Count);
            for (int i = 0; i < dataBlock.Data.Length; i++)
            {
                var outgoing = dataBlock.Data[i];
                var ttl = outgoing.Value is null
                    ? CacheValue.TTL_NOT_FOUND
                    : outgoing.Value.Ttl(accessTime.Ticks, partition.Metadata.ExpirationTicks);

                data.Add(new KeyValuePair<CacheKey, long>(outgoing.Key, ttl));
            }

            return data;
        }

        public WriteResult Put(string partitionKey, CacheKey key, ByteString value, DateTime accessTime)
        {
            WriteResult result = TryGetOrAddPartitionImplicit(partitionKey, accessTime, out var partition);
            if (result == WriteResult.Success)
            {
                result = partition.Put(key, value, accessTime);

                while (partition.Stats.ShouldEvict())
                {
                    var selectedPartition = GetEvictionCandidate(partition, 1);
                    if (selectedPartition.Evict(1, accessTime) == 0) break;
                }
            }

            return result;
        }

        public WriteResult Put(string partitionKey, IReadOnlyDictionary<string, ByteString> items, DateTime accessTime)
        {
            WriteResult result = TryGetOrAddPartitionImplicit(partitionKey, accessTime, out var partition);
            if (result == WriteResult.Success)
            {
                result = partition.Put(items, accessTime);

                while (partition.Stats.ShouldEvict())
                {
                    var selectedPartition = GetEvictionCandidate(partition, items.Count);
                    if (selectedPartition.Evict(items.Count, accessTime) == 0) break;
                }
            }

            return result;
        }

        public WriteResult PutIfNotExists(string partitionKey, CacheKey key, ByteString value, DateTime accessTime)
        {
            WriteResult result = TryGetOrAddPartitionImplicit(partitionKey, accessTime, out var partition);
            if (result == WriteResult.Success)
            {
                result = partition.PutIfNotExists(key, value, accessTime);

                while (partition.Stats.ShouldEvict())
                {
                    var selectedPartition = GetEvictionCandidate(partition, 1);
                    if (selectedPartition.Evict(1, accessTime) == 0) break;
                }
            }

            return result;
        }

        public WriteResult PutIfNotExists(string partitionKey, IReadOnlyDictionary<string, ByteString> items, DateTime accessTime)
        {
            WriteResult result = TryGetOrAddPartitionImplicit(partitionKey, accessTime, out var partition);
            if (result == WriteResult.Success)
            {
                result = partition.PutIfNotExists(items, accessTime);

                while (partition.Stats.ShouldEvict())
                {
                    var selectedPartition = GetEvictionCandidate(partition, items.Count);
                    if (selectedPartition.Evict(items.Count, accessTime) == 0) break;
                }
            }

            return result;
        }

        public WriteResult PutIfExists(string partitionKey, CacheKey key, ByteString value, DateTime accessTime)
        {
            WriteResult result = TryGetOrAddPartitionImplicit(partitionKey, accessTime, out var partition);
            if (result == WriteResult.Success)
            {
                result = partition.PutIfExists(key, value, accessTime);

                while (partition.Stats.ShouldEvict())
                {
                    var selectedPartition = GetEvictionCandidate(partition, 1);
                    if (selectedPartition.Evict(1, accessTime) == 0) break;
                }
            }

            return result;
        }

        public WriteResult PutIfExists(string partitionKey, IReadOnlyDictionary<string, ByteString> items, DateTime accessTime)
        {
            WriteResult result = TryGetOrAddPartitionImplicit(partitionKey, accessTime, out var partition);
            if (result == WriteResult.Success)
            {
                result = partition.PutIfExists(items, accessTime);

                while (partition.Stats.ShouldEvict())
                {
                    var selectedPartition = GetEvictionCandidate(partition, items.Count);
                    if (selectedPartition.Evict(items.Count, accessTime) == 0) break;
                }
            }

            return result;
        }

        public WriteResult Delete(string partitionKey, CacheKey key, DateTime accessTime)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)) return WriteResult.MissingPartitionKey;

            if (_partitions.TryGetValue(partitionKey, out var partition))
            {
                return partition.Delete(key, accessTime);
            }

            return WriteResult.PartitionDoesNotExist;
        }

        public int Delete(string partitionKey, IReadOnlyList<string> keys, DateTime accessTime)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)) return 0;

            if (_partitions.TryGetValue(partitionKey, out var partition))
            {
                return partition.Delete(keys, accessTime);
            }

            return 0;

        }

        public WriteResult DeletePartition(string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)) return WriteResult.MissingPartitionKey;

            CacheKey partitionCacheKey = partitionKey;
            if (_partitions.TryGetValue(partitionCacheKey, out var partition))
            {
                // TODO: Predicates need cleaned up here since it blocks us from deleting a partition
                if (partition.HasWatchers())
                {
                    return WriteResult.PartitionInUse;
                }
            }

            if (_partitions.TryRemove(partitionCacheKey, out partition))
            {
                Stats.DecrementPartitions();
                Stats.DecrementSize(partitionCacheKey.Key.Length);
                Stats.DecrementReservedSize(partition.Metadata.MaxCacheSize);
                partition.Flush();
                if (partition.Metadata.IsPersisted)
                {
                    _partitionStorage.Delete(partitionKey);
                }

                partition.Dispose();
                return WriteResult.Success;
            }

            return WriteResult.PartitionDoesNotExist;
        }

        public void FlushPartition(string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)) return;

            if (_partitions.TryGetValue(partitionKey, out var partition))
            {
                partition.Flush();
            }
        }

        public WriteResult TryIncrementValue(string partitionKey, CacheKey key, long value, DateTime accessTime, out long result)
        {
            WriteResult writeResult = TryGetOrAddPartitionImplicit(partitionKey, accessTime, out var partition);
            if (writeResult == WriteResult.Success)
            {
                return partition.TryIncrementValue(key, value, accessTime, out result);
            }

            result = 0;
            return writeResult;
        }

        #endregion

        #region Watch Operations

        public bool PutWatchPredicate(WatcherChannel watcher, string watchId, string partitionKey, CacheKey key, IReadOnlyList<WatchEventType> filters, DateTime accessTime)
        {
            if (string.IsNullOrWhiteSpace(watchId)) return false;

            WriteResult result = TryGetOrAddPartitionImplicit(partitionKey, accessTime, out var partition);
            if (result == WriteResult.Success)
            {
                return partition.PutWatchPredicate(watcher, watchId, partitionKey, key, filters);
            }

            return false;
        }

        public bool DeleteWatchPredicate(WatcherChannel watcher, string watchId, string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(watchId)) return false;
            if (string.IsNullOrWhiteSpace(partitionKey)) return false;

            if (_partitions.TryGetValue(partitionKey, out var partition))
            {
                return partition.DeleteWatchPredicate(watcher, watchId);
            }

            return false;
        }

        #endregion

        #region Utility Operations

        public void FlushAll()
        {
            foreach (var item in _partitions)
            {
                item.Value.Flush();
            }

            Stats.ResetCount();
        }

        public KeyValuePair<CacheKey, MemoryStorePartition> GetPartition(string partitionKey)
        {
            CacheKey partitionCacheKey = partitionKey;
            if (_partitions.TryGetValue(partitionCacheKey, out var partition))
            {
                return new KeyValuePair<CacheKey, MemoryStorePartition>(partitionCacheKey, partition);
            }

            return new KeyValuePair<CacheKey, MemoryStorePartition>(partitionCacheKey, default);
        }

        public async Task<WriteResult> PutPartition(string partitionKey, PartitionMetadata metadata, DateTime accessTime)
        {
            if (string.IsNullOrWhiteSpace(partitionKey)) return WriteResult.MissingPartitionKey;
            if (!partitionKey.IsValidPartitionKey()) return WriteResult.InvalidPartitionKey;

            CacheKey partitionCacheKey = partitionKey;
            if (!_partitions.TryGetValue(partitionCacheKey, out var partition))
            {
                if (partitionCacheKey.Key.Length > Conf.MaxBytesPerKey)
                {
                    return WriteResult.KeyTooLarge;
                }

                if (metadata.MaxCacheSize > Stats.AvailableCapacity
                    || !MemoryStatus.HasSufficientMemory(metadata.MaxCacheSize))
                {
                    return WriteResult.InsufficientCapacity;
                }

                partition = new MemoryStorePartition(
                    Conf,
                    new PartitionStatistics(metadata, Stats, accessTime),
                    metadata,
                    EvictionPolicyFactory.GetEvictionPolicy(metadata.EvictionPolicy));

                if (_partitions.TryAdd(partitionCacheKey, partition))
                {
                    Stats.IncrementPartitions();
                    partition.Stats.IncrementSize(partitionCacheKey.Key.Length);
                    Stats.IncrementReservedSize(metadata.MaxCacheSize);
                }
            }
            else
            {
                if (metadata.MaxCacheSize > partition.Metadata.MaxCacheSize)
                {
                    var requiredMemory = metadata.MaxCacheSize - partition.Metadata.MaxCacheSize;
                    if (requiredMemory > Stats.AvailableCapacity
                        || !MemoryStatus.HasSufficientMemory(requiredMemory))
                    {
                        return WriteResult.InsufficientCapacity;
                    }
                    Stats.IncrementReservedSize(requiredMemory);
                }
                else
                {
                    Stats.DecrementReservedSize(partition.Metadata.MaxCacheSize - metadata.MaxCacheSize);
                }

                partition.Metadata.SetSize(metadata.MaxCacheSize);

                if (metadata.EvictionPolicy != partition.Metadata.EvictionPolicy)
                {
                    partition.SetEvictionPolicy(metadata.EvictionPolicy);
                }

                partition.Metadata.SetExpiration(metadata.ExpirationTicks, metadata.IsAbsoluteExpiration);
            }

            // single threaded file system access
            await _semaphore.WaitAsync();
            try
            {
                if (metadata.IsPersisted)
                {
                    partition.Metadata.SetPersisted(true);
                    // Only explicitly created and requested partitions get persisted, 
                    // the rest are ephemeral
                    await _partitionStorage.Persist(partitionKey, partition);
                }
                else if (partition.Metadata.IsPersisted)
                {
                    partition.Metadata.SetPersisted(false);
                    _partitionStorage.Delete(partitionKey);
                }
            }
            finally
            {
                _semaphore.Release();
            }

            return WriteResult.Success;
        }

        public void Dispose()
        {
            foreach (var item in _partitions)
            {
                item.Value.Dispose();
            }

            _partitions.Clear();
            Stats.Dispose();
            _semaphore.Dispose();
        }

        private WriteResult TryGetOrAddPartitionImplicit(string partitionKey, DateTime accessTime, out MemoryStorePartition partition)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                partition = default;
                return WriteResult.MissingPartitionKey;
            }
            if (!partitionKey.IsValidPartitionKey())
            {
                partition = default;
                return WriteResult.InvalidPartitionKey;
            }

            CacheKey castedPartitionKey = partitionKey;

            if (!_partitions.TryGetValue(castedPartitionKey, out partition))
            {
                if (castedPartitionKey.Key.Length > Conf.MaxBytesPerKey)
                {
                    return WriteResult.KeyTooLarge;
                }

                var metadata = new PartitionMetadata(accessTime.Ticks, 0, false, false, EvictionPolicy.Lru, 0);
                partition = new MemoryStorePartition(
                    Conf,
                    new PartitionStatistics(metadata, Stats, accessTime),
                    metadata,
                    EvictionPolicyFactory.GetEvictionPolicy(metadata.EvictionPolicy));

                if (_partitions.TryAdd(castedPartitionKey, partition))
                {
                    Stats.IncrementPartitions();
                    partition.Stats.IncrementSize(castedPartitionKey.Key.Length);
                }
                else
                {
                    _partitions.TryGetValue(castedPartitionKey, out partition);
                }
            }

            return WriteResult.Success;
        }

        public IEnumerator<KeyValuePair<CacheKey, MemoryStorePartition>> GetEnumerator() => _partitions.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _partitions.GetEnumerator();

        public KeyValuePair<CacheKey, MemoryStorePartition>[] GetPartitions()
        {
            return _partitions.ToArray();
        }

        private MemoryStorePartition GetEvictionCandidate(MemoryStorePartition currentPartition, int count)
        {
            if (!currentPartition.Metadata.IsUnboundedCache)
            {
                // if a partition has a reserved size, it should only evict from itself.
                return currentPartition;
            }
            else
            {
                // if the current partition is unbounded, we can look across all other unbounded partitions and find a suitable candidate

                var selectedPartition = currentPartition;
                int relativeSampleSize = Conf.KeyEvictionSamples * count;
                var randomPartitionIndex = ThreadLocalRandom.Current.Next(Stats.Partitions);
                int index = 0;
                var enumerator = _partitions.GetEnumerator();
                while (enumerator.MoveNext() && index < randomPartitionIndex)
                {
                    var current = enumerator.Current;

                    // we can only look at other unbounded partitions.
                    if (current.Value.Metadata.IsUnboundedCache && current.Value.Metadata.EvictionPolicy != EvictionPolicy.NoEviction)
                    {
                        // make sure the partition has enough keys to evict from
                        if (current.Value.Stats.CurrentItemCount >= relativeSampleSize)
                        {
                            // and its been idle longer
                            if (current.Value.Stats.LastHit < selectedPartition.Stats.LastHit)
                            {
                                selectedPartition = current.Value;
                            }
                            else if (selectedPartition.Metadata.EvictionPolicy == EvictionPolicy.NoEviction)
                            {
                                // should only happen once, if the "currentPartition" was set to NoEviction
                                selectedPartition = current.Value;
                            }
                        }
                    }

                    index++;
                }

                if (selectedPartition.Metadata.EvictionPolicy == EvictionPolicy.NoEviction)
                {
                    _logger.LogWarning("Eviction needed but no eligible partitions found. This may be caused by too many unbounded {evictionType} partitions.", EvictionPolicy.NoEviction);
                }

                return selectedPartition;
            }
        }

        #endregion
    }
}
