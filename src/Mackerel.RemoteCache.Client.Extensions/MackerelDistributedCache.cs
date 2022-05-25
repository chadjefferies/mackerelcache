using System;
using System.Threading;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Util;
using Mackerel.RemoteCache.Client.Watch;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;

namespace Mackerel.RemoteCache.Client.Extensions
{
    /// <summary>
    /// Distributed cache implementation using the Mackerel remote cache.
    /// </summary>
    public class MackerelDistributedCache : IDistributedCache, IDisposable
    {
        private readonly ICache<byte[]> _cache;
        private readonly ICacheConnection _connection;
        private readonly MackerelDistributedCacheOptions _options;
        private readonly IMemoryCache _nearCache;
        private readonly SemaphoreSlim _semaphore;
        private readonly ILogger<MackerelDistributedCache> _logger;
        private readonly ISystemClock _systemClock;
        private bool _partitionCreated;
        private bool _watchCreated;

        public MackerelDistributedCache(
            ICacheConnection cacheConnection,
            ICache<byte[]> cache,
            IOptions<MackerelDistributedCacheOptions> options,
            IMemoryCache nearCache,
            ILogger<MackerelDistributedCache> logger,
            ISystemClock systemClock)
        {
            _connection = cacheConnection;
            _cache = cache;
            _options = options.Value;
            _nearCache = nearCache;
            _logger = logger;
            _systemClock = systemClock;
            _semaphore = new SemaphoreSlim(1, 1);
        }

        public byte[] Get(string key)
        {
            return _cache.GetAsync(_options.Partition, key).GetAwaiter().GetResult();
        }

        public async Task<byte[]> GetAsync(string key, CancellationToken token = default)
        {
            if (_options.UseNearCache)
            {
                if (!_watchCreated)
                {
                    await CreateWatchAsync(token).ConfigureAwait(false);
                }

                if (!_nearCache.TryGetValue<byte[]>(GetLocalKey(_options.Partition, key), out var value))
                {
                    value = await _cache.GetAsync(_options.Partition, key, token).ConfigureAwait(false);
                    using var entry = _nearCache.CreateEntry(GetLocalKey(_options.Partition, key));
                    entry.Value = value;
                    entry.AddExpirationToken(new CancellationChangeToken(_cache.WatchToken));
                    entry.SetSize(value?.Length ?? 0);
                }

                return value;
            }

            return await _cache.GetAsync(_options.Partition, key, token).ConfigureAwait(false);

        }

        public void Refresh(string key)
        {
            RefreshAsync(key, CancellationToken.None).GetAwaiter().GetResult();
        }

        public Task RefreshAsync(string key, CancellationToken token = default)
        {
            return _cache.TouchAsync(_options.Partition, key, token);
        }

        public void Remove(string key)
        {
            RemoveAsync(key, CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task RemoveAsync(string key, CancellationToken token = default)
        {
            if (_options.UseNearCache)
            {
                if (!_watchCreated)
                {
                    await CreateWatchAsync(token).ConfigureAwait(false);
                }

                // we'll get these updates via our watch, but since we know ahead of time the result
                // we can make this "optimization"
                using var entry = _nearCache.CreateEntry(GetLocalKey(_options.Partition, key));
                entry.Value = null;
                entry.AddExpirationToken(new CancellationChangeToken(_cache.WatchToken));
                entry.SetSize(0);
            }
            await _cache.DeleteAsync(_options.Partition, key, token);
        }

        public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            SetAsync(key, value, options, CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
        {
            CachePreconditions.CheckNotNull(options, nameof(options));
            if (!_partitionCreated)
            {
                await CreatePartitionAsync(options, token).ConfigureAwait(false);
            }

            if (_options.UseNearCache)
            {
                if (!_watchCreated)
                {
                    await CreateWatchAsync(token).ConfigureAwait(false);
                }

                var cacheEntryOptions = new MemoryCacheEntryOptions()
                    .SetSize(value?.Length ?? 0)
                    .AddExpirationToken(new CancellationChangeToken(_cache.WatchToken));

                // we'll get these updates via our watch, but since we know ahead of time the result
                // we can make this "optimization"
                _nearCache.Set(GetLocalKey(_options.Partition, key), value, cacheEntryOptions);
            }
            await _cache.PutAsync(_options.Partition, key, value, token).ConfigureAwait(false);
        }

        private async Task CreatePartitionAsync(DistributedCacheEntryOptions options, CancellationToken token)
        {
            await _semaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!_partitionCreated)
                {
                    var expType = ExpirationType.Sliding;
                    var expiration = TimeSpan.Zero;
                    if (_options.ExpirationType.HasValue && _options.Expiration.HasValue)
                    {
                        expType = _options.ExpirationType.Value;
                        expiration = _options.Expiration.Value;
                    }
                    else if (options.AbsoluteExpirationRelativeToNow.HasValue)
                    {
                        expType = ExpirationType.Absolute;
                        expiration = options.AbsoluteExpirationRelativeToNow.Value;
                    }
                    else if (options.AbsoluteExpiration.HasValue)
                    {
                        expType = ExpirationType.Absolute;
                        expiration = options.AbsoluteExpiration.Value - _systemClock.UtcNow;
                    }
                    else if (options.SlidingExpiration.HasValue)
                    {
                        expType = ExpirationType.Sliding;
                        expiration = options.SlidingExpiration.Value;
                    }

                    await _connection.PutPartitionAsync(_options.Partition, expiration, expType, true, _options.EvictionPolicy, _options.CacheSize, token).ConfigureAwait(false);

                    _partitionCreated = true;
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        private async Task CreateWatchAsync(CancellationToken token)
        {
            await _semaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!_watchCreated)
                {
                    if (_options.EvictionPolicy != EvictionPolicy.NoEviction && _options.CacheSize == 0)
                    {
                        // less hits on the remote cache means this partition will get ranked higher for eviction
                        _logger.LogWarning("When using a near cache it is advisable to either request a reserved cache size or set NoEviction if there are relatively few keys with short TTLs.");
                    }
                    await _cache.WatchAsync($"{_options.Partition}:*", _options.Partition, UpdateNearCache, token).ConfigureAwait(false);

                    _watchCreated = true;
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public void Dispose()
        {
            _semaphore.Dispose();
        }

        private void UpdateNearCache(WatchEvent<byte[]> watchEvent)
        {
            using var entry = _nearCache.CreateEntry(GetLocalKey(watchEvent.Partition, watchEvent.Key));
            entry.AddExpirationToken(new CancellationChangeToken(_cache.WatchToken));
            entry.SetSize(watchEvent.Value == null ? 0 : watchEvent.Value.Length);

            switch (watchEvent.EventType)
            {
                case WatchEventType.Delete:
                case WatchEventType.Evict:
                case WatchEventType.Expire:
                    {
                        // Setting the near cache to null is preferred over removing the near cache entry
                        // since it will short circuit future remote calls for keys that don't exist.
                        entry.Value = null;
                        break;
                    }
                case WatchEventType.Write:
                    {
                        entry.Value = watchEvent.Value;
                        break;
                    }
                default:
                    throw new NotSupportedException($"{watchEvent.EventType} watch event type not yet supported.");
            }
        }

        private static string GetLocalKey(string partitionKey, string key) =>
            $"__MACKNC:{partitionKey}:{key}";
    }
}
