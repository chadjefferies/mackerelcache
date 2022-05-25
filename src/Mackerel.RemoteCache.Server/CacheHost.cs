using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Persistence;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Server
{
    [ExcludeFromCodeCoverage]
    public class CacheHost : IHostedService
    {
        private readonly MemoryStore _cache;
        private readonly CacheServerOptions _options;
        private readonly IPartitionStorage _partitionStorage;
        private readonly ISystemClock _systemClock;
        private readonly ILogger<CacheHost> _logger;

        public CacheHost(
            ILogger<CacheHost> logger,
            MemoryStore cache,
            IOptions<CacheServerOptions> conf,
            IPartitionStorage partitionStorage,
            ISystemClock systemClock)
        {
            _logger = logger;
            _cache = cache;
            _options = conf.Value;
            _partitionStorage = partitionStorage;
            _systemClock = systemClock;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Starting service");
            EnsureMemoryResources();

            _logger.LogInformation("Initializing partition metadata from storage");
            await foreach (var partition in _partitionStorage.RecoverMetaData())
            {
                var result = await _cache.PutPartition(
                        partition.Key,
                        partition.Value,
                        _systemClock.UtcNow.UtcDateTime);
                if (result != WriteResult.Success)
                {
                    _logger.LogWarning("Unable to recover partition. Partition: {partition}, Reason: {result}", partition.Key, result);
                }
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Flushing the cache");
            _cache.FlushAll();
            return Task.CompletedTask;
        }

        private void EnsureMemoryResources()
        {
            if (!_options.IsUnboundedCache)
            {
                try
                {
                    var maxMemory = (long)_options.MaxCacheSize;
                    using var _ = MemoryStatus.GetMemoryFailPoint(maxMemory);

                    var gcThreshold = MemoryStatus.GetHighMemoryThreshold();
                    if (maxMemory >= gcThreshold)
                    {
                        _logger.LogWarning("Running with a cache size larger than GC High Memory Threshold of {gcThreshold}. Unexpected results may occur under memory pressure.", gcThreshold.ToMBString());
                    }
                }
                catch (InsufficientMemoryException e)
                {
                    _logger.LogCritical(e, "There is not enough memory available to start server.");
                    throw;
                }
            }
            else
            {
                _logger.LogWarning("Running with an unbounded cache size. Unexpected results may occur under memory pressure.");
            }

            if (!GCSettings.IsServerGC)
            {
                _logger.LogWarning("GC server mode is not enabled, this could lead to less than optimal performance.");
            }
        }
    }
}
