using System;
using System.Runtime;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Util;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;

namespace Mackerel.RemoteCache.Server.Rpc
{
    public class MaintenanceServiceHandler : MaintenanceServiceBase
    {
        private readonly CacheServerOptions _config;
        private readonly MemoryStore _cache;
        private readonly RuntimeStatistics _statistics;

        public MaintenanceServiceHandler(MemoryStore cache)
        {
            _config = cache.Conf;
            _statistics = cache.Stats;
            _cache = cache;
        }

        public override Task<CacheStats> GetStats(GetStatsRequest request, ServerCallContext context)
        {
            _statistics.Crunch();
            // TODO: Automapper?
            var response = new CacheStats
            {
                AllocatedMemory = _statistics.AllocatedMemory,
                AllocatedMemoryHuman = _statistics.AllocatedMemory.ToMBString(),
                CurrentItems = _statistics.CurrentItems,
                EvictedTime = Duration.FromTimeSpan(TimeSpan.FromMilliseconds(_statistics.EvictedTimeMs)),
                GarbageCollections = _statistics.GarbageCollections,
                HitRate = RuntimeStatistics.CalculateHitRate(_statistics.Hits, _statistics.Misses),
                Hits = _statistics.Hits,
                MemoryUsed = _statistics.MemoryUsed,
                MemoryUsedHuman = _statistics.MemoryUsed.ToMBString(),
                Misses = _statistics.Misses,
                ModifiedDate = new DateTime(_statistics.ModifiedDate, DateTimeKind.Utc).ToTimestamp(),
                Partitions = _statistics.Partitions,
                Pid = _statistics.PID,
                PeakPagedMemorySize = _statistics.PeakPagedMemorySize,
                PeakPagedMemorySizeHuman = _statistics.PeakPagedMemorySize.ToMBString(),
                PagedMemorySize = _statistics.PagedMemorySize,
                PagedMemorySizeHuman = _statistics.PagedMemorySize.ToMBString(),
                WorkingSet = _statistics.WorkingSet,
                WorkingSetHuman = _statistics.WorkingSet.ToMBString(),
                TotalProcessorTime = Duration.FromTimeSpan(TimeSpan.FromMilliseconds(_statistics.TotalProcessorTimeMs)),
                PeakVirtualMemorySize = _statistics.PeakVirtualMemorySize,
                PeakVirtualMemorySizeHuman = _statistics.PeakVirtualMemorySize.ToMBString(),
                ServerName = _statistics.ServerName,
                PeakWorkingSet = _statistics.PeakWorkingSet,
                PeakWorkingSetHuman = _statistics.PeakWorkingSet.ToMBString(),
                ServerProcessors = _statistics.ServerProcessors,
                ServerTotalMemory = _statistics.ServerTotalMemory,
                ServerTotalMemoryHuman = _statistics.ServerTotalMemory.ToMBString(),
                Uptime = Duration.FromTimeSpan(TimeSpan.FromMilliseconds(_statistics.UptimeMs)),
                PagedSystemMemorySize = _statistics.PagedSystemMemorySize,
                PagedSystemMemorySizeHuman = _statistics.PagedSystemMemorySize.ToMBString(),
                TotalEvictions = _statistics.TotalEvictions,
                VirtualMemorySize = _statistics.VirtualMemorySize,
                VirtualMemorySizeHuman = _statistics.VirtualMemorySize.ToMBString(),
                TotalExpirations = _statistics.TotalExpirations,
                TotalItems = _statistics.TotalItems,
                CurrentWatches = _statistics.CurrentWatches,
                CurrentWatchStreams = _statistics.CurrentWatchStreams,
                TotalWatchEvents = _statistics.TotalWatchEvents,
                TotalCacheSize = _statistics.TotalCacheSize,
                TotalCacheSizeHuman = _statistics.TotalCacheSize.ToMBString(),
                TotalReservedCacheSize = _statistics.TotalReservedCacheSize,
                TotalReservedCacheSizeHuman = _statistics.TotalReservedCacheSize.ToMBString(),
                HeapFragmentation = _statistics.HeapFragmentation,
                HeapFragmentationHuman = _statistics.HeapFragmentation.ToMBString(),
                TotalHeapSize = _statistics.TotalHeapSize,
                TotalHeapSizeHuman = _statistics.TotalHeapSize.ToMBString(),
                GcPauseTimePercentage= _statistics.GcPauseTimePercentage,
                HeapCommitted = _statistics.HeapCommitted,
                HeapCommittedHuman= _statistics.HeapCommitted.ToMBString(),
            };
            response.HeapSizes.AddRange(_statistics.HeapSizes);
            return Task.FromResult(response);
        }

        public override Task<PartitionStats> GetPartitionStats(GetPartitionStatsRequest request, ServerCallContext context)
        {
            var partition = _cache.GetPartition(request.PartitionKey);
            if (partition.Value == default)
            {
                return Task.FromResult(new PartitionStats
                {
                    PartitionKey = request.PartitionKey,
                });
            }

            // TODO: Automapper?
            return Task.FromResult(new PartitionStats
            {
                CurrentItemCount = partition.Value.Stats.CurrentItemCount,
                HitRate = RuntimeStatistics.CalculateHitRate(partition.Value.Stats.TotalHits, partition.Value.Stats.TotalMisses),
                Expiration = TimeSpan.FromTicks(partition.Value.Metadata.ExpirationTicks).ToDuration(),
                ExpirationType = partition.Value.Metadata.IsAbsoluteExpiration ? ExpirationType.Absolute : ExpirationType.Sliding,
                LastHitDate = new DateTime(partition.Value.Stats.LastHit, DateTimeKind.Utc).ToTimestamp(),
                PartitionKey = request.PartitionKey,
                TotalEvictionCount = partition.Value.Stats.TotalEvictionCount,
                TotalExpiredCount = partition.Value.Stats.TotalExpiredCount,
                TotalHits = partition.Value.Stats.TotalHits,
                TotalMisses = partition.Value.Stats.TotalMisses,
                Persisted = partition.Value.Metadata.IsPersisted,
                TotalCacheSize = partition.Value.Stats.TotalCacheSize,
                EvictionPolicy = partition.Value.Metadata.EvictionPolicy,
                MaxCacheSize = partition.Value.Metadata.MaxCacheSize,
                CreateDate = new DateTime(partition.Value.Metadata.CreateDate, DateTimeKind.Utc).ToTimestamp(),
            });
        }

        public override Task<PongResponse> Ping(PingRequest request, ServerCallContext context)
        {
            return StaticResponse.Pong;
        }

        public override Task<CacheConfiguration> GetConf(GetConfRequest request, ServerCallContext context)
        {
            // TODO: Automapper?
            return Task.FromResult(new CacheConfiguration
            {
                KeyEvictionSamples = _cache.Conf.KeyEvictionSamples,
                MaxBytesPerKey = _cache.Conf.MaxBytesPerKey,
                MaxBytesPerValue = _cache.Conf.MaxBytesPerValue,
                EvictionSampleRate = _config.EvictionSampleRate,
                MaxCacheSize = _config.MaxCacheSize,
                MaxCacheSizeHuman = ((long)_config.MaxCacheSize).ToMBString(),
            });
        }

        public override Task<InvokeGCResponse> InvokeGC(InvokeGCRequest request, ServerCallContext context)
        {
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Forced);
            return StaticResponse.InvokeGC;
        }
    }
}
