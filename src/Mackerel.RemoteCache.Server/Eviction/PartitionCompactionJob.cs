using System;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Util;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Quartz;

namespace Mackerel.RemoteCache.Server.Eviction
{
    [DisallowConcurrentExecution]
    public class PartitionCompactionJob : IJob
    {
        private readonly ILogger<PartitionCompactionJob> _logger;
        private readonly MemoryStore _cache;
        private readonly CacheServerOptions _configuration;

        public PartitionCompactionJob(ILogger<PartitionCompactionJob> logger, MemoryStore cache, IOptions<CacheServerOptions> conf)
        {
            _logger = logger;
            _cache = cache;
            _configuration = conf.Value;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                CacheKey partitionKey = context.MergedJobDataMap.GetString("partitionKey");
                var partition = _cache.GetPartition(partitionKey).Value;

                _logger.Log(LogLevel.Debug, default,
                    new LogData("Partition added or updated, running compaction",
                        ("partitionKey", partitionKey),
                        ("maxCacheSize", partition.Metadata.MaxCacheSize),
                        ("evictionPolicy", partition.Metadata.EvictionPolicy)),
                    null, LogData.Formatter);
                int sampleSize = Convert.ToInt32(Math.Ceiling(partition.Stats.CurrentItemCount * _configuration.EvictionSampleRate));
                int count = 0;
                if (partition.Metadata.EvictionPolicy != Api.V1.EvictionPolicy.NoEviction)
                {
                    // in a tight loop, run evict
                    // may cause blocking and locking
                    while (partition.Stats.ShouldEvict())
                    {
                        count += partition.Evict(sampleSize, context.FireTimeUtc.UtcDateTime);
                    }
                }
                _logger.Log(LogLevel.Information, default,
                    new LogData("Partition compaction complete",
                        ("partitionKey", partitionKey),
                        ("evictionPolicy", partition.Metadata.EvictionPolicy),
                        ("evicted", count)),
                    null, LogData.Formatter);

                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                throw new JobExecutionException(e, false);
            }
        }
    }
}
