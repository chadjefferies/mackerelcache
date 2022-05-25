using System;
using System.Buffers;
using System.Diagnostics;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Util;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Quartz;

namespace Mackerel.RemoteCache.Server.Expiration
{
    /// <summary>
    /// When a key is accessed and is found to be expired, it will be removed from the cache at that time (passively).
    /// However, we need a way to eagerly expire keys that will never be accessed again in order to reduce cache size and ultimately memory size.
    /// The frequency at which this job runs depends on the "EagerExpirationInterval" setting.
    /// </summary>
    [DisallowConcurrentExecution]
    [PersistJobDataAfterExecution]
    public class EagerExpirationJob : IJob
    {
        const string LAST_PARTITION_INDEX_VAR_NAME = "lastPartitionIndex";

        private readonly ILogger<EagerExpirationJob> _logger;

        private readonly MemoryStore _cache;
        private readonly CacheServerOptions _configuration;

        public EagerExpirationJob(ILogger<EagerExpirationJob> logger, MemoryStore cache, IOptions<CacheServerOptions> conf)
        {
            _logger = logger;
            _cache = cache;
            _configuration = conf.Value;
        }

        public Task Execute(IJobExecutionContext context)
        {
            _logger.LogTrace("Running the {jobType}", nameof(EagerExpirationJob));

            var dataMap = context.MergedJobDataMap;
            var lastPartitionIndex = dataMap.GetInt(LAST_PARTITION_INDEX_VAR_NAME) >= _cache.Stats.Partitions ? 0 : dataMap.GetInt(LAST_PARTITION_INDEX_VAR_NAME);
            var fireTime = context.FireTimeUtc.UtcDateTime;
            int[] randomIndexes = default;
            CacheKey[] keysToExpire = default;

            var jobTimer = Stopwatch.StartNew();
            try
            {
                Span<int> spRandomIndexes = default;
                int totalExpired = 0;
                int partitionIndex = 0;

                var partitionEnumerator = _cache.GetEnumerator();
                while (partitionEnumerator.MoveNext()
                    && jobTimer.Elapsed < _configuration.EagerExpirationJobLimit
                    && !context.CancellationToken.IsCancellationRequested)
                {
                    if (partitionIndex < lastPartitionIndex)
                    {
                        partitionIndex++;
                        continue;
                    }

                    var partition = partitionEnumerator.Current.Value;
                    int partitionExpireCount = 0;
                    int currentPartitionIterations = 0;
                    int partitionSampleSize = _configuration.KeyExpirationSamples;

                    do
                    {
                        // reset the count
                        partitionExpireCount = 0;
                        if (partition.Metadata.ExpirationTicks > 0 && partition.Stats.CurrentItemCount > 0)
                        {
                            if (spRandomIndexes.Length < partitionSampleSize)
                            {
                                if (randomIndexes != null) ArrayPool<int>.Shared.Return(randomIndexes);
                                if (keysToExpire != null) ArrayPool<CacheKey>.Shared.Return(keysToExpire, true);
                                randomIndexes = ArrayPool<int>.Shared.Rent(partitionSampleSize);
                                keysToExpire = ArrayPool<CacheKey>.Shared.Rent(partitionSampleSize);
                            }

                            int index = 0;
                            int indexRandom = 0;

                            var threadLocalRandom = ThreadLocalRandom.Current;
                            for (int j = 0; j < Math.Min(partitionSampleSize, partition.Stats.CurrentItemCount); j++)
                            {
                                // CurrentItemCount may change will iterating
                                // we are OK with that since we want to lock the partition for as little time as possible
                                randomIndexes[j] = threadLocalRandom.Next(partition.Stats.CurrentItemCount);
                            }
                            spRandomIndexes = randomIndexes.AsSpan(0, partitionSampleSize);
                            spRandomIndexes.Sort();

                            using (partition.Lock.EnterReadLock())
                            {
                                var itemEnumerator = partition.GetEnumerator();
                                while (itemEnumerator.MoveNext()
                                    && indexRandom < partitionSampleSize
                                    && partitionExpireCount < partitionSampleSize)
                                {
                                    var current = itemEnumerator.Current;
                                    if (spRandomIndexes[indexRandom] < index)
                                    {
                                        // handle dupes in random indexes
                                        indexRandom++;
                                        continue;
                                    }

                                    if (current.Value.IsExpired(fireTime.Ticks, partition.Metadata.ExpirationTicks))
                                    {
                                        keysToExpire[partitionExpireCount] = current.Key;
                                        partitionExpireCount++;
                                    }
                                    else if (spRandomIndexes[indexRandom] == index)
                                    {
                                        indexRandom++;
                                    }

                                    index++;
                                }
                            }

                            if (partitionExpireCount > 0)
                            {
                                var spKeysToExpire = keysToExpire.AsSpan(0, partitionExpireCount);
                                partition.Expire(spKeysToExpire, fireTime);
                                totalExpired += partitionExpireCount;
                            }

                            currentPartitionIterations++;
                        }
                    } while (partitionSampleSize > 0
                        && partitionExpireCount >= (partitionSampleSize * .1)
                        && jobTimer.Elapsed < _configuration.EagerExpirationJobLimit
                        && !context.CancellationToken.IsCancellationRequested);

                    partitionIndex++;
                }

                if (totalExpired > 0)
                {
                    _logger.Log(LogLevel.Debug, default,
                        new LogData("Expiration complete",
                            ("expiredKeys", totalExpired),
                            ("elapsedMilliseconds", jobTimer.ElapsedMilliseconds),
                            ("partitionsScanned", partitionIndex - lastPartitionIndex)),
                        null, LogData.Formatter);
                }

                context.JobDetail.JobDataMap.Put(LAST_PARTITION_INDEX_VAR_NAME, partitionIndex);
                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                throw new JobExecutionException(e, false);
            }
            finally
            {
                if (randomIndexes != null) ArrayPool<int>.Shared.Return(randomIndexes);
                if (keysToExpire != null) ArrayPool<CacheKey>.Shared.Return(keysToExpire, true);
            }
        }
    }
}
