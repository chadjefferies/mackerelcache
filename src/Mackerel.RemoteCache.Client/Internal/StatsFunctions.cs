using System;
using System.Collections.Generic;
using System.Linq;
using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Client.Internal
{
    internal static class StatsFunctions
    {
        public static double CalculateHitRate(long hits, long misses)
        {
            if (hits == 0 && misses == 0)
            {
                return 0.0;
            }

            return Math.Round(hits / Convert.ToDouble(hits + misses), 4);
        }

        public static PartitionStats SumPartitionStats(IEnumerable<PartitionStats> partitionStats)
        {
            var stat = new PartitionStats();
            var firstResult = partitionStats.First();
            if (firstResult != null)
            {
                stat.PartitionKey = firstResult.PartitionKey;
                stat.Expiration = firstResult.Expiration;
                stat.ExpirationType = firstResult.ExpirationType;
                stat.Persisted = firstResult.Persisted;
                stat.EvictionPolicy = firstResult.EvictionPolicy;
                stat.CreateDate = firstResult.CreateDate;
            }

            foreach (var ps in partitionStats)
            {
                stat.TotalHits += ps.TotalHits;
                stat.TotalMisses += ps.TotalMisses;
                stat.CurrentItemCount += ps.CurrentItemCount;
                stat.TotalEvictionCount += ps.TotalEvictionCount;
                stat.TotalExpiredCount += ps.TotalExpiredCount;
                stat.TotalCacheSize += ps.TotalCacheSize;
                stat.MaxCacheSize += ps.MaxCacheSize;
                if (ps.LastHitDate != null && ps.LastHitDate > stat.LastHitDate)
                {
                    stat.LastHitDate = ps.LastHitDate;
                }
                if (ps.CreateDate != null && ps.CreateDate < stat.CreateDate)
                {
                    stat.CreateDate = ps.CreateDate;
                }
            }

            stat.HitRate = CalculateHitRate(stat.TotalHits, stat.TotalMisses);

            return stat;
        }

        public static CacheStats SumStats(IEnumerable<CacheStats> stats)
        {
            var stat = new CacheStats();
            foreach (var s in stats)
            {
                if (s != null)
                {
                    stat.CurrentItems += s.CurrentItems;
                    stat.TotalItems += s.TotalItems;
                    stat.Hits += s.Hits;
                    stat.Misses += s.Misses;
                    stat.TotalEvictions += s.TotalEvictions;
                    stat.TotalExpirations += s.TotalExpirations;
                    stat.MemoryUsed += s.MemoryUsed;
                    stat.AllocatedMemory += s.AllocatedMemory;
                    stat.ServerTotalMemory += s.ServerTotalMemory;
                    stat.PagedSystemMemorySize += s.PagedSystemMemorySize;
                    stat.PagedMemorySize += s.PagedMemorySize;
                    stat.VirtualMemorySize += s.VirtualMemorySize;
                    stat.WorkingSet += s.WorkingSet;
                    stat.PeakPagedMemorySize += s.PeakPagedMemorySize;
                    stat.PeakVirtualMemorySize += s.PeakVirtualMemorySize;
                    stat.PeakWorkingSet += s.PeakWorkingSet;
                    stat.ServerName = string.IsNullOrEmpty(stat.ServerName) ? s.ServerName : string.Concat(stat.ServerName, ',', s.ServerName);
                    stat.TotalProcessorTime = stat.TotalProcessorTime == null ? s.TotalProcessorTime : stat.TotalProcessorTime + s.TotalProcessorTime;
                    stat.GarbageCollections += s.GarbageCollections;
                    stat.ServerProcessors += s.ServerProcessors;
                    stat.TotalCacheSize += s.TotalCacheSize;
                    stat.TotalReservedCacheSize += s.TotalReservedCacheSize;
                    stat.TotalWatchEvents += s.TotalWatchEvents;
                    stat.HeapFragmentation += s.HeapFragmentation;
                    stat.TotalHeapSize += s.TotalHeapSize;
                    stat.HeapCommitted += s.HeapCommitted;

                    for (int i = 0; i < s.HeapSizes.Count; i++)
                    {
                        if (stat.HeapSizes.Count <= i)
                        {
                            stat.HeapSizes.Add(s.HeapSizes[i]);
                        }
                        else
                        {
                            stat.HeapSizes[i] += s.HeapSizes[i];
                        }
                    }

                    if (s.ModifiedDate != null && s.ModifiedDate >= stat.ModifiedDate)
                    {
                        stat.ModifiedDate = s.ModifiedDate;
                    }
                    if (s.Partitions > stat.Partitions)
                    {
                        stat.Partitions = s.Partitions;
                    }
                    if (s.CurrentWatchStreams > stat.CurrentWatchStreams)
                    {
                        stat.CurrentWatchStreams = s.CurrentWatchStreams;
                    }
                    if (s.CurrentWatches > stat.CurrentWatches)
                    {
                        stat.CurrentWatches = s.CurrentWatches;
                    }
                    if (stat.EvictedTime == null || s.EvictedTime.Seconds >= stat.EvictedTime.Seconds)
                    {
                        stat.EvictedTime = s.EvictedTime;
                    }
                    if (stat.Uptime == null || s.Uptime.Seconds <= stat.Uptime.Seconds)
                    {
                        stat.Uptime = s.Uptime;
                    }
                    if (s.GcPauseTimePercentage > stat.GcPauseTimePercentage)
                    {
                        stat.GcPauseTimePercentage = s.GcPauseTimePercentage;
                    }
                }
            }

            stat.HitRate = CalculateHitRate(stat.Hits, stat.Misses);
            stat.AllocatedMemoryHuman = ToHumanMBString(stat.AllocatedMemory);
            stat.MemoryUsedHuman = ToHumanMBString(stat.MemoryUsed);
            stat.PagedMemorySizeHuman = ToHumanMBString(stat.PagedMemorySize);
            stat.PeakPagedMemorySizeHuman = ToHumanMBString(stat.PeakPagedMemorySize);
            stat.WorkingSetHuman = ToHumanMBString(stat.WorkingSet);
            stat.PeakWorkingSetHuman = ToHumanMBString(stat.PeakWorkingSet);
            stat.PagedSystemMemorySizeHuman = ToHumanMBString(stat.PagedSystemMemorySize);
            stat.VirtualMemorySizeHuman = ToHumanMBString(stat.VirtualMemorySize);
            stat.ServerTotalMemoryHuman = ToHumanMBString(stat.ServerTotalMemory);
            stat.PeakVirtualMemorySizeHuman = ToHumanMBString(stat.PeakVirtualMemorySize);
            stat.TotalCacheSizeHuman = ToHumanMBString(stat.TotalCacheSize);
            stat.TotalReservedCacheSizeHuman = ToHumanMBString(stat.TotalReservedCacheSize);
            stat.HeapFragmentationHuman = ToHumanMBString(stat.HeapFragmentation);
            stat.TotalHeapSizeHuman = ToHumanMBString(stat.TotalHeapSize);
            stat.HeapCommittedHuman = ToHumanMBString(stat.HeapCommitted);
            return stat;
        }

        public static string ToHumanMBString(long value)
        {
            return $"{Convert.ToInt32(value / Math.Pow(1024, 2))} MB";
        }
    }
}
