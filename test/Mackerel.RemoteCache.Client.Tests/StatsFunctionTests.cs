using System;
using System.Linq;
using Google.Protobuf.WellKnownTypes;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Internal;
using Xunit;

namespace Mackerel.RemoteCache.Client.Tests
{
    public class StatsFunctionTests
    {
        [Fact]
        public void SumPartitionStats()
        {
            var hd1 = DateTime.SpecifyKind(DateTime.Parse("7:00 PM"), DateTimeKind.Utc).ToTimestamp();
            var hd2 = DateTime.SpecifyKind(DateTime.Parse("7:01 PM"), DateTimeKind.Utc).ToTimestamp();
            var p1 = new PartitionStats
            {
                CurrentItemCount = 1,
                Expiration = TimeSpan.FromHours(1).ToDuration(),
                ExpirationType = ExpirationType.Absolute,
                HitRate = 1.0,
                LastHitDate = hd1,
                PartitionKey = "abc",
                TotalEvictionCount = 1,
                TotalExpiredCount = 1,
                TotalHits = 1,
                TotalMisses = 0,
                CreateDate = hd2,
                TotalCacheSize = 5,
                EvictionPolicy = EvictionPolicy.Lru,
                MaxCacheSize = 10,
                Persisted = true
            };

            var p2 = new PartitionStats
            {
                CurrentItemCount = 1,
                Expiration = TimeSpan.FromHours(1).ToDuration(),
                ExpirationType = ExpirationType.Absolute,
                HitRate = 0.5,
                LastHitDate = hd2,
                PartitionKey = "abc",
                TotalEvictionCount = 1,
                TotalExpiredCount = 1,
                TotalHits = 1,
                TotalMisses = 1,
                CreateDate = hd1,
                TotalCacheSize = 5,
                EvictionPolicy = EvictionPolicy.Lru,
                MaxCacheSize = 10,
                Persisted = true
            };

            var result = StatsFunctions.SumPartitionStats(new[] { p1, p2 });
            Assert.Equal(2, result.CurrentItemCount);
            Assert.Equal(TimeSpan.FromHours(1).ToDuration(), result.Expiration);
            Assert.Equal(ExpirationType.Absolute, result.ExpirationType);
            Assert.Equal(0.6667, result.HitRate);
            Assert.Equal(hd2, result.LastHitDate);
            Assert.Equal("abc", result.PartitionKey);
            Assert.Equal(2, result.TotalEvictionCount);
            Assert.Equal(2, result.TotalExpiredCount);
            Assert.Equal(2, result.TotalHits);
            Assert.Equal(1, result.TotalMisses);
            Assert.Equal(hd1, result.CreateDate);
            Assert.Equal(10, result.TotalCacheSize);
            Assert.Equal(EvictionPolicy.Lru, result.EvictionPolicy);
            Assert.Equal(20, result.MaxCacheSize);
            Assert.True(result.Persisted);
        }

        [Fact]
        public void SumStats()
        {
            var md1 = DateTime.SpecifyKind(DateTime.Parse("2019-05-01 7:00 PM"), DateTimeKind.Utc).ToTimestamp();
            var s1 = new CacheStats
            {
                CurrentItems = 1,
                TotalItems = 1,
                Hits = 1,
                Misses = 0,
                HitRate = 1.0,
                TotalExpirations = 1,
                TotalEvictions = 1,
                Uptime = TimeSpan.FromSeconds(1).ToDuration(),
                MemoryUsed = 1,
                AllocatedMemory = 1,
                Pid = 1,
                ServerTotalMemory = 1,
                PagedSystemMemorySize = 1,
                PagedMemorySize = 1,
                VirtualMemorySize = 1,
                WorkingSet = 1,
                PeakPagedMemorySize = 1,
                PeakVirtualMemorySize = 1,
                PeakWorkingSet = 1,
                ServerName = "a",
                TotalProcessorTime = TimeSpan.FromSeconds(1).ToDuration(),
                ServerProcessors = 1,
                GarbageCollections = 1,
                ModifiedDate = md1,
                Partitions = 2,
                EvictedTime = TimeSpan.FromSeconds(1).ToDuration(),
                CurrentWatches = 1,
                CurrentWatchStreams = 1,
                TotalCacheSize = 1,
                TotalWatchEvents = 1,
                TotalReservedCacheSize = 2,
                HeapFragmentation = 1,
                TotalHeapSize = 1,
                GcPauseTimePercentage = 1.0,
                HeapCommitted = 1,
            };
            s1.HeapSizes.AddRange(Enumerable.Repeat<long>(1, 5));

            var md2 = DateTime.SpecifyKind(DateTime.Parse("2019-05-01 7:01 PM"), DateTimeKind.Utc).ToTimestamp();
            var s2 = new CacheStats
            {
                CurrentItems = 1,
                TotalItems = 1,
                Hits = 1,
                Misses = 1,
                HitRate = 0.5,
                TotalExpirations = 1,
                TotalEvictions = 1,
                Uptime = TimeSpan.FromSeconds(1).ToDuration(),
                MemoryUsed = 1,
                AllocatedMemory = 1,
                Pid = 2,
                ServerTotalMemory = 1,
                PagedSystemMemorySize = 1,
                PagedMemorySize = 1,
                VirtualMemorySize = 1,
                WorkingSet = 1,
                PeakPagedMemorySize = 1,
                PeakVirtualMemorySize = 1,
                PeakWorkingSet = 1,
                ServerName = "b",
                TotalProcessorTime = TimeSpan.FromSeconds(1).ToDuration(),
                ServerProcessors = 1,
                GarbageCollections = 1,
                ModifiedDate = md2,
                Partitions = 1,
                EvictedTime = TimeSpan.FromSeconds(3).ToDuration(),
                CurrentWatches = 1,
                CurrentWatchStreams = 1,
                TotalCacheSize = 1,
                TotalWatchEvents = 1,
                TotalReservedCacheSize = 2,
                HeapFragmentation = 1,
                TotalHeapSize = 1,
                GcPauseTimePercentage = 1.0,
                HeapCommitted = 1,
            };
            s2.HeapSizes.AddRange(Enumerable.Repeat<long>(1, 5));

            var result = StatsFunctions.SumStats(new[] { s1, s2 });

            Assert.Equal(2, result.CurrentItems);
            Assert.Equal(2, result.TotalItems);
            Assert.Equal(2, result.Hits);
            Assert.Equal(1, result.Misses);
            Assert.Equal(0.6667, result.HitRate);
            Assert.Equal(2, result.TotalExpirations);
            Assert.Equal(2, result.TotalEvictions);
            Assert.Equal(TimeSpan.FromSeconds(1).ToDuration(), result.Uptime);
            Assert.Equal(2, result.MemoryUsed);
            Assert.Equal(2, result.AllocatedMemory);
            Assert.Equal(0, result.Pid);
            Assert.Equal(2, result.ServerTotalMemory);
            Assert.Equal(2, result.PagedSystemMemorySize);
            Assert.Equal(2, result.PagedMemorySize);
            Assert.Equal(2, result.VirtualMemorySize);
            Assert.Equal(2, result.WorkingSet);
            Assert.Equal(2, result.PeakPagedMemorySize);
            Assert.Equal(2, result.PeakVirtualMemorySize);
            Assert.Equal(2, result.PeakWorkingSet);
            Assert.Equal("a,b", result.ServerName);
            Assert.Equal(TimeSpan.FromSeconds(2).ToDuration(), result.TotalProcessorTime);
            Assert.Equal(2, result.ServerProcessors);
            Assert.Equal(2, result.GarbageCollections);
            Assert.Equal(md2, result.ModifiedDate);
            Assert.Equal(2, result.Partitions);
            Assert.Equal(TimeSpan.FromSeconds(3).ToDuration(), result.EvictedTime);
            Assert.Equal(1, result.CurrentWatches);
            Assert.Equal(1, result.CurrentWatchStreams);
            Assert.Equal(2, result.TotalCacheSize);
            Assert.Equal(2, result.TotalWatchEvents);
            Assert.Equal(2, result.HeapFragmentation);
            Assert.Equal(2, result.TotalHeapSize);
            Assert.Equal(1.0, result.GcPauseTimePercentage);
            Assert.Equal(2, result.HeapCommitted);
            Assert.Equal(2, result.HeapSizes[0]);
            Assert.Equal(2, result.HeapSizes[1]);
            Assert.Equal(2, result.HeapSizes[2]);
            Assert.Equal(2, result.HeapSizes[3]);
            Assert.Equal(2, result.HeapSizes[4]);
        }
    }
}
