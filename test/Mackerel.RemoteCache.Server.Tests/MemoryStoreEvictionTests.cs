using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Eviction;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Tests.Util;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class MemoryStoreEvictionTests
    {
        private readonly Mock<ILogger<MemoryStore>> _mockLogger;
        private readonly Mock<ISystemClock> _mockClock;

        public MemoryStoreEvictionTests()
        {
            _mockClock = new Mock<ISystemClock>();
            _mockLogger = new Mock<ILogger<MemoryStore>>();
            _mockLogger
                .Setup(x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Exception>(),
                    (Func<It.IsAnyType, Exception, string>)It.IsAny<object>()))
                .Callback(() => { });
        }

        [Fact]
        public void ShouldEvict_Below()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
                MaxCacheSize = 4
            };

            using (var cache = CreatePartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = new CacheValue(new byte[] { 22 }, default);
                cache.Put(key, value, default);

                Assert.False(cache.Stats.ShouldEvict());
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void ShouldEvict_Equal()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
                MaxCacheSize = 4
            };

            using (var cache = CreatePartition(conf))
            {
                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = new CacheValue(new byte[] { 22 }, default);
                cache.Put(key1, value1, default);

                var key2 = new CacheKey(new byte[] { 33 });
                var value2 = new CacheValue(new byte[] { 44 }, default);
                cache.Put(key2, value2, default);

                Assert.False(cache.Stats.ShouldEvict());
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void ShouldEvict_Above()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
                MaxCacheSize = 4
            };

            using (var cache = CreatePartition(conf))
            {
                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = new CacheValue(new byte[] { 22 }, default);
                cache.Put(key1, value1, default);

                var key2 = new CacheKey(new byte[] { 33 });
                var value2 = new CacheValue(new byte[] { 44 }, default);
                cache.Put(key2, value2, default);

                var key3 = new CacheKey(new byte[] { 55 });
                var value3 = new CacheValue(new byte[] { 66 }, default);
                cache.Put(key3, value3, default);

                Assert.True(cache.Stats.ShouldEvict());
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(3, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void GetItems_EvictionPolicy_AllKeys()
        {
            using var cachePartition = CreatePartition(new CacheServerOptions
            {
                KeyEvictionSamples = 5
            });

            cachePartition.Put("key1", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:06 PM"));
            cachePartition.Put("key2", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:05 PM"));
            cachePartition.Put("key3", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:04 PM"));
            cachePartition.Put("key4", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:03 PM"));
            cachePartition.Put("key5", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:02 PM"));
            cachePartition.Put("key6", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:01 PM"));
            cachePartition.Put("key7", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:00 PM"));

            var keysToEvict = cachePartition.EvictionPolicy.GetItems(
                cachePartition,
                count: 7,
                DateTime.Parse("3:07 PM"));
            Assert.Equal(7, keysToEvict.Length);
            Assert.Equal("key7", keysToEvict[0].Key);
            Assert.Equal("key6", keysToEvict[1].Key);
            Assert.Equal("key5", keysToEvict[2].Key);
            Assert.Equal("key4", keysToEvict[3].Key);
            Assert.Equal("key3", keysToEvict[4].Key);
            Assert.Equal("key2", keysToEvict[5].Key);
            Assert.Equal("key1", keysToEvict[6].Key);
        }

        [Fact]
        public void GetItems_EvictionPolicy_SmallerSample_RandomSample()
        {
            using var cachePartition = CreatePartition(new CacheServerOptions
            {
                KeyEvictionSamples = 5
            });

            cachePartition.Put("key1", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:00 PM"));
            cachePartition.Put("key2", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:01 PM"));
            cachePartition.Put("key3", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:02 PM"));
            cachePartition.Put("key4", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:03 PM"));
            cachePartition.Put("key5", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:04 PM"));
            cachePartition.Put("key6", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:05 PM"));
            cachePartition.Put("key7", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:06 PM"));

            var keysToEvict = cachePartition.EvictionPolicy.GetItems(
                cachePartition,
                count: 2,
                DateTime.Parse("3:07 PM"));
            Assert.Equal(7, keysToEvict.Length);
            Assert.Equal("key1", keysToEvict[0].Key);
            Assert.Equal("key2", keysToEvict[1].Key);
            Assert.Equal("key3", keysToEvict[2].Key);
            Assert.Equal("key4", keysToEvict[3].Key);
            Assert.Equal("key5", keysToEvict[4].Key);
            Assert.Equal("key6", keysToEvict[5].Key);
            Assert.Equal("key7", keysToEvict[6].Key);
        }

        [Fact]
        public void GetItems_EvictionPolicy_Random_ExpiredKeys()
        {
            using var cachePartition = CreatePartition(new CacheServerOptions
            {
                KeyEvictionSamples = 2
            },
            EvictionPolicy.Lru,
            TimeSpan.FromMinutes(4));

            cachePartition.Put("key1", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:06 PM"));
            cachePartition.Put("key2", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:05 PM"));
            cachePartition.Put("key3", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:04 PM"));
            cachePartition.Put("key4", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:03 PM"));
            cachePartition.Put("key5", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:02 PM"));
            cachePartition.Put("key6", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:01 PM"));
            cachePartition.Put("key7", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:00 PM"));

            var keysToEvict = cachePartition.EvictionPolicy.GetItems(
                cachePartition,
                count: 3,
                DateTime.Parse("3:07 PM"));
            Assert.InRange(keysToEvict.Length, 1, 6);
            // algorithm takes a random sample. Hard to assert anything other than counts.
        }

        [Fact]
        public void GetItems_EvictionPolicy_NoEviction()
        {
            using var cachePartition = CreatePartition(new CacheServerOptions(), EvictionPolicy.NoEviction);

            cachePartition.Put("key1", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:00 PM"));
            cachePartition.Put("key2", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:01 PM"));
            cachePartition.Put("key3", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:02 PM"));
            cachePartition.Put("key4", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:03 PM"));
            cachePartition.Put("key5", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:04 PM"));
            cachePartition.Put("key6", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:05 PM"));
            cachePartition.Put("key7", ByteString.CopyFromUtf8("value"), DateTime.Parse("3:06 PM"));

            var keysToEvict = cachePartition.EvictionPolicy.GetItems(
                cachePartition,
                count: 7,
                DateTime.Parse("3:07 PM"));
            Assert.Equal(0, keysToEvict.Length);
        }

        [Fact]
        public void Evict()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

                KeyEvictionSamples = 10
            };


            using (var cache = CreatePartition(conf))
            {
                var key = new CacheKey(new byte[] { 11 });
                var value = new CacheValue(new byte[] { 22 }, default);
                cache.Put(key, value, default);

                var evictStatus = cache.EvictKeyWithNoLock(key, default);

                Assert.True(evictStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(1, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(0, cache.Stats.CurrentItemCount);
            }
        }

        [Fact]
        public void Evict_Multiple()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

                KeyEvictionSamples = 10
            };

            using (var cache = CreatePartition(conf))
            {
                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = new CacheValue(new byte[] { 22 }, default);
                cache.Put(key1, value1, default);

                var key2 = new CacheKey(new byte[] { 33 });
                var value2 = new CacheValue(new byte[] { 44 }, default);
                cache.Put(key2, value2, default);

                var evictStatus = cache.EvictKeyWithNoLock(key1, default);

                Assert.True(evictStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(1, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.Global.EvictedTimeMs);
            }
        }

        [Fact]
        public void Evict_LRU()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,
                KeyEvictionSamples = 10
            };

            using (var cache = CreatePartition(conf))
            {
                DateTime accessTime = DateTime.Parse("2015-12-11 12:00");
                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = new CacheValue(new byte[] { 11 }, accessTime.Ticks);
                cache.Put(key1, value1, accessTime);

                accessTime = DateTime.Parse("2015-12-11 12:01");
                var key2 = new CacheKey(new byte[] { 2 });
                var value2 = new CacheValue(new byte[] { 22 }, accessTime.Ticks);
                cache.Put(key2, value2, accessTime);

                accessTime = DateTime.Parse("2015-12-11 12:02");
                var key3 = new CacheKey(new byte[] { 33 });
                var value3 = new CacheValue(new byte[] { 33 }, accessTime.Ticks);
                cache.Put(key3, value3, accessTime);

                var evictions = cache.EvictKeyWithNoLock(key1, accessTime);
                Assert.True(evictions);

                Assert.Null(cache.Get(key1, accessTime, true, true));
                Assert.Equal(value2.Value, cache.Get(key2, accessTime, true, true).Value);
                Assert.Equal(value3.Value, cache.Get(key3, accessTime, true, true).Value);

                Assert.Equal(1, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(TimeSpan.FromMinutes(2).TotalMilliseconds, cache.Stats.Global.EvictedTimeMs);
            }
        }

        [Fact]
        public void Evict_WithUpdate_LRU()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

                KeyEvictionSamples = 10
            };

            using (var cache = CreatePartition(conf))
            {
                DateTime accessTime = DateTime.Parse("2015-12-11 12:00");
                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = new CacheValue(new byte[] { 11 }, accessTime.Ticks);
                cache.Put(key1, value1, accessTime);

                accessTime = DateTime.Parse("2015-12-11 12:01");
                var key2 = new CacheKey(new byte[] { 2 });
                var value2 = new CacheValue(new byte[] { 22 }, accessTime.Ticks);
                cache.Put(key2, value2, accessTime);


                accessTime = DateTime.Parse("2015-12-11 12:02");
                var key3 = new CacheKey(new byte[] { 33 });
                var value3 = new CacheValue(new byte[] { 33 }, accessTime.Ticks);
                cache.Put(key3, value3, accessTime);

                var evictions = cache.EvictKeyWithNoLock(key1, accessTime);
                Assert.True(evictions);

                Assert.Null(cache.Get(key1, accessTime, true, true));
                Assert.Equal(value2.Value, cache.Get(key2, accessTime, true, true).Value);
                Assert.Equal(value3.Value, cache.Get(key3, accessTime, true, true).Value);

                Assert.Equal(1, cache.Stats.TotalEvictionCount);
                Assert.Equal(0, cache.Stats.TotalExpiredCount);
                Assert.Equal(2, cache.Stats.CurrentItemCount);
                Assert.Equal(TimeSpan.FromMinutes(2).TotalMilliseconds, cache.Stats.Global.EvictedTimeMs);
            }
        }

        [Fact]
        public void Evict_Expired()
        {
            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 2048,
                MaxBytesPerKey = 2048,

                KeyEvictionSamples = 10
            };

            var accessTime = DateTime.Parse("2015-12-01 3:00 PM");
            using (var cache = CreatePartition(conf))
            {
                cache.Metadata.SetExpiration(TimeSpan.FromMinutes(1).Ticks, false);

                var key1 = new CacheKey(new byte[] { 11 });
                var value1 = new CacheValue(new byte[] { 22 }, accessTime.Ticks);
                cache.Put(key1, value1, default);

                var key2 = new CacheKey(new byte[] { 33 });
                var value2 = new CacheValue(new byte[] { 44 }, accessTime.AddMinutes(-2).Ticks);
                cache.Put(key2, value2, default);

                var evictStatus = cache.EvictKeyWithNoLock(key2, accessTime);

                Assert.True(evictStatus);
                Assert.Equal(0, cache.Stats.TotalHits);
                Assert.Equal(0, cache.Stats.TotalMisses);
                Assert.Equal(0, cache.Stats.TotalEvictionCount);
                Assert.Equal(1, cache.Stats.TotalExpiredCount);
                Assert.Equal(1, cache.Stats.CurrentItemCount);
                Assert.Equal(0, cache.Stats.Global.EvictedTimeMs);
            }
        }

        [Fact]
        public async Task Put_Evict_ReservedSize()
        {
            var opt = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage()))
            {
                var createDate = DateTime.Parse("16:00:00");
                var partitionPutStatus = await cache.PutPartition("p",
                    new PartitionMetadata(
                        createDate.Ticks,
                        0,
                        false,
                        false,
                        EvictionPolicy.Lru,
                        2),
                    createDate);

                Assert.Equal(WriteResult.Success, partitionPutStatus);

                var putStatus = cache.Put("p", "k1", ByteString.CopyFrom(new byte[] { 2 }), createDate);

                // item count is less than eviction sample size

                // partition is empty, writing a single value exceeds threshold and we evict right away
                // should we just return insufficient here instead??? don't know. seems like an unlikely use case
                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(1, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(1, cache.Stats.TotalItems);
                Assert.Equal(1, cache.Stats.TotalCacheSize);
                Assert.Equal(2, cache.Stats.TotalReservedCacheSize);
            }
        }

        [Fact]
        public async Task Put_Evict_ReservedSize2()
        {
            var opt = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage()))
            {
                var createDate = DateTime.Parse("16:00:00");

                var partitionPutStatus = await cache.PutPartition("p",
                    new PartitionMetadata(
                        createDate.Ticks,
                        0,
                        false,
                        false,
                        EvictionPolicy.Lru,
                        5),
                    createDate);

                Assert.Equal(WriteResult.Success, partitionPutStatus);

                var putStatus = cache.Put("p", "k1", ByteString.CopyFrom(new byte[] { 2 }), createDate);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(1, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(1, cache.Stats.TotalItems);
                Assert.Equal(4, cache.Stats.TotalCacheSize);
                Assert.Equal(5, cache.Stats.TotalReservedCacheSize);

                // putting this item kicks the other out
                // item count is less than eviction sample size
                var putStatus2 = cache.Put("p", "k2", ByteString.CopyFrom(new byte[] { 2 }), createDate);

                Assert.Equal(WriteResult.Success, putStatus2);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(1, cache.Stats.TotalEvictions);
                Assert.Equal(1, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(2, cache.Stats.TotalItems);
                Assert.Equal(4, cache.Stats.TotalCacheSize);
                Assert.Equal(5, cache.Stats.TotalReservedCacheSize);
            }
        }

        [Fact]
        public async Task Put_Evict_ReservedSize_Random()
        {
            var opt = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128,
                KeyEvictionSamples = 5
            };

            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage()))
            {
                var createDate = DateTime.Parse("16:00:00");

                for (int i = 0; i < 10; i++)
                {
                    createDate = createDate.AddTicks(i);
                    var partitionKey = i.ToString("00");
                    var partitionPutStatus = await cache.PutPartition(partitionKey,
                       new PartitionMetadata(
                           createDate.Ticks,
                           0,
                           false,
                           false,
                           EvictionPolicy.Lru,
                           2002),
                       createDate);

                    Assert.Equal(WriteResult.Success, partitionPutStatus);

                    for (int j = 0; j < 100; j++)
                    {
                        var putStatus = cache.Put(partitionKey,
                            j.ToString("0000"),
                            ByteString.CopyFrom(Guid.NewGuid().ToByteArray()), // 20-byte total entry
                            createDate.AddTicks(i));

                        Assert.Equal(WriteResult.Success, putStatus);
                        Assert.Equal(0, cache.Stats.TotalEvictions);
                    }
                }

                // item count is greater than eviction sample size, takes a random sample

                var putStatus2 = cache.Put("07",
                    "0101",
                    ByteString.CopyFrom(Helpers.BuildRandomByteArray(36)), // 40-byte total entry
                    createDate.AddTicks(100));
                Assert.Equal(WriteResult.Success, putStatus2);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(10, cache.Stats.Partitions);
                // putting a single larger item results in 2 items needing evicted to make room.
                Assert.Equal(2, cache.Stats.TotalEvictions);
                Assert.Equal(999, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(1001, cache.Stats.TotalItems);
                Assert.Equal(20020, cache.Stats.TotalCacheSize);
                Assert.Equal(20020, cache.Stats.TotalReservedCacheSize);
                var partition = cache.GetPartition("07").Value;
                Assert.Equal(2, partition.Stats.TotalEvictionCount);
                Assert.Equal(99, partition.Stats.CurrentItemCount);
                Assert.Equal(0, partition.Stats.TotalMisses);
                Assert.Equal(2002, partition.Stats.TotalCacheSize);
            }
        }

        [Fact]
        public async Task Put_Evict_Unbounded_Random()
        {
            var opt = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128,
                KeyEvictionSamples = 5,
                MaxCacheSize = 20020
            };

            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage()))
            {
                var createDate = DateTime.Parse("16:00:00");

                for (int i = 0; i < 10; i++)
                {
                    createDate = createDate.AddTicks(i);
                    var partitionKey = i.ToString("00");
                    var partitionPutStatus = await cache.PutPartition(partitionKey,
                       new PartitionMetadata(
                           createDate.Ticks,
                           0,
                           false,
                           false,
                           EvictionPolicy.Lru,
                           0),
                       createDate);

                    Assert.Equal(WriteResult.Success, partitionPutStatus);

                    for (int j = 0; j < 100; j++)
                    {
                        var putStatus = cache.Put(partitionKey,
                            j.ToString("0000"),
                            ByteString.CopyFrom(Guid.NewGuid().ToByteArray()), // 20-byte total entry
                            createDate.AddTicks(i));

                        Assert.Equal(WriteResult.Success, putStatus);
                        Assert.Equal(0, cache.Stats.TotalEvictions);
                    }
                }

                // item count is greater than eviction sample size, takes a random sample.
                // partitions are unbounded, grabs a "random" partition 
                // from the global pool of unbounded partitions to evict from
                var putStatus2 = cache.Put("08",
                    "0101",
                    ByteString.CopyFrom(Helpers.BuildRandomByteArray(36)), // 40-byte total entry
                    createDate.AddTicks(100));
                Assert.Equal(WriteResult.Success, putStatus2);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(10, cache.Stats.Partitions);
                // putting a single item results in 2 items needing evicted to make room.
                Assert.Equal(2, cache.Stats.TotalEvictions);
                Assert.Equal(999, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(1001, cache.Stats.TotalItems);
                Assert.Equal(20020, cache.Stats.TotalCacheSize);
                Assert.Equal(0, cache.Stats.TotalReservedCacheSize);
            }
        }

        public MemoryStorePartition CreatePartition(
            CacheServerOptions options,
            EvictionPolicy evictionPolicy = EvictionPolicy.Lru,
            TimeSpan expiration = default,
            long size = 0)
        {
            _mockClock
               .SetupGet(x => x.UtcNow)
               .Returns(DateTime.Parse("3:00 PM"));

            var meta = new PartitionMetadata(
                _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                expiration.Ticks,
                false,
                false,
                evictionPolicy,
                size);
            return new MemoryStorePartition(
                options,
                new PartitionStatistics(meta, new RuntimeStatistics(options, _mockClock.Object), _mockClock.Object.UtcNow.UtcDateTime),
                meta,
                EvictionPolicyFactory.GetEvictionPolicy(evictionPolicy));
        }
    }
}

