using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Tests.Util;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class MemoryStoreTests
    {
        private readonly Mock<ILogger<MemoryStore>> _mockLogger;
        private readonly Mock<ISystemClock> _mockClock;

        public MemoryStoreTests()
        {
            _mockClock = new Mock<ISystemClock>();
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 4:11 PM"));
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
        public void Put()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var itemKey = new byte[] { 20 };
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var itemValue = new CacheValue(new byte[] { 2 }, createDate.Ticks);

                var putStatus = partitionedCache.Put(partitionKey, itemKey, itemValue, createDate);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.Misses);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Put_NullItemKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var itemValue = new CacheValue(new byte[] { 2 }, createDate.Ticks);
                var putStatus = partitionedCache.Put(partitionKey, new CacheKey(), itemValue, createDate);

                Assert.Equal(WriteResult.MissingKey, putStatus);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.Misses);
                Assert.Equal(0, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Put_NullItemValue()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var itemKey = new byte[] { 20 };
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var putStatus = partitionedCache.Put(partitionKey, itemKey, null, createDate);

                Assert.Equal(WriteResult.MissingValue, putStatus);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.Misses);
                Assert.Equal(0, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Put_ImplicitPartition_KeyTooLarge()
        {
            var opt = new CacheServerOptions { MaxBytesPerKey = 5 };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "abcdef";
                var itemKey = new byte[] { 20 };
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var itemValue = new CacheValue(new byte[] { 2 }, createDate.Ticks);

                var putStatus = partitionedCache.Put(partitionKey, itemKey, itemValue, createDate);

                Assert.Equal(WriteResult.KeyTooLarge, putStatus);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(0, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.Misses);
                Assert.Equal(0, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Put_ImplicitPartition_MissingPartitionKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var itemKey = new byte[] { 20 };
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var itemValue = new CacheValue(new byte[] { 2 }, createDate.Ticks);

                var putStatus = partitionedCache.Put(new CacheKey(), itemKey, itemValue, createDate);

                Assert.Equal(WriteResult.MissingPartitionKey, putStatus);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(0, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.Misses);
                Assert.Equal(0, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Put_ImplicitPartition_NotInvalidPartitionKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };
            using (var cache = new MemoryStore(_mockLogger.Object, new CacheServerOptions(), new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var itemKey = new byte[] { 20 };
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var itemValue = new CacheValue(new byte[] { 2 }, createDate.Ticks);

                var putStatus = cache.Put("abc/123", itemKey, itemValue, createDate);

                Assert.Equal(WriteResult.InvalidPartitionKey, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
            }
        }

        [Fact]
        public async Task PutPartition_MissingPartitionKey()
        {
            using (var cache = new MemoryStore(_mockLogger.Object, new CacheServerOptions(), new RuntimeStatistics(new CacheServerOptions(), _mockClock.Object), new StubMemoryStorage()))
            {
                var createDate = DateTime.Parse("2015-09-22 16:00:00");

                var putStatus = await cache.PutPartition("",
                    new PartitionMetadata(createDate.Ticks, 0, false, false, default, 0), createDate);

                Assert.Equal(WriteResult.MissingPartitionKey, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
            }
        }

        [Fact]
        public async Task PutPartition_InvalidPartitionKey()
        {
            using (var cache = new MemoryStore(_mockLogger.Object, new CacheServerOptions(), new RuntimeStatistics(new CacheServerOptions(), _mockClock.Object), new StubMemoryStorage()))
            {
                var createDate = DateTime.Parse("2015-09-22 16:00:00");

                var putStatus = await cache.PutPartition("abc/123",
                    new PartitionMetadata(createDate.Ticks, 0, false, false, default, 0), createDate);

                Assert.Equal(WriteResult.InvalidPartitionKey, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
            }
        }

        [Fact]
        public async Task PutPartition_PartitionKeyTooLarge()
        {
            using (var cache = new MemoryStore(_mockLogger.Object, new CacheServerOptions { MaxBytesPerKey = 5 }, new RuntimeStatistics(new CacheServerOptions(), _mockClock.Object), new StubMemoryStorage()))
            {
                var createDate = DateTime.Parse("2015-09-22 16:00:00");

                var putStatus = await cache.PutPartition("abcdef",
                    new PartitionMetadata(createDate.Ticks, 0, false, false, default, 0), createDate);

                Assert.Equal(WriteResult.KeyTooLarge, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
            }
        }

        [Fact]
        public async Task PutPartition()
        {
            var storage = new MockMemoryStorage();
            using (var cache = new MemoryStore(_mockLogger.Object, new CacheServerOptions(), new RuntimeStatistics(new CacheServerOptions(), _mockClock.Object), storage))
            {
                var createDate = DateTime.Parse("16:00:00");

                var putStatus = await cache.PutPartition("abc",
                    new PartitionMetadata(createDate.Ticks, 0, false, false, default, 0),
                    createDate);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
                Assert.Equal(0, cache.Stats.TotalReservedCacheSize);

                var partition = cache.GetPartition("abc");
                Assert.Equal("abc", partition.Key);
                Assert.False(partition.Value.Metadata.IsPersisted);
                Assert.False(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(0, partition.Value.Metadata.ExpirationTicks);
                Assert.True(partition.Value.Metadata.IsUnboundedCache);
                Assert.Equal(EvictionPolicy.Lru, partition.Value.Metadata.EvictionPolicy);
                Assert.Equal(0, partition.Value.Metadata.MaxCacheSize);
                Assert.Equal(createDate.Ticks, partition.Value.Metadata.CreateDate);

                Assert.Empty((await storage.RecoverMetaData().ToArrayAsync()));
            }
        }

        [Fact]
        public async Task PutPartition_Update()
        {
            var storage = new MockMemoryStorage();
            var opt = new CacheServerOptions
            {
                MaxCacheSize = 1024
            };
            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), storage))
            {
                var createDate = DateTime.Parse("16:00:00");

                var putStatus = await cache.PutPartition("abc",
                    new PartitionMetadata(createDate.Ticks, 0, false, true, default, 0),
                    createDate);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
                Assert.Equal(0, cache.Stats.TotalReservedCacheSize);

                var partition = cache.GetPartition("abc");
                Assert.Equal("abc", partition.Key);
                Assert.True(partition.Value.Metadata.IsPersisted);
                Assert.False(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(0, partition.Value.Metadata.ExpirationTicks);
                Assert.True(partition.Value.Metadata.IsUnboundedCache);
                Assert.Equal(EvictionPolicy.Lru, partition.Value.Metadata.EvictionPolicy);
                Assert.Equal(0, partition.Value.Metadata.MaxCacheSize);
                Assert.Equal(createDate.Ticks, partition.Value.Metadata.CreateDate);

                Assert.NotEmpty((await storage.RecoverMetaData().ToArrayAsync()));

                var createDate2 = DateTime.Parse("16:01:00");

                putStatus = await cache.PutPartition("abc",
                    new PartitionMetadata(createDate2.Ticks, 5, true, false, EvictionPolicy.NoEviction, 100),
                    createDate);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
                Assert.Equal(100, cache.Stats.TotalReservedCacheSize);


                partition = cache.GetPartition("abc");
                Assert.Equal("abc", partition.Key);
                Assert.False(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(5, partition.Value.Metadata.ExpirationTicks);
                Assert.False(partition.Value.Metadata.IsUnboundedCache);
                Assert.Equal(EvictionPolicy.NoEviction, partition.Value.Metadata.EvictionPolicy);
                Assert.Equal(100, partition.Value.Metadata.MaxCacheSize);
                Assert.Equal(createDate.Ticks, partition.Value.Metadata.CreateDate);

                Assert.Empty((await storage.RecoverMetaData().ToArrayAsync()));
            }
        }

        [Fact]
        public async Task PutPartition_InsufficientCapacity()
        {
            var storage = new MockMemoryStorage();
            var opt = new CacheServerOptions
            {
                MaxCacheSize = 1024
            };
            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), storage))
            {
                var createDate = DateTime.Parse("16:00:00");

                var putStatus = await cache.PutPartition("abc",
                    new PartitionMetadata(createDate.Ticks, 0, false, false, default, 1025),
                    createDate);

                Assert.Equal(WriteResult.InsufficientCapacity, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
                Assert.Equal(0, cache.Stats.TotalReservedCacheSize);
                Assert.Empty((await storage.RecoverMetaData().ToArrayAsync()));

                var partition = cache.GetPartition("abc");
                Assert.Equal("abc", partition.Key);
                Assert.Null(partition.Value);
            }
        }

        [Fact]
        public async Task PutPartition_Update_InsufficientCapacity()
        {
            var storage = new MockMemoryStorage();
            var opt = new CacheServerOptions
            {
                MaxCacheSize = 1024
            };
            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), storage))
            {
                var createDate = DateTime.Parse("16:00:00");

                var putStatus = await cache.PutPartition("abc",
                    new PartitionMetadata(createDate.Ticks, 5, true, true, EvictionPolicy.NoEviction, 1),
                    createDate);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
                Assert.Equal(1, cache.Stats.TotalReservedCacheSize);

                var partition = cache.GetPartition("abc");
                Assert.Equal("abc", partition.Key);
                Assert.True(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(5, partition.Value.Metadata.ExpirationTicks);
                Assert.False(partition.Value.Metadata.IsUnboundedCache);
                Assert.Equal(EvictionPolicy.NoEviction, partition.Value.Metadata.EvictionPolicy);
                Assert.Equal(1, partition.Value.Metadata.MaxCacheSize);
                Assert.Equal(createDate.Ticks, partition.Value.Metadata.CreateDate);

                Assert.NotEmpty((await storage.RecoverMetaData().ToArrayAsync()));

                var createDate2 = DateTime.Parse("16:01:00");

                putStatus = await cache.PutPartition("abc",
                    new PartitionMetadata(createDate2.Ticks, 5, true, false, EvictionPolicy.NoEviction, 1024),
                    createDate);

                Assert.Equal(WriteResult.InsufficientCapacity, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);
                Assert.Equal(1, cache.Stats.TotalReservedCacheSize);

                partition = cache.GetPartition("abc");
                Assert.Equal("abc", partition.Key);
                Assert.True(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(5, partition.Value.Metadata.ExpirationTicks);
                Assert.False(partition.Value.Metadata.IsUnboundedCache);
                Assert.Equal(EvictionPolicy.NoEviction, partition.Value.Metadata.EvictionPolicy);
                Assert.Equal(1, partition.Value.Metadata.MaxCacheSize);
                Assert.Equal(createDate.Ticks, partition.Value.Metadata.CreateDate);

                Assert.NotEmpty((await storage.RecoverMetaData().ToArrayAsync()));
            }
        }

        [Fact]
        public void PutMany()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var items = new Dictionary<string, ByteString>
                {
                    { "20", new CacheValue(new byte[] { 2 }, createDate.Ticks) },
                    { "30", new CacheValue(new byte[] { 3 }, createDate.Ticks) }
                };

                partitionedCache.Put(partitionKey, items, createDate);

                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.Misses);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Get()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };
            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var itemKey = new byte[] { 20 };
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var itemValue = new CacheValue(new byte[] { 2 }, createDate.Ticks);
                partitionedCache.Put(partitionKey, itemKey, itemValue, default);

                var item = partitionedCache.Get(partitionKey, itemKey, DateTime.Parse("2015-09-22 16:00:00"));
                Assert.Equal(itemValue, item);
                Assert.Equal(1, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.Misses);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public async Task Get_Expired()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, default);
                Assert.Equal(WriteResult.Success, await partitionedCache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        default,
                        TimeSpan.FromMinutes(5).Ticks,
                        false,
                        false,
                        default,
                        0), default));
                partitionedCache.Put(partitionKey, itemKey, itemValue, default);

                var item = partitionedCache.Get(partitionKey, itemKey, DateTime.Parse("2015-09-22 16:10:00"));
                Assert.Null(item);

                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.TotalExpirations);
                Assert.Equal(3, partitionedCache.Stats.TotalCacheSize);
                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Misses);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public async Task Get_NullPartitionKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, default);
                await partitionedCache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        default,
                        TimeSpan.FromMinutes(5).Ticks,
                        false,
                        false,
                        default,
                        1024), default);
                partitionedCache.Put(partitionKey, itemKey, itemValue, default);

                var item = partitionedCache.Get(new CacheKey(), itemKey, DateTime.Parse("2015-09-22 16:10:00"));
                Assert.Null(item);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Misses);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public async Task Get_NullItemKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, default);
                await partitionedCache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        default,
                        TimeSpan.FromMinutes(5).Ticks,
                        false,
                        false,
                        default,
                        1024), default);
                partitionedCache.Put(partitionKey, itemKey, itemValue, default);

                var item = partitionedCache.Get(partitionKey, (byte[])null, DateTime.Parse("2015-09-22 16:10:00"));
                Assert.Null(item);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Misses);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Get_MissingPartitionKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey1 = "200";
                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, DateTime.Parse("2015-09-22 16:00:00").Ticks);
                partitionedCache.Put(partitionKey1, itemKey, itemValue, default);

                var partitionKey2 = "250";
                var item = partitionedCache.Get(partitionKey2, itemKey, DateTime.Parse("2015-09-22 16:10:00"));
                Assert.Null(item);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Misses);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Get_MissingItemKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var itemKey1 = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, DateTime.Parse("2015-09-22 16:00:00").Ticks);
                partitionedCache.Put(partitionKey, itemKey1, itemValue, default);

                var itemKey2 = new byte[] { 25 };
                var item = partitionedCache.Get(partitionKey, itemKey2, DateTime.Parse("2015-09-22 16:10:00"));
                Assert.Null(item);
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Misses);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void GetMany()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var items = new Dictionary<string, ByteString>
                {
                    { "20", new CacheValue(new byte[] { 2 }, createDate.Ticks) },
                    { "30", new CacheValue(new byte[] { 3 }, createDate.Ticks) }
                };

                partitionedCache.Put(partitionKey, items, default);

                using var dataBlock = partitionedCache.Get(partitionKey, items.Keys.ToList(), DateTime.Parse("2015-09-22 16:00:00"));
                Assert.Equal(2, dataBlock.Data.Length);
                Assert.Equal(items.First().Value, dataBlock.Data[0].Value);
                Assert.Equal(items.Last().Value, dataBlock.Data[1].Value);
                Assert.Equal(2, partitionedCache.Stats.Hits);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.Misses);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void GetMany_NullPartition()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var keys = new List<string>(2);

                keys.Add("20");
                keys.Add("30");

                using var dataBlock = partitionedCache.Get(new CacheKey(), keys, DateTime.Parse("2015-09-22 16:00:00"));
                Assert.Empty(dataBlock.Data.ToArray());
                Assert.Equal(0, partitionedCache.Stats.Hits);
                Assert.Equal(2, partitionedCache.Stats.Misses);
                Assert.Equal(0, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(0, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Get_Count_MultiPartition()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey1 = "100";
                var itemKey1 = new byte[] { 10 };
                var itemValue1 = new CacheValue(new byte[] { 1 }, default);
                partitionedCache.Put(partitionKey1, itemKey1, itemValue1, default);

                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);

                var partitionKey2 = "200";
                var itemKey2 = new byte[] { 20 };
                var itemValue2 = new CacheValue(new byte[] { 2 }, default);
                partitionedCache.Put(partitionKey2, itemKey2, itemValue2, default);

                Assert.Equal(2, partitionedCache.Stats.CurrentItems);
                Assert.Equal(2, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);

                var deleteResult1 = partitionedCache.Delete(partitionKey1, itemKey1, default);

                Assert.Equal(WriteResult.Success, deleteResult1);
                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(2, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);

                var deleteResult2 = partitionedCache.Delete(partitionKey2, itemKey2, default);

                Assert.Equal(WriteResult.Success, deleteResult2);
                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(2, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Get_Count_SinglePartition_DeleteMany()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey1 = "100";
                var itemKey1 = "10";
                var itemValue1 = new CacheValue(new byte[] { 1 }, default);

                partitionedCache.Put(partitionKey1, itemKey1, itemValue1, default);

                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);

                var itemKey2 = "20";
                var itemValue2 = new CacheValue(new byte[] { 2 }, default);
                partitionedCache.Put(partitionKey1, itemKey2, itemValue2, default);

                Assert.Equal(2, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);

                var data = new List<string>(2);
                data.Add(itemKey1);
                data.Add(itemKey2);
                partitionedCache.Delete(partitionKey1, data, default);


                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Get_Count_MultiPartition_DeletePartition()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey1 = "100";
                var itemKey1 = new byte[] { 10 };
                var itemValue1 = new CacheValue(new byte[] { 1 }, default);
                partitionedCache.Put(partitionKey1, itemKey1, itemValue1, default);

                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);

                var partitionKey2 = "200";
                var itemKey2 = new byte[] { 20 };
                var itemValue2 = new CacheValue(new byte[] { 2 }, default);
                partitionedCache.Put(partitionKey2, itemKey2, itemValue2, default);

                Assert.Equal(2, partitionedCache.Stats.CurrentItems);
                Assert.Equal(2, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);

                var deleteResult1 = partitionedCache.DeletePartition(partitionKey1);

                Assert.Equal(WriteResult.Success, deleteResult1);
                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Delete()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var cache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var itemKey = new byte[] { 20 };
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var itemValue = new CacheValue(new byte[] { 2 }, createDate.Ticks);

                cache.Put(partitionKey, itemKey, itemValue, default);

                Assert.Equal(1, cache.Stats.CurrentItems);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(1, cache.Stats.TotalItems);

                var deleteResult = cache.Delete(partitionKey, itemKey, createDate);

                Assert.Equal(WriteResult.Success, deleteResult);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(1, cache.Stats.TotalItems);
            }
        }

        [Fact]
        public void DeleteMany()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var cache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "200";
                var createDate = DateTime.Parse("2015-09-22 16:00:00");
                var items = new Dictionary<string, ByteString>
                {
                    { "20", new CacheValue(new byte[] { 2 }, createDate.Ticks) },
                    { "30", new CacheValue(new byte[] { 3 }, createDate.Ticks) }
                };

                Assert.Equal(WriteResult.Success, cache.Put(partitionKey, items, createDate));

                Assert.Equal(2, cache.Delete(partitionKey, items.Keys.ToList(), createDate));

                using var dataBlock = cache.Get(partitionKey, items.Keys.ToList(), createDate);
                Assert.Empty(dataBlock.Data.ToArray());
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(2, cache.Stats.Misses);
                Assert.Equal(2, cache.Stats.TotalItems);
            }
        }

        [Fact]
        public void Touch()
        {
            var opt = new CacheServerOptions();
            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "p";
                var itemKey = "k1";
                var itemValue = ByteString.CopyFromUtf8("v1");

                Assert.Equal(WriteResult.Success, cache.Put(partitionKey, itemKey, itemValue, DateTime.Parse("16:00:00")));
                Assert.Equal(1, cache.Stats.CurrentItems);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(1, cache.Stats.TotalItems);

                Assert.Equal(WriteResult.Success, cache.Touch(partitionKey, itemKey, DateTime.Parse("16:01:00")));

                var value = cache.Get(partitionKey, itemKey, DateTime.Parse("16:01:00"));
                Assert.Equal(300000000, value.Ttl(DateTime.Parse("16:01:30").Ticks, TimeSpan.FromMinutes(1).Ticks));

                Assert.Equal(2, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.CurrentItems);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(1, cache.Stats.TotalItems);
            }
        }

        [Fact]
        public void TouchMany()
        {
            var opt = new CacheServerOptions();
            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey = "p";
                var items = new Dictionary<string, ByteString>
                {
                    { "k1", ByteString.CopyFromUtf8("v1") },
                    { "k2", ByteString.CopyFromUtf8("v2") }
                };

                Assert.Equal(WriteResult.Success, cache.Put(partitionKey, items, DateTime.Parse("16:00:00")));

                Assert.Equal(2, cache.Touch(partitionKey, items.Keys.ToList(), DateTime.Parse("16:01:00")));

                using var dataBlock = cache.Get(partitionKey, items.Keys.ToList(), DateTime.Parse("16:01:00"));
                Assert.Equal(2, dataBlock.Data.Length);
                Assert.Equal(300000000, dataBlock.Data[0].Value.Ttl(DateTime.Parse("16:01:30").Ticks, TimeSpan.FromMinutes(1).Ticks));
                Assert.Equal(300000000, dataBlock.Data[1].Value.Ttl(DateTime.Parse("16:01:30").Ticks, TimeSpan.FromMinutes(1).Ticks));
                Assert.Equal(4, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(2, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(2, cache.Stats.TotalItems);
            }
        }

        [Fact]
        public void DeletePartition_MissingPartitionKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var deleteResult1 = partitionedCache.DeletePartition(null);

                Assert.Equal(WriteResult.MissingPartitionKey, deleteResult1);
            }
        }

        [Fact]
        public void DeletePartition_EmptyPartitionKey()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var deleteResult1 = partitionedCache.DeletePartition("");

                Assert.Equal(WriteResult.MissingPartitionKey, deleteResult1);
            }
        }

        [Fact]
        public void FlushPartition()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey1 = "100";
                var itemKey1 = new byte[] { 10 };
                var itemValue1 = new CacheValue(new byte[] { 1 }, default);
                partitionedCache.Put(partitionKey1, itemKey1, itemValue1, default);

                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);

                var partitionKey2 = "200";
                var itemKey2 = new byte[] { 20 };
                var itemValue2 = new CacheValue(new byte[] { 2 }, default);
                partitionedCache.Put(partitionKey2, itemKey2, itemValue2, default);

                Assert.Equal(2, partitionedCache.Stats.CurrentItems);
                Assert.Equal(2, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);

                partitionedCache.FlushPartition(partitionKey1);

                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(2, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void FlushAll()
        {
            var partitionConf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var partitionedCache = new MemoryStore(_mockLogger.Object, partitionConf, new RuntimeStatistics(partitionConf, _mockClock.Object), new StubMemoryStorage()))
            {
                var partitionKey1 = "100";
                var itemKey1 = new byte[] { 10 };
                var itemValue1 = new CacheValue(new byte[] { 1 }, default);
                partitionedCache.Put(partitionKey1, itemKey1, itemValue1, default);

                Assert.Equal(1, partitionedCache.Stats.CurrentItems);
                Assert.Equal(1, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(1, partitionedCache.Stats.TotalItems);

                var partitionKey2 = "200";
                var itemKey2 = new byte[] { 20 };
                var itemValue2 = new CacheValue(new byte[] { 2 }, default);
                partitionedCache.Put(partitionKey2, itemKey2, itemValue2, default);

                Assert.Equal(2, partitionedCache.Stats.CurrentItems);
                Assert.Equal(2, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);

                partitionedCache.FlushAll();

                Assert.Equal(0, partitionedCache.Stats.CurrentItems);
                Assert.Equal(2, partitionedCache.Stats.Partitions);
                Assert.Equal(0, partitionedCache.Stats.TotalEvictions);
                Assert.Equal(2, partitionedCache.Stats.TotalItems);
            }
        }

        [Fact]
        public void TryIncrementValue()
        {
            var opt = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using (var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage()))
            {
                var itemKey = new byte[] { 20 };
                var createDate = DateTime.Parse("16:00:00");
                var status = cache.TryIncrementValue("p", itemKey, 1, createDate, out var result);

                Assert.Equal(WriteResult.Success, status);
                Assert.Equal(1, result);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(1, cache.Stats.CurrentItems);
                Assert.Equal(1, cache.Stats.Misses);
                Assert.Equal(1, cache.Stats.TotalItems);
            }
        }
    }
}
