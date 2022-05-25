using System;
using System.IO;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Persistence;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Tests.Util;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    [Collection("File system collection")]
    public class MemoryStoreStorageTests
    {
        private readonly Mock<ILogger<MemoryStore>> _mockLogger;
        private readonly FileSystemFixture _fixture;
        private readonly Mock<ISystemClock> _mockClock;

        public MemoryStoreStorageTests(FileSystemFixture fixture)
        {
            _mockClock = new Mock<ISystemClock>();
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 4:11 PM"));
            _fixture = fixture;
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
        public async Task NoStorage_CreatePartition_Implicit()
        {
            var opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            var stats = new RuntimeStatistics(opt, _mockClock.Object);
            var storage = new FileSystemPartitionStorage(opt);
            var partitionKey = Guid.NewGuid().ToString();
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {

                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, _mockClock.Object.UtcNow.UtcDateTime.Ticks);

                var putStatus = cache.Put(partitionKey, itemKey, itemValue, _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, putStatus);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(1, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(1, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.False(partition.Value.Metadata.IsPersisted);
                Assert.False(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(0, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);

                Assert.False(File.Exists(opt.DataLocation + "/" + partitionKey));
            }

            opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            stats = new RuntimeStatistics(opt, _mockClock.Object);
            storage = new FileSystemPartitionStorage(opt);
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {
                await foreach (var p in storage.RecoverMetaData())
                {
                    await cache.PutPartition(
                            p.Key,
                            p.Value,
                            new DateTime(p.Value.CreateDate, DateTimeKind.Utc));
                }
                Assert.Equal(0, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.Null(partition.Value);
            }
        }

        [Fact]
        public async Task Storage_CreatePartition_Explicit()
        {
            var opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            var stats = new RuntimeStatistics(opt, _mockClock.Object);
            var storage = new FileSystemPartitionStorage(opt);
            var partitionKey = Guid.NewGuid().ToString();
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {
                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, _mockClock.Object.UtcNow.UtcDateTime.Ticks);
                var status = await cache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                        TimeSpan.FromMinutes(1).Ticks,
                        true,
                        true,
                        EvictionPolicy.Lru,
                        0), _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, status);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.True(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(TimeSpan.FromMinutes(1).Ticks, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);

                Assert.True(File.Exists(opt.DataLocation + "/" + partitionKey));
            }

            opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            stats = new RuntimeStatistics(opt, _mockClock.Object);
            storage = new FileSystemPartitionStorage(opt);
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {
                await foreach (var p in storage.RecoverMetaData())
                {
                    await cache.PutPartition(
                            p.Key,
                            p.Value,
                            new DateTime(p.Value.CreateDate, DateTimeKind.Utc));
                }
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.True(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(TimeSpan.FromMinutes(1).Ticks, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);
                cache.DeletePartition(partitionKey);
            }
        }

        [Fact]
        public async Task NoStorage_CreatePartition_Explicit()
        {
            var opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            var stats = new RuntimeStatistics(opt, _mockClock.Object);
            var storage = new FileSystemPartitionStorage(opt);
            var partitionKey = Guid.NewGuid().ToString();
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {

                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, _mockClock.Object.UtcNow.UtcDateTime.Ticks);
                var status = await cache.PutPartition(partitionKey,
                   new PartitionMetadata(
                        _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                        TimeSpan.FromMinutes(1).Ticks,
                        true,
                        false,
                        EvictionPolicy.Lru,
                        0), _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, status);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.False(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(TimeSpan.FromMinutes(1).Ticks, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);

                Assert.False(File.Exists(opt.DataLocation + "/" + partitionKey));
            }

            opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            stats = new RuntimeStatistics(opt, _mockClock.Object);
            storage = new FileSystemPartitionStorage(opt);
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {
                await foreach (var p in storage.RecoverMetaData())
                {
                    await cache.PutPartition(
                            p.Key,
                            p.Value,
                            new DateTime(p.Value.CreateDate, DateTimeKind.Utc));
                }
                Assert.Equal(0, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.Null(partition.Value);
            }
        }

        [Fact]
        public async Task Storage_DeletePartition()
        {
            var opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            var stats = new RuntimeStatistics(opt, _mockClock.Object);
            var storage = new FileSystemPartitionStorage(opt);
            var partitionKey = Guid.NewGuid().ToString();
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {

                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, _mockClock.Object.UtcNow.UtcDateTime.Ticks);
                var status = await cache.PutPartition(partitionKey,
                     new PartitionMetadata(
                        _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                        TimeSpan.FromMinutes(1).Ticks,
                        true,
                        true,
                        EvictionPolicy.Lru,
                        0), _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, status);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                Assert.True(File.Exists(opt.DataLocation + "/" + partitionKey));

                cache.DeletePartition(partitionKey);
                Assert.False(File.Exists(opt.DataLocation + "/" + partitionKey));
            }
        }

        [Fact]
        public async Task Storage_FlushPartition()
        {
            var opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            var stats = new RuntimeStatistics(opt, _mockClock.Object);
            var storage = new FileSystemPartitionStorage(opt);
            var partitionKey = Guid.NewGuid().ToString();
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {

                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, _mockClock.Object.UtcNow.UtcDateTime.Ticks);
                var status = await cache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                        TimeSpan.FromMinutes(1).Ticks,
                        true,
                        true,
                        EvictionPolicy.Lru,
                        0), _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, status);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                Assert.True(File.Exists(opt.DataLocation + "/" + partitionKey));

                cache.FlushPartition(partitionKey);
                Assert.True(File.Exists(opt.DataLocation + "/" + partitionKey));
            }

            opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            stats = new RuntimeStatistics(opt, _mockClock.Object);
            storage = new FileSystemPartitionStorage(opt);
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {
                await foreach (var p in storage.RecoverMetaData())
                {
                    await cache.PutPartition(
                            p.Key,
                            p.Value,
                            new DateTime(p.Value.CreateDate, DateTimeKind.Utc));
                }
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(TimeSpan.FromMinutes(1).Ticks, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);

                cache.DeletePartition(partitionKey);
            }
        }

        [Fact]
        public async Task StorageSwap_CreatePartition_Explicit()
        {
            var opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            var stats = new RuntimeStatistics(opt, _mockClock.Object);
            var storage = new FileSystemPartitionStorage(opt);
            var partitionKey = Guid.NewGuid().ToString();
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {

                var itemKey = new byte[] { 20 };
                var itemValue = new CacheValue(new byte[] { 2 }, _mockClock.Object.UtcNow.UtcDateTime.Ticks);
                // first persist it
                var status = await cache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                        TimeSpan.FromMinutes(1).Ticks,
                        true,
                        true,
                        EvictionPolicy.Lru,
                        0), _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, status);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.True(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(TimeSpan.FromMinutes(1).Ticks, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);

                Assert.True(File.Exists(opt.DataLocation + "/" + partitionKey));

                // then un-persist it
                status = await cache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                        TimeSpan.FromMinutes(1).Ticks,
                        true,
                        false,
                        EvictionPolicy.Lru,
                        0), _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, status);
                partition = cache.GetPartition(partitionKey);
                Assert.False(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(TimeSpan.FromMinutes(1).Ticks, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);

                Assert.False(File.Exists(opt.DataLocation + "/" + partitionKey));
            }

            opt = new CacheServerOptions
            {
                DataLocation = _fixture.FilePath
            };
            stats = new RuntimeStatistics(opt, _mockClock.Object);
            storage = new FileSystemPartitionStorage(opt);
            using (var cache = new MemoryStore(_mockLogger.Object, opt, stats, storage))
            {
                await foreach (var p in storage.RecoverMetaData())
                {
                    await cache.PutPartition(
                            p.Key,
                            p.Value,
                            new DateTime(p.Value.CreateDate, DateTimeKind.Utc));
                }
                Assert.Equal(0, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                var partition = cache.GetPartition(partitionKey);
                Assert.Null(partition.Value);

                // then un-persist it again
                var status = await cache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                        TimeSpan.FromMinutes(1).Ticks,
                        true,
                        false,
                        EvictionPolicy.Lru,
                        0), _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, status);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                partition = cache.GetPartition(partitionKey);
                Assert.False(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(TimeSpan.FromMinutes(1).Ticks, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);

                Assert.False(File.Exists(opt.DataLocation + "/" + partitionKey));

                // and finally persist it... again
                status = await cache.PutPartition(partitionKey,
                    new PartitionMetadata(
                        _mockClock.Object.UtcNow.UtcDateTime.Ticks,
                        TimeSpan.FromMinutes(1).Ticks,
                        true,
                        true,
                        EvictionPolicy.Lru,
                        0), _mockClock.Object.UtcNow.UtcDateTime);

                Assert.Equal(WriteResult.Success, status);
                Assert.Equal(0, cache.Stats.Hits);
                Assert.Equal(1, cache.Stats.Partitions);
                Assert.Equal(0, cache.Stats.TotalEvictions);
                Assert.Equal(0, cache.Stats.CurrentItems);
                Assert.Equal(0, cache.Stats.Misses);
                Assert.Equal(0, cache.Stats.TotalItems);

                partition = cache.GetPartition(partitionKey);
                Assert.True(partition.Value.Metadata.IsPersisted);
                Assert.True(partition.Value.Metadata.IsAbsoluteExpiration);
                Assert.Equal(TimeSpan.FromMinutes(1).Ticks, partition.Value.Metadata.ExpirationTicks);
                Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.Ticks, partition.Value.Metadata.CreateDate);

                Assert.True(File.Exists(opt.DataLocation + "/" + partitionKey));


                cache.DeletePartition(partitionKey);
                Assert.False(File.Exists(opt.DataLocation + "/" + partitionKey));
            }
        }
    }
}
