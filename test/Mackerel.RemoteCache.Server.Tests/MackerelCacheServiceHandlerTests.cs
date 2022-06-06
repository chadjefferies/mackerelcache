using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core.Testing;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Rpc;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Tests.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using Quartz;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class MackerelCacheServiceHandlerTests
    {
        private readonly Mock<ILogger<MemoryStore>> _mockLogger;
        private readonly Mock<ISystemClock> _mockClock;
        private readonly Mock<IScheduler> _mockScheduler;
        private readonly Mock<ISchedulerFactory> _mockSchedulerFactory;
        private readonly Mock<IHostApplicationLifetime> _mockHostLifetime;

        public MackerelCacheServiceHandlerTests()
        {
            _mockClock = new Mock<ISystemClock>();
            _mockScheduler = new Mock<IScheduler>();
            _mockScheduler
                .Setup(x => x.GetJobDetail(It.IsAny<JobKey>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(() => { return null; });
            _mockScheduler
                .Setup(x => x.CheckExists(It.IsAny<TriggerKey>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(false);

            _mockSchedulerFactory = new Mock<ISchedulerFactory>();
            _mockSchedulerFactory
                .Setup(x => x.GetScheduler(It.IsAny<CancellationToken>()))
                    .ReturnsAsync(_mockScheduler.Object);
            _mockHostLifetime = new Mock<IHostApplicationLifetime>();

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
        public async Task Put()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);

            var partitionKey = "partition";
            var itemKey = "key20";
            var itemValue = ByteString.CopyFrom(new byte[] { 2 });

            var result = await server.Put(new PutRequest
            {
                PartitionKey = partitionKey,
                Key = itemKey,
                Value = itemValue
            }, null);

            Assert.Equal(WriteResult.Success, result.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task Get()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var request = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Get(request, null);

            Assert.Empty(results.Value.ToByteArray());
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(0, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(1, memoryCache.Stats.Misses);
            Assert.Equal(0, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task Put_Get()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putRequest = new PutRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putResults = await server.Put(putRequest, null);

            Assert.Equal(WriteResult.Success, putResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var getRequest = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Get(getRequest, null);

            Assert.Equal("data20", results.Value.ToStringUtf8());
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task Put_Get_Update()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putRequest = new PutRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putResults = await server.Put(putRequest, null);

            Assert.Equal(WriteResult.Success, putResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var getRequest = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Get(getRequest, null);

            Assert.Equal("data20", results.Value.ToStringUtf8());
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            putRequest = new PutRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data30")
            };
            putResults = await server.Put(putRequest, null);

            Assert.Equal(WriteResult.Success, putResults.Result);
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            getRequest = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            results = await server.Get(getRequest, null);

            Assert.Equal("data30", results.Value.ToStringUtf8());
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutIfNotExists_Get()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putRequest = new PutIfNotExistsRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putResults = await server.PutIfNotExists(putRequest, null);

            Assert.Equal(WriteResult.Success, putResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var getRequest = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Get(getRequest, null);

            Assert.Equal("data20", results.Value.ToStringUtf8());
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutIfExists_Get()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putIfNotExistsRequest = new PutIfNotExistsRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putIfNotExistsResults = await server.PutIfNotExists(putIfNotExistsRequest, null);

            Assert.Equal(WriteResult.Success, putIfNotExistsResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var putIfExistsRequest = new PutIfExistsRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putIfExistsResults = await server.PutIfExists(putIfExistsRequest, null);

            Assert.Equal(WriteResult.Success, putIfExistsResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var getRequest = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Get(getRequest, null);

            Assert.Equal("data20", results.Value.ToStringUtf8());
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutIfExists_Get_Update()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putIfNotExistsRequest = new PutIfNotExistsRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putIfNotExistsResults = await server.PutIfNotExists(putIfNotExistsRequest, null);

            Assert.Equal(WriteResult.Success, putIfNotExistsResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var putIfExistsRequest = new PutIfExistsRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putIfExistsResults = await server.PutIfExists(putIfExistsRequest, null);

            Assert.Equal(WriteResult.Success, putIfExistsResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var getRequest = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Get(getRequest, null);

            Assert.Equal("data20", results.Value.ToStringUtf8());
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            putIfExistsRequest = new PutIfExistsRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data30")
            };
            putIfExistsResults = await server.PutIfExists(putIfExistsRequest, null);

            Assert.Equal(WriteResult.Success, putIfExistsResults.Result);
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            getRequest = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            results = await server.Get(getRequest, null);

            Assert.Equal("data30", results.Value.ToStringUtf8());
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutMany()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
            {
                { "key20", ByteString.CopyFromUtf8("data20") },
                { "key30", ByteString.CopyFromUtf8("data30") }
            };

            var putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutMany(putManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task GetMany()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var request = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            request.Keys.Add("key20");
            request.Keys.Add("key30");
            var results = await server.GetMany(request, null);

            Assert.Empty(results.Entries);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(0, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(2, memoryCache.Stats.Misses);
            Assert.Equal(0, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutMany_GetMany()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutMany(putManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            var getRequest = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            getRequest.Keys.Add("key20");
            getRequest.Keys.Add("key30");
            var results = await server.GetMany(getRequest, null);

            Assert.Equal(2, results.Entries.Count);
            var value1 = results.Entries["key20"].ToStringUtf8();
            var value2 = results.Entries["key30"].ToStringUtf8();
            Assert.Equal("data20", value1);
            Assert.Equal("data30", value2);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutMany_GetMany_Update()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutMany(putManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            var getRequest = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            getRequest.Keys.Add("key20");
            getRequest.Keys.Add("key30");
            var results = await server.GetMany(getRequest, null);

            Assert.Equal(2, results.Entries.Count);
            var value1 = results.Entries["key20"].ToStringUtf8();
            var value2 = results.Entries["key30"].ToStringUtf8();
            Assert.Equal("data20", value1);
            Assert.Equal("data30", value2);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data22") },
                    { "key30", ByteString.CopyFromUtf8("data33") }
                };

            putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutMany(putManyRequest, null);

            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            getRequest = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            getRequest.Keys.Add("key20");
            getRequest.Keys.Add("key30");
            results = await server.GetMany(getRequest, null);

            Assert.Equal(2, results.Entries.Count);
            value1 = results.Entries["key20"].ToStringUtf8();
            value2 = results.Entries["key30"].ToStringUtf8();
            Assert.Equal("data22", value1);
            Assert.Equal("data33", value2);
            Assert.Equal(4, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task PutIfNotExistsMany_GetMany()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putManyRequest = new PutIfNotExistsManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutIfNotExistsMany(putManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            var getRequest = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            getRequest.Keys.Add("key20");
            getRequest.Keys.Add("key30");
            var results = await server.GetMany(getRequest, null);

            Assert.Equal(2, results.Entries.Count);
            var value1 = results.Entries["key20"].ToStringUtf8();
            var value2 = results.Entries["key30"].ToStringUtf8();
            Assert.Equal("data20", value1);
            Assert.Equal("data30", value2);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task PutIfExistsMany_GetMany()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putIfNotExistsManyRequest = new PutIfNotExistsManyRequest
            {
                PartitionKey = partitionKey
            };
            putIfNotExistsManyRequest.Entries.Add(items);
            await server.PutIfNotExistsMany(putIfNotExistsManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data22") },
                    { "key30", ByteString.CopyFromUtf8("data33") }
                };

            var putIfExistsManyRequest = new PutIfExistsManyRequest
            {
                PartitionKey = partitionKey
            };
            putIfExistsManyRequest.Entries.Add(items);
            await server.PutIfExistsMany(putIfExistsManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            var getRequest = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            getRequest.Keys.Add("key20");
            getRequest.Keys.Add("key30");
            var results = await server.GetMany(getRequest, null);

            Assert.Equal(2, results.Entries.Count);
            var value1 = results.Entries["key20"].ToStringUtf8();
            var value2 = results.Entries["key30"].ToStringUtf8();
            Assert.Equal("data22", value1);
            Assert.Equal("data33", value2);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task PutIfExistsMany_GetMany_Update()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putIfNotExistsManyRequest = new PutIfNotExistsManyRequest
            {
                PartitionKey = partitionKey
            };
            putIfNotExistsManyRequest.Entries.Add(items);
            await server.PutIfNotExistsMany(putIfNotExistsManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data22") },
                    { "key30", ByteString.CopyFromUtf8("data33") }
                };

            var putIfExistsManyRequest = new PutIfExistsManyRequest
            {
                PartitionKey = partitionKey
            };
            putIfExistsManyRequest.Entries.Add(items);
            await server.PutIfExistsMany(putIfExistsManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            var getRequest = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            getRequest.Keys.Add("key20");
            getRequest.Keys.Add("key30");
            var results = await server.GetMany(getRequest, null);

            Assert.Equal(2, results.Entries.Count);
            var value1 = results.Entries["key20"].ToStringUtf8();
            var value2 = results.Entries["key30"].ToStringUtf8();
            Assert.Equal("data22", value1);
            Assert.Equal("data33", value2);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data222") },
                    { "key30", ByteString.CopyFromUtf8("data333") }
                };

            putIfExistsManyRequest = new PutIfExistsManyRequest
            {
                PartitionKey = partitionKey
            };
            putIfExistsManyRequest.Entries.Add(items);
            await server.PutIfExistsMany(putIfExistsManyRequest, null);

            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            getRequest = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            getRequest.Keys.Add("key20");
            getRequest.Keys.Add("key30");
            results = await server.GetMany(getRequest, null);

            Assert.Equal(2, results.Entries.Count);
            value1 = results.Entries["key20"].ToStringUtf8();
            value2 = results.Entries["key30"].ToStringUtf8();
            Assert.Equal("data222", value1);
            Assert.Equal("data333", value2);
            Assert.Equal(4, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task Put_Get_Delete()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putRequest = new PutRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putResults = await server.Put(putRequest, null);

            Assert.Equal(WriteResult.Success, putResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var getRequest = new GetRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var getResult = await server.Get(getRequest, null);

            Assert.Equal("data20", getResult.Value.ToStringUtf8());
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var delRequest = new DeleteRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var delResults = await server.Delete(delRequest, null);

            Assert.Equal(WriteResult.Success, delResults.Result);

            getResult = await server.Get(getRequest, null);

            Assert.Empty(getResult.Value.ToByteArray());
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(1, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutMany_GetMany_DeleteMany()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutMany(putManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            var getRequest = new GetManyRequest
            {
                PartitionKey = partitionKey
            };
            getRequest.Keys.Add("key20");
            getRequest.Keys.Add("key30");
            var getResults = await server.GetMany(getRequest, null);

            Assert.Equal(2, getResults.Entries.Count);
            var value1 = getResults.Entries["key20"].ToStringUtf8();
            var value2 = getResults.Entries["key30"].ToStringUtf8();
            Assert.Equal("data20", value1);
            Assert.Equal("data30", value2);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            var delRequest = new DeleteManyRequest
            {
                PartitionKey = partitionKey
            };
            delRequest.Keys.Add("key20");
            delRequest.Keys.Add("key30");

            var delResults = await server.DeleteMany(delRequest, null);

            getResults = await server.GetMany(getRequest, null);

            Assert.Empty(getResults.Entries);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(2, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutPartition_GetPartitionStats()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putPartitionRequest = new PutPartitionRequest
            {
                PartitionKey = partitionKey,
                Expiration = TimeSpan.FromSeconds(10).ToDuration(),
                ExpirationType = ExpirationType.Absolute
            };
            var putPartitionResults = await server.PutPartition(putPartitionRequest,
                TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default));
            Assert.Equal(WriteResult.Success, putPartitionResults.Result);

            var request = new GetPartitionStatsRequest
            {
                PartitionKey = partitionKey,
            };
            var maintenance = new MaintenanceServiceHandler(memoryCache);
            var results = await maintenance.GetPartitionStats(request, null);

            Assert.Equal(partitionKey, results.PartitionKey);
            Assert.Equal(0, results.CurrentItemCount);
            Assert.Equal(TimeSpan.FromSeconds(10), results.Expiration.ToTimeSpan());
            Assert.Equal(ExpirationType.Absolute, results.ExpirationType);
            Assert.Equal(0.0, results.HitRate);
            Assert.Equal(_mockClock.Object.UtcNow, results.LastHitDate.ToDateTimeOffset());
            Assert.Equal(0, results.TotalEvictionCount);
            Assert.Equal(0, results.TotalExpiredCount);
            Assert.Equal(0, results.TotalHits);
            Assert.Equal(0, results.TotalMisses);

        }

        [Fact]
        public async Task GetPartitionStats_Null()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var request = new GetPartitionStatsRequest
            {
                PartitionKey = partitionKey,
            };
            var maintenance = new MaintenanceServiceHandler(memoryCache);
            var results = await maintenance.GetPartitionStats(request, null);

            Assert.Equal(partitionKey, results.PartitionKey);
            Assert.Equal(0, results.CurrentItemCount);
            Assert.Null(results.Expiration);
            Assert.Equal(ExpirationType.Sliding, results.ExpirationType);
            Assert.Equal(0.0, results.HitRate);
            Assert.Null(results.LastHitDate);
            Assert.Equal(0, results.TotalEvictionCount);
            Assert.Equal(0, results.TotalExpiredCount);
            Assert.Equal(0, results.TotalHits);
            Assert.Equal(0, results.TotalMisses);

        }

        [Fact]
        public async Task PutPartition_DeletePartition()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putPartitionRequest = new PutPartitionRequest
            {
                PartitionKey = partitionKey,
                Expiration = TimeSpan.FromSeconds(10).ToDuration(),
                ExpirationType = ExpirationType.Absolute
            };
            var putPartitionResults = await server.PutPartition(putPartitionRequest,
                TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default)
                );
            Assert.Equal(WriteResult.Success, putPartitionResults.Result);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(0, memoryCache.Stats.TotalItems);
            Assert.Equal(0, memoryCache.Stats.TotalExpirations);

            var request = new DeletePartitionRequest
            {
                PartitionKey = partitionKey,
            };
            var results = await server.DeletePartition(request, null);

            Assert.Equal(WriteResult.Success, results.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(0, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(0, memoryCache.Stats.TotalItems);
            Assert.Equal(0, memoryCache.Stats.TotalExpirations);

        }

        [Fact]
        public async Task Put_FlushPartition()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";
            var itemKey = "key20";
            var itemValue = ByteString.CopyFrom(new byte[] { 2 });

            var result = await server.Put(new PutRequest
            {
                PartitionKey = partitionKey,
                Key = itemKey,
                Value = itemValue
            }, null);

            Assert.Equal(WriteResult.Success, result.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var request = new FlushPartitionRequest
            {
                PartitionKey = partitionKey,
            };
            var results = await server.FlushPartition(request, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);
            Assert.Equal(0, memoryCache.Stats.TotalExpirations);

        }

        [Fact]
        public async Task Put_FlushAll()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";
            var itemKey = "key20";
            var itemValue = ByteString.CopyFrom(new byte[] { 2 });

            var result = await server.Put(new PutRequest
            {
                PartitionKey = partitionKey,
                Key = itemKey,
                Value = itemValue
            }, null);

            Assert.Equal(WriteResult.Success, result.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var request = new FlushAllRequest();
            var results = await server.FlushAll(request, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);
            Assert.Equal(0, memoryCache.Stats.TotalExpirations);

        }

        [Fact]
        public async Task Put_Touch()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putRequest = new PutRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putResults = await server.Put(putRequest, null);

            Assert.Equal(WriteResult.Success, putResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            var touchRequest = new TouchRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Touch(touchRequest, null);

            Assert.Equal(WriteResult.Success, results.Result);
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutMany_TouchMany()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            var putResult = await server.PutMany(putManyRequest, null);
            Assert.Equal(WriteResult.Success, putResult.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            var touchRequest = new TouchManyRequest
            {
                PartitionKey = partitionKey
            };
            touchRequest.Keys.Add("key20");
            touchRequest.Keys.Add("key30");
            var results = await server.TouchMany(touchRequest, null);

            Assert.Equal(2, results.Touched);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task Put_Ttl()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putPartitionRequest = new PutPartitionRequest
            {
                PartitionKey = partitionKey,
                Expiration = TimeSpan.FromMinutes(2).ToDuration(),
                ExpirationType = ExpirationType.Absolute
            };
            var putPartitionResults = await server.PutPartition(putPartitionRequest,
                TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default));
            Assert.Equal(WriteResult.Success, putPartitionResults.Result);

            var putRequest = new PutRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = ByteString.CopyFromUtf8("data20")
            };
            var putResults = await server.Put(putRequest, null);

            Assert.Equal(WriteResult.Success, putResults.Result);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

            _mockClock
               .SetupGet(x => x.UtcNow)
               .Returns(DateTime.Parse("2019-04-25 3:01 PM"));
            server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var ttlRequest = new TtlRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Ttl(ttlRequest, null);

            Assert.Equal(60000, results.ValueMs);
            Assert.Equal(1, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task PutMany_TtlMany()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putPartitionRequest = new PutPartitionRequest
            {
                PartitionKey = partitionKey,
                Expiration = TimeSpan.FromMinutes(2).ToDuration(),
                ExpirationType = ExpirationType.Sliding
            };
            var putPartitionResults = await server.PutPartition(putPartitionRequest,
                TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default));
            Assert.Equal(WriteResult.Success, putPartitionResults.Result);
            var items = new Dictionary<string, ByteString>
                {
                    { "key20", ByteString.CopyFromUtf8("data20") },
                    { "key30", ByteString.CopyFromUtf8("data30") }
                };

            var putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutMany(putManyRequest, null);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:01 PM"));
            server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var ttlRequest = new TtlManyRequest
            {
                PartitionKey = partitionKey
            };
            ttlRequest.Keys.Add("key20");
            ttlRequest.Keys.Add("key30");
            var results = await server.TtlMany(ttlRequest, null);

            Assert.Equal(2, results.Entries.Count);
            var value1 = results.Entries["key20"];
            var value2 = results.Entries["key30"];
            Assert.Equal(60000, value1);
            Assert.Equal(60000, value2);
            Assert.Equal(2, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);

        }

        [Fact]
        public async Task Increment()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var request = new IncrementRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Increment(request, null);

            Assert.Equal(1, results.Value);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(1, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task IncrementBy()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var request = new IncrementByRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = 2
            };
            var results = await server.IncrementBy(request, null);

            Assert.Equal(2, results.Value);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(1, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task Decrement()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var request = new DecrementRequest
            {
                PartitionKey = partitionKey,
                Key = "key20"
            };
            var results = await server.Decrement(request, null);

            Assert.Equal(-1, results.Value);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(1, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task DecrementBy()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM"));

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var request = new DecrementByRequest
            {
                PartitionKey = partitionKey,
                Key = "key20",
                Value = 2
            };
            var results = await server.DecrementBy(request, null);

            Assert.Equal(-2, results.Value);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(1, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task ScanKeys()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("3:00 PM"));

            var conf = new CacheServerOptions();
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());
            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
            {
                { "key1", ByteString.CopyFromUtf8("data1") },
                { "key2", ByteString.CopyFromUtf8("data2") }
            };

            var putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutMany(putManyRequest, null);

            var request = new ScanKeysRequest
            {
                PartitionKey = partitionKey,
                Count = 1,
                Pattern = "*"
            };

            var streamWriter = new MockServerStreamWriter<ScanKeysResponse>();

            await server.ScanKeys(request, streamWriter, TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default));

            Assert.Single(streamWriter.Items);
            Assert.Equal("key1", streamWriter.Items[0].Key);
            Assert.Equal("data1", streamWriter.Items[0].Value.ToStringUtf8());
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task ScanKeys_NoMatch()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("3:00 PM"));

            var conf = new CacheServerOptions();
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());
            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var items = new Dictionary<string, ByteString>
            {
                { "key1", ByteString.CopyFromUtf8("data1") },
                { "key2", ByteString.CopyFromUtf8("data2") }
            };

            var putManyRequest = new PutManyRequest
            {
                PartitionKey = partitionKey
            };
            putManyRequest.Entries.Add(items);
            await server.PutMany(putManyRequest, null);

            var request = new ScanKeysRequest
            {
                PartitionKey = partitionKey,
                Count = 1,
                Pattern = "*abc*"
            };

            var streamWriter = new MockServerStreamWriter<ScanKeysResponse>();

            await server.ScanKeys(request, streamWriter, TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default));

            Assert.Empty(streamWriter.Items);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(2, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(2, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task ScanPartitions()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("3:00 PM"));

            var conf = new CacheServerOptions();
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());
            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putPartitionRequest = new PutPartitionRequest
            {
                PartitionKey = partitionKey,
                Expiration = TimeSpan.FromSeconds(10).ToDuration(),
                ExpirationType = ExpirationType.Absolute
            };
            var putPartitionResults = await server.PutPartition(putPartitionRequest,
                TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default)
                );

            var request = new ScanPartitionsRequest
            {
                Count = 1,
                Pattern = "*"
            };

            var streamWriter = new MockServerStreamWriter<ScanPartitionsResponse>();

            await server.ScanPartitions(request, streamWriter, TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default));

            Assert.Single(streamWriter.Items);
            Assert.Equal(0, streamWriter.Items[0].Stats.CurrentItemCount);
            Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.ToTimestamp(), streamWriter.Items[0].Stats.CreateDate);
            Assert.Equal(EvictionPolicy.Lru, streamWriter.Items[0].Stats.EvictionPolicy);
            Assert.Equal(TimeSpan.FromSeconds(10).ToDuration(), streamWriter.Items[0].Stats.Expiration);
            Assert.Equal(ExpirationType.Absolute, streamWriter.Items[0].Stats.ExpirationType);
            Assert.Equal(0, streamWriter.Items[0].Stats.HitRate);
            Assert.Equal(_mockClock.Object.UtcNow.UtcDateTime.ToTimestamp(), streamWriter.Items[0].Stats.LastHitDate);
            Assert.Equal(0, streamWriter.Items[0].Stats.MaxCacheSize);
            Assert.Equal(partitionKey, streamWriter.Items[0].Stats.PartitionKey);
            Assert.False(streamWriter.Items[0].Stats.Persisted);
            Assert.Equal(9, streamWriter.Items[0].Stats.TotalCacheSize);
            Assert.Equal(0, streamWriter.Items[0].Stats.TotalEvictionCount);
            Assert.Equal(0, streamWriter.Items[0].Stats.TotalExpiredCount);
            Assert.Equal(0, streamWriter.Items[0].Stats.TotalHits);
            Assert.Equal(0, streamWriter.Items[0].Stats.TotalMisses);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(0, memoryCache.Stats.TotalItems);
        }

        [Fact]
        public async Task ScanPartitions_NoMatch()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("3:00 PM"));

            var conf = new CacheServerOptions();
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());
            var server = new MackerelCacheServiceHandler(memoryCache, _mockClock.Object, _mockSchedulerFactory.Object, _mockHostLifetime.Object);
            var partitionKey = "partition";

            var putPartitionRequest = new PutPartitionRequest
            {
                PartitionKey = partitionKey,
                Expiration = TimeSpan.FromSeconds(10).ToDuration(),
                ExpirationType = ExpirationType.Absolute
            };
            var putPartitionResults = await server.PutPartition(putPartitionRequest,
                TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default)
                );

            var request = new ScanPartitionsRequest
            {
                Count = 1,
                Pattern = "*abc*"
            };

            var streamWriter = new MockServerStreamWriter<ScanPartitionsResponse>();

            await server.ScanPartitions(request, streamWriter, TestServerCallContext.Create(
                    default, default, default, default,
                    CancellationToken.None,
                    default, default, default, default, default, default));

            Assert.Empty(streamWriter.Items);

            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(0, memoryCache.Stats.TotalItems);
        }

    }
}
