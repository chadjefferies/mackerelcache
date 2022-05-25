using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Expiration;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Tests.Util;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using Quartz;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class EagerExpirationTests
    {
        private readonly Mock<ILogger<MemoryStore>> _mockLogger;
        private readonly Mock<ILogger<EagerExpirationJob>> _mockLogger2;
        private readonly Mock<ISystemClock> _mockClock;
        private readonly Mock<IJobExecutionContext> _mockJobExecutionContext;
        private readonly Mock<IJobDetail> _mockJobDetail;


        public EagerExpirationTests()
        {
            _mockClock = new Mock<ISystemClock>();
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("4:11 PM"));
            _mockJobExecutionContext = new Mock<IJobExecutionContext>();
            _mockJobExecutionContext.SetupGet(x => x.CancellationToken)
                .Returns(CancellationToken.None);
            _mockJobExecutionContext.SetupGet(x => x.FireTimeUtc)
                .Returns(DateTime.Parse("4:11 PM"));
            _mockJobExecutionContext.SetupGet(x => x.MergedJobDataMap)
                .Returns(new JobDataMap((IDictionary<string, object>)new Dictionary<string, object>()
                {
                    { "lastPartitionIndex", 0 }
                }));
            _mockJobDetail = new Mock<IJobDetail>();
            _mockJobDetail.SetupGet(x => x.JobDataMap)
                .Returns(new JobDataMap());
            _mockJobExecutionContext.SetupGet(x => x.JobDetail)
                .Returns(_mockJobDetail.Object);
            _mockLogger = new Mock<ILogger<MemoryStore>>();
            _mockLogger
                .Setup(x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Exception>(),
                    (Func<It.IsAnyType, Exception, string>)It.IsAny<object>()))
                .Callback(() => { });
            _mockLogger2 = new Mock<ILogger<EagerExpirationJob>>();
            _mockLogger2
                .Setup(x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Exception>(),
                    (Func<It.IsAnyType, Exception, string>)It.IsAny<object>()))
                .Callback(() => { });
        }

        [Fact]
        public async Task ExpireSingleItem()
        {
            var opt = new CacheServerOptions()
            {
                KeyExpirationSamples = 20,
                EagerExpirationJobLimit = TimeSpan.FromMinutes(1),
            };

            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());

            var createDate = DateTime.Parse("4:09 PM");

            var job = new EagerExpirationJob(_mockLogger2.Object, cache, opt);

            var putStatus = await cache.PutPartition("p1",
                new PartitionMetadata(createDate.Ticks, TimeSpan.FromMinutes(1).Ticks, true, false, default, 0),
                createDate);
            Assert.Equal(WriteResult.Success, putStatus);

            putStatus = cache.Put("p1", "k", ByteString.CopyFromUtf8("v"), createDate);
            Assert.Equal(WriteResult.Success, putStatus);

            await job.Execute(_mockJobExecutionContext.Object);

            Assert.Equal(1, cache.Stats.Partitions);
            Assert.Equal(0, cache.Stats.TotalEvictions);
            Assert.Equal(1, cache.Stats.TotalExpirations);
            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(1, cache.Stats.TotalItems);

            var partition = cache.GetPartition("p1").Value;
            Assert.Equal(0, partition.Stats.TotalEvictionCount);
            Assert.Equal(1, partition.Stats.TotalExpiredCount);
            Assert.Equal(0, partition.Stats.CurrentItemCount);
        }

        [Fact]
        public async Task ExpireManyItems()
        {
            var opt = new CacheServerOptions
            {
                KeyExpirationSamples = 20,
                EagerExpirationJobLimit = TimeSpan.FromMinutes(1),
            };

            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());

            var createDate = DateTime.Parse("4:09 PM");

            var job = new EagerExpirationJob(_mockLogger2.Object, cache, opt);

            for (int i = 0; i < 10; i++)
            {
                createDate = createDate.AddTicks(i);
                var partitionKey = i.ToString("00");
                var partitionPutStatus = await cache.PutPartition(partitionKey,
                   new PartitionMetadata(
                       createDate.Ticks,
                       TimeSpan.FromMinutes(1).Ticks,
                       false,
                       false,
                       EvictionPolicy.Lru,
                       0),
                   createDate);

                Assert.Equal(WriteResult.Success, partitionPutStatus);

                for (int j = 0; j < 10; j++)
                {
                    var putStatus = cache.Put(partitionKey,
                        j.ToString("0000"),
                        ByteString.CopyFrom(Guid.NewGuid().ToByteArray()),
                        createDate.AddTicks(i));

                    Assert.Equal(WriteResult.Success, putStatus);
                    Assert.Equal(0, cache.Stats.TotalEvictions);
                }
            }

            for (int i = 0; i < 5; i++)
            {
                await job.Execute(_mockJobExecutionContext.Object);
            }

            Assert.Equal(10, cache.Stats.Partitions);
            Assert.Equal(0, cache.Stats.TotalEvictions);
            Assert.Equal(100, cache.Stats.TotalExpirations);
            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(100, cache.Stats.TotalItems);
        }
    }
}
