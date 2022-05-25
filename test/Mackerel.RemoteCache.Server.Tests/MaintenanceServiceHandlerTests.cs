using System;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Rpc;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Tests.Util;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class MaintenanceServiceHandlerTests
    {
        private readonly Mock<ILogger<MemoryStore>> _mockLogger;
        private readonly Mock<ISystemClock> _mockClock;

        public MaintenanceServiceHandlerTests()
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
        public async Task GetStats()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM").ToUniversalTime());

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };
            using var memoryCache = new MemoryStore(
                _mockLogger.Object, 
                conf, 
                RuntimeStatistics.Create(conf, _mockClock.Object), 
                new StubMemoryStorage());

            var server = new MaintenanceServiceHandler(memoryCache);

            var request = new GetStatsRequest();
            var results = await server.GetStats(request, null);

            Assert.Equal(0, results.CurrentItems);
            Assert.Equal(0, results.TotalItems);
            Assert.Equal(0, results.Hits);
            Assert.Equal(0, results.Misses);
            Assert.Equal(0, results.HitRate);
            Assert.Equal(0, results.TotalEvictions);
            Assert.Equal(0, results.TotalExpirations);
            Assert.Equal(0, results.Partitions);
            Assert.Equal(TimeSpan.Zero.ToDuration(), results.EvictedTime);
            Assert.Equal(0, results.CurrentWatches);
            Assert.Equal(0, results.TotalWatchEvents);
            Assert.Equal(0, results.TotalCacheSize);
            Assert.Equal(0, results.TotalReservedCacheSize);
            Assert.Equal(DateTime.Parse("2019-04-25 3:00 PM").ToUniversalTime(), results.ModifiedDate.ToDateTime());
        }

        [Fact]
        public async Task Ping()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 3:00 PM").ToUniversalTime());

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128
            };
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, RuntimeStatistics.Create(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MaintenanceServiceHandler(memoryCache);

            var request = new PingRequest();
            var results = await server.Ping(request, null);

            Assert.Equal("PONG", results.Result);
        }

        [Fact]
        public async Task GetConf()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("3:00 PM").ToUniversalTime());

            var conf = new CacheServerOptions()
            {
                MaxBytesPerValue = 128,
                MaxBytesPerKey = 128,
                KeyEvictionSamples = 5,
                DataLocation = "abc",
                EagerExpirationJobLimit = TimeSpan.FromSeconds(10),
                EagerExpirationInterval = TimeSpan.FromSeconds(10),
                EvictionSampleRate = .4,
                KeyExpirationSamples = 10,
                MaxCacheSize = 999,
                StatsInterval = TimeSpan.FromHours(5)
            };

            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, RuntimeStatistics.Create(conf, _mockClock.Object), new StubMemoryStorage());

            var server = new MaintenanceServiceHandler(memoryCache);

            var request = new GetConfRequest();
            var results = await server.GetConf(request, null);

            Assert.Equal(128, results.MaxBytesPerKey);
            Assert.Equal(128, results.MaxBytesPerValue);
            Assert.Equal(5, results.KeyEvictionSamples); 
            Assert.Equal(.4, results.EvictionSampleRate);
            Assert.Equal(999, results.MaxCacheSize);
        }
    }
}
