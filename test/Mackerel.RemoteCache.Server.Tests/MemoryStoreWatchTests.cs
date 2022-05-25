using System;
using System.Collections.Generic;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Mackerel.RemoteCache.Server.Tests.Util;
using Mackerel.RemoteCache.Server.Watch;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class MemoryStoreWatchTests
    {
        private readonly Mock<ILogger<MemoryStore>> _mockLogger;
        private readonly Mock<ISystemClock> _mockClock;

        public MemoryStoreWatchTests()
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
        public void PutWatchPredicate_New()
        {
            var opt = new CacheServerOptions();
            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());
            using var channel = new WatcherChannel("c1");

            Assert.True(cache.PutWatchPredicate(channel, "w1", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);
        }

        [Fact]
        public void PutWatchPredicate_Update()
        {
            var opt = new CacheServerOptions();
            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());
            using var channel = new WatcherChannel("c1");

            Assert.True(cache.PutWatchPredicate(channel, "w1", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);

            Assert.False(cache.PutWatchPredicate(channel, "w1", "p1", "k1", new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);
        }

        [Fact]
        public void PutWatchPredicate_NewWatch()
        {
            var opt = new CacheServerOptions();
            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());
            using var channel = new WatcherChannel("c1");

            Assert.True(cache.PutWatchPredicate(channel, "w1", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);

            Assert.True(cache.PutWatchPredicate(channel, "w2", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);
        }

        [Fact]
        public void PutWatchPredicate_NewClient()
        {
            var opt = new CacheServerOptions();
            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());
            using var channel1 = new WatcherChannel("c1");

            Assert.True(cache.PutWatchPredicate(channel1, "w1", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);

            using var channel2 = new WatcherChannel("c2");
            Assert.True(cache.PutWatchPredicate(channel2, "w1", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);
        }

        [Fact]
        public void DeleteWatchPredicate_None()
        {
            var opt = new CacheServerOptions();
            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());
            using var channel = new WatcherChannel("c1");

            Assert.False(cache.DeleteWatchPredicate(channel, "w1", "p1"));
        }

        [Fact]
        public void DeleteWatchPredicate_Exists()
        {
            var opt = new CacheServerOptions();
            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());
            using var channel = new WatcherChannel("c1");

            Assert.True(cache.PutWatchPredicate(channel, "w1", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);

            Assert.True(cache.DeleteWatchPredicate(channel, "w1", "p1"));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);
        }

        [Fact]
        public void DeleteWatchPredicate_AcrossClient()
        {
            var opt = new CacheServerOptions();
            using var cache = new MemoryStore(_mockLogger.Object, opt, new RuntimeStatistics(opt, _mockClock.Object), new StubMemoryStorage());
            using var channel1 = new WatcherChannel("c1");

            Assert.True(cache.PutWatchPredicate(channel1, "w1", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);

            using var channel2 = new WatcherChannel("c2");
            Assert.True(cache.PutWatchPredicate(channel2, "w1", "p1", new CacheKey(), new List<WatchEventType>(), DateTime.Parse("3:00:00 PM")));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);

            Assert.True(cache.DeleteWatchPredicate(channel1, "w1", "p1"));

            Assert.Equal(0, cache.Stats.CurrentItems);
            Assert.Equal(2, cache.Stats.TotalCacheSize);
        }
    }
}
