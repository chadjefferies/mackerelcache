using System.Collections.Generic;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Watch;
using Microsoft.Extensions.Internal;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class WatchPredicateTests
    {
        private readonly Mock<ISystemClock> _mockClock;

        public WatchPredicateTests()
        {
            _mockClock = new Mock<ISystemClock>();
        }

        [Fact]
        public void WatchPredicate_Equals()
        {
            var clientId = "c1";
            using var channel = new WatcherChannel(clientId);

            var watchId = "w1";
            var partitionKey = "p1";
            var predicate1 = new WatchPredicate(
                channel,
                watchId,
                partitionKey,
                new CacheKey(),
                new List<WatchEventType>());

            var predicate2 = new WatchPredicate(
                channel,
                watchId,
                partitionKey,
                new CacheKey(),
                new List<WatchEventType>());

            Assert.Equal(predicate1, predicate2);
        }

        [Fact]
        public void WatchPredicate_NotEquals()
        {
            using var channel = new WatcherChannel("c1");

            var predicate1 = new WatchPredicate(
                channel,
                "w1",
                "p1",
                new CacheKey(),
                new List<WatchEventType>());

            var predicate2 = new WatchPredicate(
                channel,
                "w2",
                "p1",
                new CacheKey(),
                new List<WatchEventType>());

            Assert.NotEqual(predicate1, predicate2);
        }

        [Fact]
        public void WatchPredicate_NoKey_NoFilters()
        {
            using var channel = new WatcherChannel("c1");

            var predicate1 = new WatchPredicate(
                channel,
                "w1",
                "p1",
                new CacheKey(),
                new List<WatchEventType>());

            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Write));
            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Delete));
            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Expire));
            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Evict));
        }

        [Fact]
        public void WatchPredicate_Key_NoFilters()
        {
            using var channel = new WatcherChannel("c1");

            var predicate1 = new WatchPredicate(
                channel,
                "w1",
                "p1",
                "k1",
                new List<WatchEventType>());

            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Write));
            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Delete));
            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Expire));
            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Evict));

            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Write));
            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Delete));
            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Expire));
            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Evict));
        }

        [Fact]
        public void WatchPredicate_NoKey_Filters()
        {
            using var channel = new WatcherChannel("c1");

            var predicate1 = new WatchPredicate(
                channel,
                "w1",
                "p1",
                new CacheKey(),
                new List<WatchEventType>() { WatchEventType.Delete });

            Assert.False(predicate1.IsMatch("k1", WatchFilterFlags.Write));
            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Delete));
            Assert.False(predicate1.IsMatch("k1", WatchFilterFlags.Expire));
            Assert.False(predicate1.IsMatch("k1", WatchFilterFlags.Evict));

            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Write));
            Assert.True(predicate1.IsMatch("k2", WatchFilterFlags.Delete));
            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Expire));
            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Evict));
        }

        [Fact]
        public void WatchPredicate_Key_Filters()
        {
            using var channel = new WatcherChannel("c1");

            var predicate1 = new WatchPredicate(
                channel,
                "w1",
                "p1",
                "k1",
                new List<WatchEventType>() { WatchEventType.Write, WatchEventType.Evict });

            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Write));
            Assert.False(predicate1.IsMatch("k1", WatchFilterFlags.Delete));
            Assert.False(predicate1.IsMatch("k1", WatchFilterFlags.Expire));
            Assert.True(predicate1.IsMatch("k1", WatchFilterFlags.Evict));

            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Write));
            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Delete));
            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Expire));
            Assert.False(predicate1.IsMatch("k2", WatchFilterFlags.Evict));
        }
    }
}
