using System;
using Mackerel.RemoteCache.Server.Statistics;
using Microsoft.Extensions.Internal;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class RuntimeStatisticsTests
    {
        private readonly Mock<ISystemClock> _mockClock;

        public RuntimeStatisticsTests()
        {
            _mockClock = new Mock<ISystemClock>();
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("2019-04-25 4:11 PM"));
        }

        [Fact]
        public void ShouldEvict_Defaults()
        {
            var stats = new RuntimeStatistics(new CacheServerOptions(), _mockClock.Object);
            Assert.False(stats.ShouldEvict());
        }

        [Fact]
        public void ShouldEvict_Size()
        {
            var stats = new RuntimeStatistics(new CacheServerOptions
            {
                MaxCacheSize = 1
            }, _mockClock.Object);
            stats.IncrementSize(2);
            Assert.True(stats.ShouldEvict());
        }

        [Fact]
        public void ShouldEvict_Zeros()
        {
            var stats = new RuntimeStatistics(new CacheServerOptions
            {
                MaxCacheSize = 0
            }, _mockClock.Object);
            stats.IncrementSize(2);
            Assert.False(stats.ShouldEvict());
        }

        [Fact]
        public void ShouldEvict_CurrentItemsZero_Size()
        {
            var stats = new RuntimeStatistics(new CacheServerOptions
            {
                MaxCacheSize = 1
            }, _mockClock.Object);
            stats.IncrementSize(2);
            Assert.True(stats.ShouldEvict());
        }

        [Fact]
        public void AvailableCapacity_Unbounded()
        {
            var stats = new RuntimeStatistics(new CacheServerOptions(), _mockClock.Object);
            Assert.Equal(long.MaxValue, stats.AvailableCapacity);

            stats.IncrementSize(100000);
            Assert.Equal(long.MaxValue, stats.AvailableCapacity);
        }

        [Fact]
        public void AvailableCapacity_Size()
        {
            var stats = new RuntimeStatistics(new CacheServerOptions { MaxCacheSize = 100 }, _mockClock.Object);
            Assert.Equal(100, stats.AvailableCapacity);
            stats.IncrementSize(10);
            Assert.Equal(90, stats.AvailableCapacity);
            stats.IncrementSize(90);
            Assert.Equal(0, stats.AvailableCapacity);
        }

        [Fact]
        public void AvailableCapacity_ReservedSize()
        {
            var stats = new RuntimeStatistics(new CacheServerOptions { MaxCacheSize = 100 }, _mockClock.Object);
            Assert.Equal(100, stats.AvailableCapacity);
            stats.IncrementReservedSize(10);
            Assert.Equal(90, stats.AvailableCapacity);
            stats.IncrementReservedSize(90);
            Assert.Equal(0, stats.AvailableCapacity);
        }

        [Fact]
        public void AvailableCapacity_SizeAndReservedSize()
        {
            var stats = new RuntimeStatistics(new CacheServerOptions { MaxCacheSize = 100 }, _mockClock.Object);
            Assert.Equal(100, stats.AvailableCapacity);
            stats.IncrementReservedSize(10);
            stats.IncrementSize(60);
            Assert.Equal(40, stats.AvailableCapacity);
        }
    }
}
