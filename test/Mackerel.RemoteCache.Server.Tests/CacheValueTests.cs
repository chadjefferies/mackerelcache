using System;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Tests.Util;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class CacheValueTests
    {
        [Fact]
        public void Default()
        {
            var data = Helpers.BuildRandomByteArray(100);
            var accessTime = DateTime.Parse("2015-09-21 00:00:00");
            var value = new CacheValue(data, accessTime.Ticks);

            Assert.Equal(accessTime.Ticks, value.AccessTime);
            Assert.Equal(data, value.Value);
            Assert.Equal(100, value.Value.Length);
        }

        [Fact]
        public void IsExpired_ExpireCheckTimeSameAsCreateTime()
        {
            var data = Helpers.BuildRandomByteArray(10);
            var createTime = DateTime.Parse("2015-09-21 12:00:00");
            var value = new CacheValue(data, createTime.Ticks);
            var accessTime = DateTime.Parse("2015-09-21 12:00:00");

            Assert.False(value.IsExpired(accessTime.Ticks, TimeSpan.FromMinutes(30).Ticks));
        }

        [Fact]
        public void IsExpired_ExpireCheckTimeBeforeExpireTime()
        {
            var data = Helpers.BuildRandomByteArray(10);
            var createTime = DateTime.Parse("2015-09-21 12:00:00");
            var value = new CacheValue(data, createTime.Ticks);
            var accessTime = DateTime.Parse("2015-09-21 12:15:00");

            Assert.False(value.IsExpired(accessTime.Ticks, TimeSpan.FromMinutes(30).Ticks));
        }

        [Fact]
        public void IsExpired_ExpireCheckTimeAfterExpireTime()
        {
            var data = Helpers.BuildRandomByteArray(10);
            var createTime = DateTime.Parse("2015-09-21 12:00:00");
            var value = new CacheValue(data, createTime.Ticks);
            var accessTime = DateTime.Parse("2015-09-21 12:45:00");

            Assert.True(value.IsExpired(accessTime.Ticks, TimeSpan.FromMinutes(30).Ticks));
        }

        [Fact]
        public void IsExpired_NoExpirationGiven()
        {
            var data = Helpers.BuildRandomByteArray(10);
            var createTime = DateTime.Parse("2015-09-21 12:00:00");
            var value = new CacheValue(data, createTime.Ticks);
            var accessTime = DateTime.Parse("2015-09-21 12:15:00");

            Assert.False(value.IsExpired(accessTime.Ticks, new TimeSpan().Ticks));
        }
    }
}
