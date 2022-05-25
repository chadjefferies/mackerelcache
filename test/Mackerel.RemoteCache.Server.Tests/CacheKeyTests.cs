using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Tests.Util;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class CacheKeyTests
    {
        [Fact]
        public void Default()
        {
            var data = Helpers.BuildRandomByteArray(100);
            CacheKey key = data;

            Assert.Equal(data, key.Key);
            Assert.Equal(100, key.Key.Length);
        }

        [Fact]
        public void Equals_Op()
        {
            var data = Helpers.BuildRandomByteArray(100);
            var firstValue = new CacheKey(data);
            var secondValue = new CacheKey(data);

            Assert.Equal(firstValue, secondValue);
            Assert.True(firstValue == secondValue);
            Assert.False(firstValue != secondValue);

            Assert.True(firstValue == data);
            Assert.False(firstValue != data);
            Assert.True(data == firstValue);
            Assert.False(data != firstValue);
        }

        [Fact]
        public void NotEquals_Default()
        {
            var data = Helpers.BuildRandomByteArray(100);
            var firstValue = new CacheKey(data);

            Assert.NotEqual(default, firstValue);
            Assert.False(firstValue == default(CacheKey));
            Assert.True(firstValue != default(CacheKey));
        }

        [Fact]
        public void Equals_Default()
        {
            CacheKey firstValue = default;

            Assert.Equal(default, firstValue);
            Assert.True(firstValue == default(CacheKey));
            Assert.False(firstValue != default(CacheKey));
        }

        [Fact]
        public void Equals_Object()
        {
            var data = Helpers.BuildRandomByteArray(100);
            var firstValue = new CacheKey(data);
            object secondValue = new CacheKey(data);

            Assert.Equal(firstValue, secondValue);
        }

        [Fact]
        public void LessThan()
        {
            CacheKey key1 = "a";
            CacheKey key2 = "b";

            Assert.True(key1 < key2);
            Assert.True(key1 <= key2);
        }

        [Fact]
        public void GreaterThan()
        {
            CacheKey key1 = "a";
            CacheKey key2 = "b";

            Assert.True(key2 > key1);
            Assert.True(key2 >= key1);
        }
    }
}
