using System;
using System.Collections.Generic;
using Mackerel.RemoteCache.Client.Configuration;
using Xunit;

namespace Mackerel.RemoteCache.Client.Tests
{
    public class CacheClientOptionsTests
    {
        public static IEnumerable<object[]> ParseEndPointData()
        {
            yield return new object[] { "localhost", new Uri("http://localhost:11211") };
            yield return new object[] { "localhost:1234", new Uri("http://localhost:1234") };
            yield return new object[] { "localhost.domain.com", new Uri("http://localhost.domain.com:11211") };
            yield return new object[] { "localhost.domain.com:1234", new Uri("http://localhost.domain.com:1234") };
            yield return new object[] { "http://localhost", new Uri("http://localhost:11211") };
            yield return new object[] { "http://localhost:1234", new Uri("http://localhost:1234") };
            yield return new object[] { "http://localhost.domain.com", new Uri("http://localhost.domain.com:11211") };
            yield return new object[] { "http://localhost.domain.com:1234", new Uri("http://localhost.domain.com:1234") };
        }

        public static IEnumerable<object[]> ParseOptionData()
        {
            yield return new object[]
            {
                "abc:1234",
                new CacheClientOptions()
                {
                    Endpoints = new List<string>
                    {
                        "http://abc:1234/"
                    }
                }
            };
            yield return new object[]
            {
                "abc:1234,timeout=5",
                new CacheClientOptions()
                {
                    Endpoints = new List<string>
                    {
                        "http://abc:1234/"
                    },
                    TimeoutMilliseconds = 5
                }
            };
            yield return new object[]
            {
                "abc:123,def:456",
                new CacheClientOptions()
                {
                    Endpoints = new List<string>
                    {
                        "http://abc:123/",
                        "http://def:456/"
                    }
                }
            };
            yield return new object[]
            {
                "abc:123,def:456,timeout=50",
                new CacheClientOptions()
                {
                    Endpoints = new List<string>
                    {
                        "http://abc:123/",
                        "http://def:456/"
                    },
                    TimeoutMilliseconds = 50
                }
            };
        }

        [Theory]
        [MemberData(nameof(ParseEndPointData))]
        public void ParseEndPoint(string data, Uri uri)
        {
            var result = CacheClientOptions.ParseNode(data);
            Assert.Equal(uri, result);
        }

        [Theory]
        [MemberData(nameof(ParseOptionData))]
        public void ParseOptions(string connStr, CacheClientOptions opt)
        {
            var result = CacheClientOptions.Parse(connStr);
            Assert.Equal(opt.Endpoints.Count, result.Endpoints.Count);
            Assert.Equal(opt.TimeoutMilliseconds, result.TimeoutMilliseconds);
            for (int i = 0; i < result.Endpoints.Count; i++)
            {
                Assert.Equal(result.Endpoints[i], opt.Endpoints[i]);
            }
        }
    }
}
