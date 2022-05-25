using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
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
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests
{
    public class WatchServiceHandlerTests
    {
        private readonly Mock<ILogger<MemoryStore>> _mockLogger;
        private readonly Mock<ISystemClock> _mockClock;
        private readonly Mock<IHostApplicationLifetime> _mockHostLifetime;

        public WatchServiceHandlerTests()
        {
            _mockClock = new Mock<ISystemClock>();
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
        public async Task Watch_Timeout()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("3:00 PM"));

            var conf = new CacheServerOptions();
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());
            var server = new WatchServiceHandler(memoryCache, _mockClock.Object, _mockHostLifetime.Object);

            var streamWriter = new MockServerStreamWriter<WatchResponse>();
            var mockStreamReader = new MockAsyncStreamReader<WatchRequest>(new[]
            {
                new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest
                    {
                        WatchId = "w1",
                        PartitionKey = "p1"
                    }
                }
            });

            var headers = new Metadata();
            headers.Add(GrpcRequestExtensions.SESSION_TIMEOUT, "500");
            var rpcException = await Assert.ThrowsAsync<RpcException>(() =>
                server.Watch(mockStreamReader, streamWriter, TestServerCallContext.Create(
                        default, default, default,
                        headers,
                        CancellationToken.None,
                        default, default, default, default, default, default)));
            Assert.Equal(StatusCode.Cancelled, rpcException.StatusCode);

            Assert.Empty(streamWriter.Items);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(0, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(0, memoryCache.Stats.TotalItems);
            Assert.Equal(0, memoryCache.Stats.CurrentWatches);
            Assert.Equal(0, memoryCache.Stats.CurrentWatchStreams);
            Assert.Equal(0, memoryCache.Stats.TotalWatchEvents);
        }

        [Fact]
        public async Task Watch()
        {
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("3:00 PM"));

            var conf = new CacheServerOptions();
            using var memoryCache = new MemoryStore(_mockLogger.Object, conf, new RuntimeStatistics(conf, _mockClock.Object), new StubMemoryStorage());
            var server = new WatchServiceHandler(memoryCache, _mockClock.Object, _mockHostLifetime.Object);

            var streamWriter = new MockServerStreamWriter<WatchResponse>();
            var mockStreamReader = new MockAsyncStreamReader<WatchRequest>(new[]
            {
                new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest
                    {
                        WatchId = "w1",
                        PartitionKey = "p1"
                    }
                }
            });

            var headers = new Metadata();
            headers.Add(GrpcRequestExtensions.SESSION_TIMEOUT, "500");
            var w = Task.Run(() => server.Watch(mockStreamReader, streamWriter, TestServerCallContext.Create(
                          default, default, default,
                          headers,
                          CancellationToken.None,
                          default, default, default, default, default, default)));
            await Task.Delay(10);
            memoryCache.Put("p1", "k1", ByteString.CopyFromUtf8("v1"), _mockClock.Object.UtcNow.UtcDateTime);
            await Task.Delay(10);

            Assert.Single(streamWriter.Items);
            Assert.Equal("w1", streamWriter.Items[0].WatchId);
            Assert.Equal("p1", streamWriter.Items[0].PartitionKey);
            Assert.Equal("k1", streamWriter.Items[0].Key);
            Assert.Equal("v1", streamWriter.Items[0].Value.ToStringUtf8());
            Assert.Equal(WatchEventType.Write, streamWriter.Items[0].WatchEventType);
            Assert.Equal(0, memoryCache.Stats.Hits);
            Assert.Equal(1, memoryCache.Stats.Partitions);
            Assert.Equal(0, memoryCache.Stats.TotalEvictions);
            Assert.Equal(1, memoryCache.Stats.CurrentItems);
            Assert.Equal(0, memoryCache.Stats.Misses);
            Assert.Equal(1, memoryCache.Stats.TotalItems);
            Assert.Equal(1, memoryCache.Stats.CurrentWatches);
            Assert.Equal(1, memoryCache.Stats.CurrentWatchStreams);
            Assert.Equal(1, memoryCache.Stats.TotalWatchEvents);
        }
    }
}
