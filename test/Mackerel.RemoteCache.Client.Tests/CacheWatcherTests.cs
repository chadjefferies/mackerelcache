using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Core.Testing;
using Grpc.Net.Client;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Configuration;
using Mackerel.RemoteCache.Client.Encoding;
using Mackerel.RemoteCache.Client.Routing;
using Moq;
using Xunit;
using static Grpc.Core.Interceptors.Interceptor;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;
using static Mackerel.RemoteCache.Api.V1.MackerelCacheService;
using static Mackerel.RemoteCache.Api.V1.WatchService;

namespace Mackerel.RemoteCache.Client.Tests
{
    public class CacheWatcherTests
    {
        private readonly Mock<Interceptor> _mock;
        private readonly Mock<IClientStreamWriter<WatchRequest>> _mockStreamWriter;
        private readonly Mock<IAsyncStreamReader<WatchResponse>> _mockStreamReader;

        public CacheWatcherTests()
        {
            _mock = new Mock<Interceptor>();
            _mockStreamWriter = new Mock<IClientStreamWriter<WatchRequest>>();
            _mockStreamWriter
                .Setup(x => x.WriteAsync(It.IsAny<WatchRequest>()))
                .Returns(() => Task.FromResult(new WatchResponse()));

            _mockStreamReader = new Mock<IAsyncStreamReader<WatchResponse>>();

            _mock
              .Setup(x => x.AsyncDuplexStreamingCall(
                   It.IsAny<ClientInterceptorContext<WatchRequest, WatchResponse>>(),
                   It.IsAny<AsyncDuplexStreamingCallContinuation<WatchRequest, WatchResponse>>()))
              .Returns(() => TestCalls.AsyncDuplexStreamingCall(
                    _mockStreamWriter.Object,
                    _mockStreamReader.Object,
                    Task.FromResult(new Metadata()),
                    () => Status.DefaultSuccess,
                    () => new Metadata(),
                    () => { }));
        }

        [Fact]
        public async Task WatchAsync()
        {
            using var createEvent = new ManualResetEvent(false);
            using var keepAliveEvent = new ManualResetEvent(false);
            Func<WatchRequest, bool> expectedRequest =
               x =>
               {
                   if (x.CreateRequest != null)
                   {
                       createEvent.Set();
                       return x.CreateRequest.WatchId == "w1"
                            && x.CreateRequest.PartitionKey == "p1"
                            && x.CreateRequest.Key == ""
                            && x.CreateRequest.Filters.Count == 1
                            && x.CreateRequest.Filters[0] == WatchEventType.Write;
                   }
                   else if (x.KeepAliveRequest != null)
                   {
                       keepAliveEvent.Set();
                       return true;
                   }
                   else if (x.CancelRequest != null)
                   {
                       return x.CancelRequest.WatchId == "w1"
                            && x.CancelRequest.PartitionKey == "p1";
                   }

                   return false;
               };

            _mockStreamWriter
                .Setup(x => x.WriteAsync(It.Is<WatchRequest>(x => expectedRequest(x))))
                .Returns(() => Task.FromResult(new WatchResponse()));

            int messages = 0;
            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (createEvent.WaitOne(1000) && messages == 0)
                    {
                        messages++;
                        return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                });

            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new WatchResponse
                {
                    WatchId = "w1",
                    PartitionKey = "p1",
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("value"),
                    WatchEventType = WatchEventType.Write
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = 500
            }, new[] { CreateNodeChannel() });

            await using (var watch = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter()))
            {
                await watch.WatchAsync("w1", "p1", new WatchEventType[] { WatchEventType.Write }, (w) =>
                {
                    Assert.Equal("w1", w.WatchId);
                    Assert.Equal("p1", w.Partition);
                    Assert.Equal("k1", w.Key);
                    Assert.Equal("value", w.Value);
                    Assert.Equal(WatchEventType.Write, w.EventType);
                });

                // fires on a background timer
                keepAliveEvent.WaitOne(1000);

                await watch.CancelAsync("w1", "p1");
            }

            // the first ever watch request fires two requests, all subsequent watch requests only fire once
            _mockStreamWriter.Verify(x => x.WriteAsync(It.Is<WatchRequest>(x => expectedRequest(x))), Times.Exactly(4));
            _mockStreamWriter.Verify(x => x.CompleteAsync(), Times.Once);
            _mockStreamWriter.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task WatchAsync_ThrowsError()
        {
            _mockStreamWriter
                .Setup(x => x.WriteAsync(It.IsAny<WatchRequest>()))
                .Returns(() => Task.FromResult(new WatchResponse()));

            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new RpcException(new Status(StatusCode.DeadlineExceeded, "Bad bad things")), TimeSpan.FromMilliseconds(500));

            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new WatchResponse
                {
                    WatchId = "w1",
                    PartitionKey = "p1",
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("value"),
                    WatchEventType = WatchEventType.Write
                });
            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = 500
            }, new[] { CreateNodeChannel() });

            int errors = 0;
            connection.ErrorHandler += e =>
            {
                errors++;
            };

            await using (var watch = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter()))
            {
                await watch.WatchAsync("w1", "p1", new WatchEventType[] { WatchEventType.Write }, w =>
                {
                    Assert.Equal("w1", w.WatchId);
                    Assert.Equal("p1", w.Partition);
                    Assert.Equal("k1", w.Key);
                    Assert.Equal("value", w.Value);
                    Assert.Equal(WatchEventType.Write, w.EventType);
                });

                await Task.Delay(1100);
            }

            Assert.Equal(3, errors);
            // the first ever watch request fires two requests, so three retries equals 6
            _mockStreamWriter.Verify(x => x.WriteAsync(It.IsAny<WatchRequest>()), Times.Exactly(6));
            _mockStreamWriter.Verify(x => x.CompleteAsync(), Times.Exactly(3));
            _mockStreamWriter.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task WatchAsync_RequiredParams()
        {
            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { CreateNodeChannel() });

            await using (var watch = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter()))
            {
                await Assert.ThrowsAsync<ArgumentNullException>(() => watch.WatchAsync(null, "p1", new WatchEventType[] { WatchEventType.Write }, (w) => { }));
                await Assert.ThrowsAsync<ArgumentNullException>(() => watch.WatchAsync("w1", null, new WatchEventType[] { WatchEventType.Write }, (w) => { }));
            }

            _mockStreamWriter.VerifyNoOtherCalls();
        }

        CacheNodeChannelImpl CreateNodeChannel()
        {
            var uri = "http://ac.com";
            var channel = GrpcChannel.ForAddress(uri);
            var serviceClient = new ServiceClient(
                    new MackerelCacheServiceClient(channel.Intercept(_mock.Object)),
                    new WatchServiceClient(channel.Intercept(_mock.Object)),
                    new MaintenanceServiceClient(channel.Intercept(_mock.Object)));
            return new CacheNodeChannelImpl(
                new CacheClientOptions(),
                uri,
                channel,
                serviceClient);
        }
    }
}
