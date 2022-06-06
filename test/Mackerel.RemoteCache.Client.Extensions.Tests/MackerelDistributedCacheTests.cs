using System;
using System.Linq;
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
using Mackerel.RemoteCache.Client.Watch;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using static Grpc.Core.Interceptors.Interceptor;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;
using static Mackerel.RemoteCache.Api.V1.MackerelCacheService;
using static Mackerel.RemoteCache.Api.V1.WatchService;

namespace Mackerel.RemoteCache.Client.Extensions.Tests
{
    public class MackerelDistributedCacheTests
    {
        private readonly Mock<ILogger<MackerelDistributedCache>> _mockLogger;
        private readonly MemoryCache _memoryCache;
        private readonly Mock<ICacheConnection> _mockConnection;
        private readonly Mock<ICache<byte[]>> _mockCache;
        private readonly Mock<ISystemClock> _mockClock;
        private CancellationTokenSource _cts;

        private readonly Mock<Interceptor> _mockInterceptor;
        private readonly Mock<IClientStreamWriter<WatchRequest>> _mockStreamWriter;
        private readonly Mock<IAsyncStreamReader<WatchResponse>> _mockStreamReader;
        private readonly ICacheNodeChannel _nodeChannel;

        public MackerelDistributedCacheTests()
        {
            _memoryCache = new MemoryCache(new MemoryCacheOptions());
            _mockLogger = new Mock<ILogger<MackerelDistributedCache>>();
            _mockConnection = new Mock<ICacheConnection>();
            _mockCache = new Mock<ICache<byte[]>>();
            _mockClock = new Mock<ISystemClock>();
            _mockClock
                .SetupGet(x => x.UtcNow)
                .Returns(DateTime.Parse("4:11 PM"));

            _mockLogger
                .Setup(x => x.Log(
                    It.IsAny<LogLevel>(),
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Exception>(),
                    (Func<It.IsAnyType, Exception, string>)It.IsAny<object>()))
                .Callback(() => { });

            _mockConnection
                .Setup(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalMinutes == 2),
                    It.IsAny<ExpirationType>(),
                    It.Is<bool>(x => x),
                    It.IsAny<EvictionPolicy>(),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _mockCache
                .Setup(x => x.DeleteAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _mockCache
                .Setup(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _mockCache
                .Setup(x => x.GetAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(System.Text.Encoding.UTF8.GetBytes("v1")));

            _mockCache
                .Setup(x => x.TouchAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _mockCache
                .Setup(x => x.WatchAsync(
                    It.Is<string>(x => x == "p1:*"),
                    It.Is<string>(x => x == "p1"),
                    It.IsAny<Action<WatchEvent<byte[]>>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            _cts = new CancellationTokenSource();
            _mockCache
               .SetupGet(x => x.WatchToken)
               .Returns(() => _cts.Token);


            _mockStreamWriter = new Mock<IClientStreamWriter<WatchRequest>>();
            _mockStreamReader = new Mock<IAsyncStreamReader<WatchResponse>>();


            _mockInterceptor = new Mock<Interceptor>();
            _mockInterceptor
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
            _mockInterceptor
             .Setup(x => x.AsyncUnaryCall(
                  It.Is<PutPartitionRequest>(x =>
                  x.PartitionKey == "p1" &&
                  x.Persist &&
                  x.MaxCacheSize == 0 &&
                  x.ExpirationType == ExpirationType.Absolute &&
                  x.Expiration.ToTimeSpan() == TimeSpan.FromSeconds(10) &&
                  x.EvictionPolicy == EvictionPolicy.Lru),
                  It.IsAny<ClientInterceptorContext<PutPartitionRequest, PutPartitionResponse>>(),
                  It.IsAny<AsyncUnaryCallContinuation<PutPartitionRequest, PutPartitionResponse>>()))
             .Returns(() => new AsyncUnaryCall<PutPartitionResponse>(
                 Task.FromResult(new PutPartitionResponse { Result = WriteResult.Success }), null, null, null, null));
            _mockInterceptor
              .Setup(x => x.AsyncUnaryCall(
                   It.Is<PutRequest>(x => x.PartitionKey == "p1" && x.Key == "k1" && x.Value == ByteString.CopyFromUtf8("v1")),
                   It.IsAny<ClientInterceptorContext<PutRequest, PutResponse>>(),
                   It.IsAny<AsyncUnaryCallContinuation<PutRequest, PutResponse>>()))
              .Returns(() => new AsyncUnaryCall<PutResponse>(
                  Task.FromResult(new PutResponse { Result = WriteResult.Success }), null, null, null, null));

            var uri = "http://ac.com";
            var channel = GrpcChannel.ForAddress(uri);
            var serviceClient = new ServiceClient(
                    new MackerelCacheServiceClient(channel.Intercept(_mockInterceptor.Object)),
                    new WatchServiceClient(channel.Intercept(_mockInterceptor.Object)),
                    new MaintenanceServiceClient(channel.Intercept(_mockInterceptor.Object)));
            _nodeChannel = new CacheNodeChannelImpl(
                new CacheClientOptions(),
                uri,
                channel,
                serviceClient);
        }

        [Fact]
        public async Task Get()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromMinutes(2),
                ExpirationType = ExpirationType.Absolute
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            var val = await distCache.GetStringAsync("k1");

            Assert.Equal("v1", val);

            _mockCache
                .Verify(x => x.GetAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.IsAny<CancellationToken>()),
               Times.Once());

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task Refresh()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1"
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.RefreshAsync("k1");

            _mockCache
               .Verify(x => x.TouchAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.IsAny<CancellationToken>()),
              Times.Once());

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task Remove()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1"
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.RemoveAsync("k1");

            _mockCache
               .Verify(x => x.DeleteAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.IsAny<CancellationToken>()),
              Times.Once());

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutPartition_ExpirationOverride()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromMinutes(2),
                ExpirationType = ExpirationType.Absolute
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1",
                new DistributedCacheEntryOptions
                {
                    // expiration set in the MackerelDistributedCacheOptions will override this
                    SlidingExpiration = TimeSpan.FromSeconds(20)
                });

            _mockConnection
                .Verify(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalMinutes == 2),
                    It.Is<ExpirationType>(x => x == ExpirationType.Absolute),
                    It.Is<bool>(x => x),
                    It.Is<EvictionPolicy>(x => x == EvictionPolicy.Lru),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()),
                Times.Once());

            _mockCache
                .Verify(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()),
               Times.Once());

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutPartition_ExpirationTypeOverride()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                // this does not override since no Expiration is set
                ExpirationType = ExpirationType.Absolute
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1",
                new DistributedCacheEntryOptions
                {
                    SlidingExpiration = TimeSpan.FromSeconds(20)
                });

            _mockConnection
                .Verify(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalSeconds == 20),
                    It.Is<ExpirationType>(x => x == ExpirationType.Sliding),
                    It.Is<bool>(x => x),
                    It.Is<EvictionPolicy>(x => x == EvictionPolicy.Lru),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()),
                Times.Once());

            _mockCache
                .Verify(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()),
               Times.Once());

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutPartition_ExpirationValueOverride()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                // this does not override since no ExpirationType is set
                Expiration = TimeSpan.FromMinutes(2),
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1",
                new DistributedCacheEntryOptions
                {
                    SlidingExpiration = TimeSpan.FromSeconds(20)
                });

            _mockConnection
                .Verify(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalSeconds == 20),
                    It.Is<ExpirationType>(x => x == ExpirationType.Sliding),
                    It.Is<bool>(x => x),
                    It.Is<EvictionPolicy>(x => x == EvictionPolicy.Lru),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()),
                Times.Once());

            _mockCache
                .Verify(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()),
               Times.Once());

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutPartition_EntryExpiration_AbsoluteExpirationRelativeToNow()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1",
                new DistributedCacheEntryOptions
                {
                    // since no expiration is set in the MackerelDistributedCacheOptions 
                    // we use the first item's expiration 
                    AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(2)
                });

            await distCache.SetStringAsync("k1", "v1",
               new DistributedCacheEntryOptions
               {
                   // this gets ignored
                   AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(4)
               });

            _mockConnection
                  .Verify(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalMinutes == 2),
                    It.Is<ExpirationType>(x => x == ExpirationType.Absolute),
                    It.Is<bool>(x => x),
                    It.Is<EvictionPolicy>(x => x == EvictionPolicy.Lru),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()),
                  Times.Once());

            _mockCache
                .Verify(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()),
               Times.Exactly(2));

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutPartition_EntryExpiration_AbsoluteExpiration()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1",
                new DistributedCacheEntryOptions
                {
                    // since no expiration is set in the MackerelDistributedCacheOptions 
                    // we use the first item's expiration 
                    AbsoluteExpiration = DateTime.Parse("4:13 PM")
                });

            await distCache.SetStringAsync("k1", "v1",
               new DistributedCacheEntryOptions
               {
                   // this gets ignored
                   AbsoluteExpiration = DateTime.Parse("4:16 PM")
               });

            _mockConnection
                  .Verify(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalMinutes == 2),
                    It.Is<ExpirationType>(x => x == ExpirationType.Absolute),
                    It.Is<bool>(x => x),
                    It.Is<EvictionPolicy>(x => x == EvictionPolicy.Lru),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()),
                  Times.Once());

            _mockCache
                .Verify(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()),
               Times.Exactly(2));

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutPartition_EntryExpiration_SlidingExpiration()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1",
                new DistributedCacheEntryOptions
                {
                    // since no expiration is set in the MackerelDistributedCacheOptions 
                    // we use the first item's expiration 
                    SlidingExpiration = TimeSpan.FromMinutes(2)
                });

            await distCache.SetStringAsync("k1", "v1",
               new DistributedCacheEntryOptions
               {
                   // this gets ignored
                   SlidingExpiration = TimeSpan.FromMinutes(4)
               });

            _mockConnection
                  .Verify(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalMinutes == 2),
                    It.Is<ExpirationType>(x => x == ExpirationType.Sliding),
                    It.Is<bool>(x => x),
                    It.Is<EvictionPolicy>(x => x == EvictionPolicy.Lru),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()),
                  Times.Once());

            _mockCache
                .Verify(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()),
               Times.Exactly(2));

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task NearCache_GetAndSet()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromSeconds(10),
                ExpirationType = ExpirationType.Absolute,
                UseNearCache = true
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1");
            var val = await distCache.GetStringAsync("k1");

            Assert.Equal("v1", val);

            _mockConnection
                .Verify(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalSeconds == 10),
                    It.Is<ExpirationType>(x => x == ExpirationType.Absolute),
                    It.Is<bool>(x => x),
                    It.Is<EvictionPolicy>(x => x == EvictionPolicy.Lru),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()),
                Times.Once());

            _mockCache
                .Verify(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()),
                Times.Once());

            _mockCache
                .Verify(x => x.WatchAsync(
                    It.Is<string>(x => x == "p1:*"),
                    It.Is<string>(x => x == "p1"),
                    It.IsAny<Action<WatchEvent<byte[]>>>(),
                    It.IsAny<CancellationToken>()),
                 Times.Once());
            _mockCache.Verify(x => x.WatchToken);

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task NearCache_GetAndSet_CancellationChangeToken()
        {
            using var distCache = new MackerelDistributedCache(_mockConnection.Object, _mockCache.Object, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromSeconds(10),
                ExpirationType = ExpirationType.Absolute,
                UseNearCache = true
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1");
            // cancelling this token should kick it out of local cache
            _cts.Cancel();
            var val = await distCache.GetStringAsync("k1");
            Assert.Equal("v1", val);
            val = await distCache.GetStringAsync("k1");
            Assert.Equal("v1", val);

            // register a new token so things get locally cached again
            _cts.Dispose();
            _cts = new CancellationTokenSource();
            await distCache.SetStringAsync("k1", "v1");
            val = await distCache.GetStringAsync("k1");

            _mockConnection
                .Verify(x => x.PutPartitionAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<TimeSpan>(x => x.TotalSeconds == 10),
                    It.Is<ExpirationType>(x => x == ExpirationType.Absolute),
                    It.Is<bool>(x => x),
                    It.Is<EvictionPolicy>(x => x == EvictionPolicy.Lru),
                    It.Is<long>(x => x == 0),
                    It.IsAny<CancellationToken>()),
                Times.Once());

            _mockCache
                .Verify(x => x.PutAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.Is<byte[]>(x => x.SequenceEqual(System.Text.Encoding.UTF8.GetBytes("v1"))),
                    It.IsAny<CancellationToken>()),
                Times.Exactly(2));

            _mockCache
               .Verify(x => x.GetAsync(
                    It.Is<string>(x => x == "p1"),
                    It.Is<string>(x => x == "k1"),
                    It.IsAny<CancellationToken>()),
              Times.Exactly(2));

            _mockCache
                .Verify(x => x.WatchAsync(
                    It.Is<string>(x => x == "p1:*"),
                    It.Is<string>(x => x == "p1"),
                    It.IsAny<Action<WatchEvent<byte[]>>>(),
                    It.IsAny<CancellationToken>()),
                 Times.Once());
            _mockCache.Verify(x => x.WatchToken);

            _mockConnection.VerifyNoOtherCalls();
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task NearCache_Set_BackgroundWatchUpdate_Get()
        {
            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new WatchResponse
                {
                    WatchId = "p1:*",
                    PartitionKey = "p1",
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("v2"),
                    WatchEventType = WatchEventType.Write
                });

            using var sendWatchEvent = new ManualResetEvent(false);
            int messages = 0;
            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (sendWatchEvent.WaitOne() && messages == 0)
                    {
                        messages++;
                        return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { _nodeChannel });

            await using var cache = connection.GetCache(new BinaryCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
            using var distCache = new MackerelDistributedCache(connection, cache, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromSeconds(10),
                ExpirationType = ExpirationType.Absolute,
                UseNearCache = true
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1");
            var val = await distCache.GetStringAsync("k1");
            Assert.Equal("v1", val);
            // trigger the background watch to send the updated value
            sendWatchEvent.Set();
            // this seems like a hack, but we need to give the background thread "some" time to do it's thing
            await Task.Delay(25);
            val = await distCache.GetStringAsync("k1");
            Assert.Equal("v2", val);

            _mockInterceptor
               .Verify(x => x.AsyncUnaryCall(
                    It.IsAny<PutRequest>(),
                    It.IsAny<ClientInterceptorContext<PutRequest, PutResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutRequest, PutResponse>>()),
               Times.Once());
            _mockInterceptor
                .Verify(x => x.AsyncUnaryCall(It.IsAny<PutPartitionRequest>(),
                  It.IsAny<ClientInterceptorContext<PutPartitionRequest, PutPartitionResponse>>(),
                  It.IsAny<AsyncUnaryCallContinuation<PutPartitionRequest, PutPartitionResponse>>()));
            _mockInterceptor
             .Verify(x => x.AsyncDuplexStreamingCall(
                  It.IsAny<ClientInterceptorContext<WatchRequest, WatchResponse>>(),
                  It.IsAny<AsyncDuplexStreamingCallContinuation<WatchRequest, WatchResponse>>()),
                Times.Once());

            _mockInterceptor.VerifyNoOtherCalls();

            _mockStreamWriter.Verify(x => x.WriteAsync(It.IsAny<WatchRequest>()), Times.Exactly(2));
            _mockStreamWriter.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task NearCache_BackgroundWatchUpdate_Get()
        {
            _mockInterceptor
             .Setup(x => x.AsyncUnaryCall(
                  It.Is<GetRequest>(x => x.PartitionKey == "p1" && x.Key == "k1"),
                  It.IsAny<ClientInterceptorContext<GetRequest, GetResponse>>(),
                  It.IsAny<AsyncUnaryCallContinuation<GetRequest, GetResponse>>()))
             .Returns(() => new AsyncUnaryCall<GetResponse>(
                 Task.FromResult(new GetResponse { Value = ByteString.CopyFromUtf8("v1") }), null, null, null, null));

            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new WatchResponse
                {
                    WatchId = "p1:*",
                    PartitionKey = "p1",
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("v2"),
                    WatchEventType = WatchEventType.Write
                });

            using var sendWatchEvent = new ManualResetEvent(false);
            int messages = 0;
            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (sendWatchEvent.WaitOne() && messages == 0)
                    {
                        messages++;
                        return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { _nodeChannel });

            await using var cache = connection.GetCache(new BinaryCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
            using var distCache = new MackerelDistributedCache(connection, cache, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromSeconds(10),
                ExpirationType = ExpirationType.Absolute,
                UseNearCache = true
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            var val = await distCache.GetStringAsync("k1");
            Assert.Equal("v1", val);
            // trigger the background watch to send the updated value
            sendWatchEvent.Set();
            // this seems like a hack, but we need to give the background thread "some" time to do it's thing
            await Task.Delay(25);
            val = await distCache.GetStringAsync("k1");
            Assert.Equal("v2", val);

            _mockInterceptor
               .Verify(x => x.AsyncUnaryCall(
                    It.IsAny<GetRequest>(),
                    It.IsAny<ClientInterceptorContext<GetRequest, GetResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<GetRequest, GetResponse>>()),
               Times.Once());
            _mockInterceptor
             .Verify(x => x.AsyncDuplexStreamingCall(
                  It.IsAny<ClientInterceptorContext<WatchRequest, WatchResponse>>(),
                  It.IsAny<AsyncDuplexStreamingCallContinuation<WatchRequest, WatchResponse>>()),
                Times.Once());

            _mockInterceptor.VerifyNoOtherCalls();

            _mockStreamWriter.Verify(x => x.WriteAsync(It.IsAny<WatchRequest>()), Times.Exactly(2));
            _mockStreamWriter.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task NearCache_Remove_BackgroundWatchUpdate_Get()
        {
            _mockInterceptor
             .Setup(x => x.AsyncUnaryCall(
                  It.Is<DeleteRequest>(x => x.PartitionKey == "p1" && x.Key == "k1"),
                  It.IsAny<ClientInterceptorContext<DeleteRequest, DeleteResponse>>(),
                  It.IsAny<AsyncUnaryCallContinuation<DeleteRequest, DeleteResponse>>()))
             .Returns(() => new AsyncUnaryCall<DeleteResponse>(
                 Task.FromResult(new DeleteResponse { Result = WriteResult.Success }), null, null, null, null));

            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new WatchResponse
                {
                    WatchId = "p1:*",
                    PartitionKey = "p1",
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("v2"),
                    WatchEventType = WatchEventType.Write
                });

            using var sendWatchEvent = new ManualResetEvent(false);
            int messages = 0;
            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (sendWatchEvent.WaitOne() && messages == 0)
                    {
                        messages++;
                        return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { _nodeChannel });

            await using var cache = connection.GetCache(new BinaryCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
            using var distCache = new MackerelDistributedCache(connection, cache, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromSeconds(10),
                ExpirationType = ExpirationType.Absolute,
                UseNearCache = true
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.RemoveAsync("k1");
            var val = await distCache.GetStringAsync("k1");
            Assert.Null(val);
            // trigger the background watch to send the updated value
            sendWatchEvent.Set();
            // this seems like a hack, but we need to give the background thread "some" time to do it's thing
            await Task.Delay(25);
            val = await distCache.GetStringAsync("k1");
            Assert.Equal("v2", val);

            _mockInterceptor
               .Verify(x => x.AsyncUnaryCall(
                    It.IsAny<DeleteRequest>(),
                    It.IsAny<ClientInterceptorContext<DeleteRequest, DeleteResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DeleteRequest, DeleteResponse>>()),
               Times.Once());
            _mockInterceptor
             .Verify(x => x.AsyncDuplexStreamingCall(
                  It.IsAny<ClientInterceptorContext<WatchRequest, WatchResponse>>(),
                  It.IsAny<AsyncDuplexStreamingCallContinuation<WatchRequest, WatchResponse>>()),
                Times.Once());

            _mockInterceptor.VerifyNoOtherCalls();

            _mockStreamWriter.Verify(x => x.WriteAsync(It.IsAny<WatchRequest>()), Times.Exactly(2));
            _mockStreamWriter.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task NearCache_BackgroundWatchExpire()
        {
            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new WatchResponse
                {
                    WatchId = "p1:*",
                    PartitionKey = "p1",
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("v2"),
                    WatchEventType = WatchEventType.Expire
                });

            using var sendWatchEvent = new ManualResetEvent(false);
            int messages = 0;
            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (sendWatchEvent.WaitOne() && messages == 0)
                    {
                        messages++;
                        return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { _nodeChannel });

            await using var cache = connection.GetCache(new BinaryCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
            using var distCache = new MackerelDistributedCache(connection, cache, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromSeconds(10),
                ExpirationType = ExpirationType.Absolute,
                UseNearCache = true
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1");
            var val = await distCache.GetStringAsync("k1");
            Assert.Equal("v1", val);
            // trigger the background watch to send the updated value
            sendWatchEvent.Set();
            // this seems like a hack, but we need to give the background thread "some" time to do it's thing
            await Task.Delay(25);
            val = await distCache.GetStringAsync("k1");
            Assert.Null(val);

            _mockInterceptor
               .Verify(x => x.AsyncUnaryCall(
                    It.IsAny<PutRequest>(),
                    It.IsAny<ClientInterceptorContext<PutRequest, PutResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutRequest, PutResponse>>()),
               Times.Once());
            _mockInterceptor
                .Verify(x => x.AsyncUnaryCall(It.IsAny<PutPartitionRequest>(),
                  It.IsAny<ClientInterceptorContext<PutPartitionRequest, PutPartitionResponse>>(),
                  It.IsAny<AsyncUnaryCallContinuation<PutPartitionRequest, PutPartitionResponse>>()));
            _mockInterceptor
             .Verify(x => x.AsyncDuplexStreamingCall(
                  It.IsAny<ClientInterceptorContext<WatchRequest, WatchResponse>>(),
                  It.IsAny<AsyncDuplexStreamingCallContinuation<WatchRequest, WatchResponse>>()),
                Times.Once());

            _mockInterceptor.VerifyNoOtherCalls();

            _mockStreamWriter.Verify(x => x.WriteAsync(It.IsAny<WatchRequest>()), Times.Exactly(2));
            _mockStreamWriter.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task NearCache_BackgroundWatchDelete()
        {
            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new WatchResponse
                {
                    WatchId = "p1:*",
                    PartitionKey = "p1",
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("v2"),
                    WatchEventType = WatchEventType.Delete
                });

            using var sendWatchEvent = new ManualResetEvent(false);
            int messages = 0;
            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (sendWatchEvent.WaitOne() && messages == 0)
                    {
                        messages++;
                        return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { _nodeChannel });

            await using var cache = connection.GetCache(new BinaryCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
            using var distCache = new MackerelDistributedCache(connection, cache, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromSeconds(10),
                ExpirationType = ExpirationType.Absolute,
                UseNearCache = true
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1");
            var val = await distCache.GetStringAsync("k1");
            Assert.Equal("v1", val);
            // trigger the background watch to send the updated value
            sendWatchEvent.Set();
            // this seems like a hack, but we need to give the background thread "some" time to do it's thing
            await Task.Delay(25);
            val = await distCache.GetStringAsync("k1");
            Assert.Null(val);

            _mockInterceptor
               .Verify(x => x.AsyncUnaryCall(
                    It.IsAny<PutRequest>(),
                    It.IsAny<ClientInterceptorContext<PutRequest, PutResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutRequest, PutResponse>>()),
               Times.Once());
            _mockInterceptor
                .Verify(x => x.AsyncUnaryCall(It.IsAny<PutPartitionRequest>(),
                  It.IsAny<ClientInterceptorContext<PutPartitionRequest, PutPartitionResponse>>(),
                  It.IsAny<AsyncUnaryCallContinuation<PutPartitionRequest, PutPartitionResponse>>()));
            _mockInterceptor
             .Verify(x => x.AsyncDuplexStreamingCall(
                  It.IsAny<ClientInterceptorContext<WatchRequest, WatchResponse>>(),
                  It.IsAny<AsyncDuplexStreamingCallContinuation<WatchRequest, WatchResponse>>()),
                Times.Once());

            _mockInterceptor.VerifyNoOtherCalls();

            _mockStreamWriter.Verify(x => x.WriteAsync(It.IsAny<WatchRequest>()), Times.Exactly(2));
            _mockStreamWriter.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task NearCache_BackgroundWatchEvict()
        {
            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new WatchResponse
                {
                    WatchId = "p1:*",
                    PartitionKey = "p1",
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("v2"),
                    WatchEventType = WatchEventType.Evict
                });

            using var sendWatchEvent = new ManualResetEvent(false);
            int messages = 0;
            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (sendWatchEvent.WaitOne() && messages == 0)
                    {
                        messages++;
                        return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { _nodeChannel });

            await using var cache = connection.GetCache(new BinaryCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
            using var distCache = new MackerelDistributedCache(connection, cache, new MackerelDistributedCacheOptions
            {
                Partition = "p1",
                Expiration = TimeSpan.FromSeconds(10),
                ExpirationType = ExpirationType.Absolute,
                UseNearCache = true
            }, _memoryCache, _mockLogger.Object, _mockClock.Object);

            await distCache.SetStringAsync("k1", "v1");
            var val = await distCache.GetStringAsync("k1");
            Assert.Equal("v1", val);
            // trigger the background watch to send the updated value
            sendWatchEvent.Set();
            // this seems like a hack, but we need to give the background thread "some" time to do it's thing
            await Task.Delay(25);
            val = await distCache.GetStringAsync("k1");
            Assert.Null(val);

            _mockInterceptor
               .Verify(x => x.AsyncUnaryCall(
                    It.IsAny<PutRequest>(),
                    It.IsAny<ClientInterceptorContext<PutRequest, PutResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutRequest, PutResponse>>()),
               Times.Once());
            _mockInterceptor
                .Verify(x => x.AsyncUnaryCall(It.IsAny<PutPartitionRequest>(),
                  It.IsAny<ClientInterceptorContext<PutPartitionRequest, PutPartitionResponse>>(),
                  It.IsAny<AsyncUnaryCallContinuation<PutPartitionRequest, PutPartitionResponse>>()));
            _mockInterceptor
             .Verify(x => x.AsyncDuplexStreamingCall(
                  It.IsAny<ClientInterceptorContext<WatchRequest, WatchResponse>>(),
                  It.IsAny<AsyncDuplexStreamingCallContinuation<WatchRequest, WatchResponse>>()),
                Times.Once());

            _mockInterceptor.VerifyNoOtherCalls();

            _mockStreamWriter.Verify(x => x.WriteAsync(It.IsAny<WatchRequest>()), Times.Exactly(2));
            _mockStreamWriter.VerifyNoOtherCalls();
        }
    }
}
