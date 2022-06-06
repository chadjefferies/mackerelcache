using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Core.Testing;
using Grpc.Net.Client;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Configuration;
using Moq;
using Xunit;
using static Grpc.Core.Interceptors.Interceptor;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;
using static Mackerel.RemoteCache.Api.V1.MackerelCacheService;
using static Mackerel.RemoteCache.Api.V1.WatchService;

namespace Mackerel.RemoteCache.Client.Tests
{
    public class CacheConnectionTests
    {
        private readonly Mock<ICacheNodeChannel> _mockCache;
        private readonly Mock<Interceptor> _mockInterceptor;
        private readonly Mock<IAsyncStreamReader<ScanPartitionsResponse>> _mockStreamReader;

        public CacheConnectionTests()
        {
            _mockCache = new Mock<ICacheNodeChannel>();
            _mockInterceptor = new Mock<Interceptor>();
            _mockStreamReader = new Mock<IAsyncStreamReader<ScanPartitionsResponse>>();

            _mockInterceptor
              .Setup(x => x.AsyncServerStreamingCall(
                  It.IsAny<ScanPartitionsRequest>(),
                  It.IsAny<ClientInterceptorContext<ScanPartitionsRequest, ScanPartitionsResponse>>(),
                  It.IsAny<AsyncServerStreamingCallContinuation<ScanPartitionsRequest, ScanPartitionsResponse>>()))
              .Returns(() => TestCalls.AsyncServerStreamingCall(
                    _mockStreamReader.Object,
                    Task.FromResult(new Metadata()),
                    () => Status.DefaultSuccess,
                    () => new Metadata(),
                    () => { }));
        }

        [Fact]
        public async Task DeletePartitionAsync()
        {
            _mockCache
               .Setup(x => x.DeletePartitionAsync("dynamo",
                    It.IsAny<CancellationToken>()))
               .Returns(() =>
                    Task.FromResult(new NodeOperationResult<WriteResult>("ac")));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { _mockCache.Object }))
            {
                await connection.DeletePartitionAsync("dynamo");
            }

            _mockCache.Verify(x => x.DeletePartitionAsync("dynamo",
                It.IsAny<CancellationToken>()),
            Times.Once());

            _mockCache.Verify(x => x.DisposeAsync());
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task FlushPartitionAsync()
        {
            _mockCache
               .Setup(x => x.FlushPartitionAsync("dynamo",
                    It.IsAny<CancellationToken>()))
               .Returns(() =>
                    Task.FromResult(new NodeOperationResult("ac")));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { _mockCache.Object }))
            {
                await connection.FlushPartitionAsync("dynamo");
            }

            _mockCache.Verify(x => x.FlushPartitionAsync("dynamo",
                It.IsAny<CancellationToken>()),
            Times.Once());

            _mockCache.Verify(x => x.DisposeAsync());
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetPartitionStatsAsync()
        {
            _mockCache
               .Setup(x => x.GetPartitionStatsAsync("dynamo",
                    It.IsAny<CancellationToken>()))
               .Returns(() =>
                    Task.FromResult(new NodeOperationResult<PartitionStats>("ac", new PartitionStats())));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { _mockCache.Object }))
            {
                await connection.GetPartitionStatsAsync("dynamo");
            }

            _mockCache.Verify(x => x.GetPartitionStatsAsync("dynamo",
                It.IsAny<CancellationToken>()),
            Times.Once());

            _mockCache.Verify(x => x.DisposeAsync());
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutPartitionAsync()
        {
            _mockCache
               .Setup(x => x.PutPartitionAsync("dynamo",
                    TimeSpan.FromSeconds(1),
                    ExpirationType.Absolute,
                    true,
                    EvictionPolicy.Lru,
                    10,
                    It.IsAny<CancellationToken>()))
               .Returns(() =>
                    Task.FromResult(new NodeOperationResult<WriteResult>("ac", WriteResult.Success)));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { _mockCache.Object }))
            {
                await connection.PutPartitionAsync(
                    "dynamo",
                    TimeSpan.FromSeconds(1),
                    ExpirationType.Absolute,
                    true,
                    EvictionPolicy.Lru,
                    10);
            }

            _mockCache.Verify(x => x.PutPartitionAsync("dynamo",
                TimeSpan.FromSeconds(1),
                ExpirationType.Absolute,
                true,
                EvictionPolicy.Lru,
                10,
                It.IsAny<CancellationToken>()),
            Times.Once());

            _mockCache.Verify(x => x.DisposeAsync());
            _mockCache.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task ScanPartitionsAsync()
        {
            int messagesSent = 0;
            _mockStreamReader
                .Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (messagesSent == 0)
                    {
                        messagesSent++;
                        return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                });

            _mockStreamReader
                .SetupGet(x => x.Current)
                .Returns(new ScanPartitionsResponse
                {
                    Stats = new PartitionStats()
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { CreateNodeChannel() });

            int messagesScanned = 0;
            await foreach (var p in connection.ScanPartitionsAsync("*", 10))
            {
                messagesScanned++;
            }

            Assert.Equal(messagesSent, messagesScanned);
        }

        [Fact]
        public async Task GetStatsAsync()
        {
            _mockCache
               .Setup(x => x.GetStatsAsync(It.IsAny<CancellationToken>()))
               .Returns(() =>
                    Task.FromResult(new NodeOperationResult<CacheStats>("ac", new CacheStats())));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { _mockCache.Object }))
            {
                await connection.GetStatsAsync();
            }

            _mockCache.Verify(x => x.GetStatsAsync(It.IsAny<CancellationToken>()),
            Times.Once());

            _mockCache.Verify(x => x.DisposeAsync());
            _mockCache.VerifyNoOtherCalls();
        }

        CacheNodeChannelImpl CreateNodeChannel()
        {
            var uri = "http://ac.com";
            var channel = GrpcChannel.ForAddress(uri);
            var serviceClient = new ServiceClient(
                    new MackerelCacheServiceClient(channel.Intercept(_mockInterceptor.Object)),
                    new WatchServiceClient(channel.Intercept(_mockInterceptor.Object)),
                    new MaintenanceServiceClient(channel.Intercept(_mockInterceptor.Object)));
            return new CacheNodeChannelImpl(
                new CacheClientOptions(),
                uri,
                channel,
                serviceClient);
        }
    }
}
