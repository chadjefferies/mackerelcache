using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client.Configuration;
using Mackerel.RemoteCache.Client.Tests.Util;
using Moq;
using Xunit;
using static Grpc.Core.Interceptors.Interceptor;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;
using static Mackerel.RemoteCache.Api.V1.MackerelCacheService;
using static Mackerel.RemoteCache.Api.V1.WatchService;

namespace Mackerel.RemoteCache.Client.Tests
{
    public class CacheNodeChannelTests
    {
        private readonly Mock<Interceptor> _mock;

        public CacheNodeChannelTests()
        {
            _mock = new Mock<Interceptor>();
        }

        [Fact]
        public async Task DeleteAsync()
        {
            Expression<Func<DeleteRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<DeleteRequest, DeleteResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DeleteRequest, DeleteResponse>>()))
               .Returns(() => new AsyncUnaryCall<DeleteResponse>(
                   Task.FromResult(new DeleteResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.DeleteAsync("dynamo", "ntesla");
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<DeleteRequest, DeleteResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<DeleteRequest, DeleteResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DeletePartitionAsync()
        {
            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is<DeletePartitionRequest>(p => p.PartitionKey == "dynamo"),
                    It.IsAny<ClientInterceptorContext<DeletePartitionRequest, DeletePartitionResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DeletePartitionRequest, DeletePartitionResponse>>()))
               .Returns(() => new AsyncUnaryCall<DeletePartitionResponse>(
                   Task.FromResult(new DeletePartitionResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.DeletePartitionAsync("dynamo");
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                    It.Is<DeletePartitionRequest>(p => p.PartitionKey == "dynamo"),
                    It.IsAny<ClientInterceptorContext<DeletePartitionRequest, DeletePartitionResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DeletePartitionRequest, DeletePartitionResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task FlushAllAsync()
        {
            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.IsAny<FlushAllRequest>(),
                    It.IsAny<ClientInterceptorContext<FlushAllRequest, FlushAllResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<FlushAllRequest, FlushAllResponse>>()))
               .Returns(() => new AsyncUnaryCall<FlushAllResponse>(
                   Task.FromResult(new FlushAllResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.FlushAllAsync();
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                    It.IsAny<FlushAllRequest>(),
                    It.IsAny<ClientInterceptorContext<FlushAllRequest, FlushAllResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<FlushAllRequest, FlushAllResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task FlushPartitionAsync()
        {
            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is<FlushPartitionRequest>(p => p.PartitionKey == "dynamo"),
                    It.IsAny<ClientInterceptorContext<FlushPartitionRequest, FlushPartitionResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<FlushPartitionRequest, FlushPartitionResponse>>()))
               .Returns(() => new AsyncUnaryCall<FlushPartitionResponse>(
                   Task.FromResult(new FlushPartitionResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.FlushPartitionAsync("dynamo");
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is<FlushPartitionRequest>(p => p.PartitionKey == "dynamo"),
                It.IsAny<ClientInterceptorContext<FlushPartitionRequest, FlushPartitionResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<FlushPartitionRequest, FlushPartitionResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetAsync()
        {
            Expression<Func<GetRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<GetRequest, GetResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<GetRequest, GetResponse>>()))
               .Returns(() => new AsyncUnaryCall<GetResponse>(
                   Task.FromResult(new GetResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.GetAsync("dynamo", "ntesla");
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<GetRequest, GetResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<GetRequest, GetResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetManyAsync()
        {
            Expression<Func<GetManyRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Keys[0] == "ntesla" && x.Keys[1] == "jbramah";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<GetManyRequest, GetManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<GetManyRequest, GetManyResponse>>()))
               .Returns(() => new AsyncUnaryCall<GetManyResponse>(
                   Task.FromResult(new GetManyResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.GetAsync("dynamo", new string[] { "ntesla", "jbramah" });
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<GetManyRequest, GetManyResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<GetManyRequest, GetManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetConfAsync()
        {
            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.IsAny<GetConfRequest>(),
                    It.IsAny<ClientInterceptorContext<GetConfRequest, CacheConfiguration>>(),
                    It.IsAny<AsyncUnaryCallContinuation<GetConfRequest, CacheConfiguration>>()))
               .Returns(() => new AsyncUnaryCall<CacheConfiguration>(
                   Task.FromResult(new CacheConfiguration()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.GetConfAsync();
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.IsAny<GetConfRequest>(),
                It.IsAny<ClientInterceptorContext<GetConfRequest, CacheConfiguration>>(),
                It.IsAny<AsyncUnaryCallContinuation<GetConfRequest, CacheConfiguration>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetPartitionStatsAsync()
        {
            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is<GetPartitionStatsRequest>(p => p.PartitionKey == "dynamo"),
                    It.IsAny<ClientInterceptorContext<GetPartitionStatsRequest, PartitionStats>>(),
                    It.IsAny<AsyncUnaryCallContinuation<GetPartitionStatsRequest, PartitionStats>>()))
               .Returns(() => new AsyncUnaryCall<PartitionStats>(
                   Task.FromResult(new PartitionStats()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.GetPartitionStatsAsync("dynamo");
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is<GetPartitionStatsRequest>(p => p.PartitionKey == "dynamo"),
                It.IsAny<ClientInterceptorContext<GetPartitionStatsRequest, PartitionStats>>(),
                It.IsAny<AsyncUnaryCallContinuation<GetPartitionStatsRequest, PartitionStats>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetStatsAsync()
        {
            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.IsAny<GetStatsRequest>(),
                    It.IsAny<ClientInterceptorContext<GetStatsRequest, CacheStats>>(),
                    It.IsAny<AsyncUnaryCallContinuation<GetStatsRequest, CacheStats>>()))
               .Returns(() => new AsyncUnaryCall<CacheStats>(
                   Task.FromResult(new CacheStats()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.GetStatsAsync();
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.IsAny<GetStatsRequest>(),
                It.IsAny<ClientInterceptorContext<GetStatsRequest, CacheStats>>(),
                It.IsAny<AsyncUnaryCallContinuation<GetStatsRequest, CacheStats>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task InvokeGCAsync()
        {
            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.IsAny<InvokeGCRequest>(),
                    It.IsAny<ClientInterceptorContext<InvokeGCRequest, InvokeGCResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<InvokeGCRequest, InvokeGCResponse>>()))
               .Returns(() => new AsyncUnaryCall<InvokeGCResponse>(
                   Task.FromResult(new InvokeGCResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.InvokeGCAsync();
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.IsAny<InvokeGCRequest>(),
                It.IsAny<ClientInterceptorContext<InvokeGCRequest, InvokeGCResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<InvokeGCRequest, InvokeGCResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PingAsync()
        {
            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.IsAny<PingRequest>(),
                    It.IsAny<ClientInterceptorContext<PingRequest, PongResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PingRequest, PongResponse>>()))
               .Returns(() => new AsyncUnaryCall<PongResponse>(
                   Task.FromResult(new PongResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.PingAsync();
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.IsAny<PingRequest>(),
                It.IsAny<ClientInterceptorContext<PingRequest, PongResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PingRequest, PongResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutAsync()
        {
            Expression<Func<PutRequest, bool>> expectedEntry =
                x => x.Key == "ntesla"
                && x.Value == "scientist".ToByteString();

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<PutRequest, PutResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutRequest, PutResponse>>()))
               .Returns(() => new AsyncUnaryCall<PutResponse>(
                   Task.FromResult(new PutResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.PutAsync("dynamo", "ntesla", "scientist".ToByteString());
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutRequest, PutResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutRequest, PutResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutManyAsync()
        {
            Expression<Func<PutManyRequest, bool>> expectedEntry =
                x => x.Entries["ntesla"] == "scientist".ToByteString()
                && x.Entries["jbramah"] == "inventor".ToByteString();

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<PutManyRequest, PutManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutManyRequest, PutManyResponse>>()))
                .Returns(() => new AsyncUnaryCall<PutManyResponse>(
                   Task.FromResult(new PutManyResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.PutAsync("dynamo",
                    new Dictionary<string, ByteString>
                    {
                        { "ntesla", "scientist".ToByteString() },
                        { "jbramah", "inventor".ToByteString() }
                    });
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutManyRequest, PutManyResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutManyRequest, PutManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfExistsAsync()
        {
            Expression<Func<PutIfExistsRequest, bool>> expectedEntry =
                x => x.Key == "ntesla"
                && x.Value == "scientist".ToByteString();

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<PutIfExistsRequest, PutIfExistsResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutIfExistsRequest, PutIfExistsResponse>>()))
               .Returns(() => new AsyncUnaryCall<PutIfExistsResponse>(
                   Task.FromResult(new PutIfExistsResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.PutIfExistsAsync("dynamo", "ntesla", "scientist".ToByteString());
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutIfExistsRequest, PutIfExistsResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutIfExistsRequest, PutIfExistsResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfExistsManyAsync()
        {
            Expression<Func<PutIfExistsManyRequest, bool>> expectedEntry =
                x => x.Entries["ntesla"] == "scientist".ToByteString()
                && x.Entries["jbramah"] == "inventor".ToByteString();

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<PutIfExistsManyRequest, PutIfExistsManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutIfExistsManyRequest, PutIfExistsManyResponse>>()))
                .Returns(() => new AsyncUnaryCall<PutIfExistsManyResponse>(
                   Task.FromResult(new PutIfExistsManyResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.PutIfExistsAsync("dynamo",
                    new Dictionary<string, ByteString>
                    {
                        { "ntesla", "scientist".ToByteString() },
                        { "jbramah", "inventor".ToByteString() }
                    });
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutIfExistsManyRequest, PutIfExistsManyResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutIfExistsManyRequest, PutIfExistsManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfNotExistsAsync()
        {
            Expression<Func<PutIfNotExistsRequest, bool>> expectedEntry =
                x => x.Key == "ntesla"
                && x.Value == "scientist".ToByteString();

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<PutIfNotExistsRequest, PutIfNotExistsResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutIfNotExistsRequest, PutIfNotExistsResponse>>()))
               .Returns(() => new AsyncUnaryCall<PutIfNotExistsResponse>(
                   Task.FromResult(new PutIfNotExistsResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.PutIfNotExistsAsync("dynamo", "ntesla", "scientist".ToByteString());
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutIfNotExistsRequest, PutIfNotExistsResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutIfNotExistsRequest, PutIfNotExistsResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfNotExistsManyAsync()
        {
            Expression<Func<PutIfNotExistsManyRequest, bool>> expectedEntry =
                x => x.Entries["ntesla"] == "scientist".ToByteString()
                && x.Entries["jbramah"] == "inventor".ToByteString();

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<PutIfNotExistsManyRequest, PutIfNotExistsManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutIfNotExistsManyRequest, PutIfNotExistsManyResponse>>()))
                .Returns(() => new AsyncUnaryCall<PutIfNotExistsManyResponse>(
                   Task.FromResult(new PutIfNotExistsManyResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.PutIfNotExistsAsync("dynamo",
                    new Dictionary<string, ByteString>
                    {
                        { "ntesla", "scientist".ToByteString() },
                        { "jbramah", "inventor".ToByteString() }
                    });
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutIfNotExistsManyRequest, PutIfNotExistsManyResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutIfNotExistsManyRequest, PutIfNotExistsManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutPartitionAsync()
        {
            Expression<Func<PutPartitionRequest, bool>> expectedEntry =
                x => x.PartitionKey == "dynamo"
                && x.ExpirationType == ExpirationType.Absolute
                && x.Expiration.ToTimeSpan() == TimeSpan.FromHours(1)
                && !x.Persist;

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<PutPartitionRequest, PutPartitionResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<PutPartitionRequest, PutPartitionResponse>>()))
               .Returns(() => new AsyncUnaryCall<PutPartitionResponse>(
                   Task.FromResult(new PutPartitionResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.PutPartitionAsync(
                    "dynamo",
                    TimeSpan.FromHours(1),
                    ExpirationType.Absolute,
                    false,
                    EvictionPolicy.Lru,
                    10);
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutPartitionRequest, PutPartitionResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutPartitionRequest, PutPartitionResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task TtlAsync()
        {
            Expression<Func<TtlRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<TtlRequest, TtlResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<TtlRequest, TtlResponse>>()))
               .Returns(() => new AsyncUnaryCall<TtlResponse>(
                   Task.FromResult(new TtlResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.TtlAsync("dynamo", "ntesla");
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<TtlRequest, TtlResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<TtlRequest, TtlResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task TtlManyAsync()
        {
            Expression<Func<TtlManyRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Keys[0] == "ntesla" && x.Keys[1] == "jbramah";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<TtlManyRequest, TtlManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<TtlManyRequest, TtlManyResponse>>()))
               .Returns(() => new AsyncUnaryCall<TtlManyResponse>(
                   Task.FromResult(new TtlManyResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.TtlAsync("dynamo", new string[] { "ntesla", "jbramah" });
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<TtlManyRequest, TtlManyResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<TtlManyRequest, TtlManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task TouchAsync()
        {
            Expression<Func<TouchRequest, bool>> expectedEntry =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<TouchRequest, TouchResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<TouchRequest, TouchResponse>>()))
               .Returns(() => new AsyncUnaryCall<TouchResponse>(
                   Task.FromResult(new TouchResponse { Result = WriteResult.Success }), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.TouchAsync("dynamo", "ntesla");
                Assert.True(result.Success);
                Assert.Equal(WriteResult.Success, result.Result);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<TouchRequest, TouchResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<TouchRequest, TouchResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task TouchManyAsync()
        {
            Expression<Func<TouchManyRequest, bool>> expectedEntry =
                x => x.PartitionKey == "dynamo" && x.Keys[0] == "ntesla" && x.Keys[1] == "jbramah";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<TouchManyRequest, TouchManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<TouchManyRequest, TouchManyResponse>>()))
                .Returns(() => new AsyncUnaryCall<TouchManyResponse>(
                   Task.FromResult(new TouchManyResponse { Touched = 2 }), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.TouchAsync("dynamo", new string[] { "ntesla", "jbramah" });
                Assert.True(result.Success);
                Assert.Equal(2, result.Result);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<TouchManyRequest, TouchManyResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<TouchManyRequest, TouchManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task IncrementAsync()
        {
            Expression<Func<IncrementRequest, bool>> expectedEntry =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<IncrementRequest, IncrementResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<IncrementRequest, IncrementResponse>>()))
               .Returns(() => new AsyncUnaryCall<IncrementResponse>(
                   Task.FromResult(new IncrementResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.IncrementAsync("dynamo", "ntesla");
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<IncrementRequest, IncrementResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<IncrementRequest, IncrementResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task IncrementByAsync()
        {
            Expression<Func<IncrementByRequest, bool>> expectedEntry =
                 x => x.PartitionKey == "dynamo" && x.Key == "ntesla" && x.Value == 5;

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<IncrementByRequest, IncrementByResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<IncrementByRequest, IncrementByResponse>>()))
               .Returns(() => new AsyncUnaryCall<IncrementByResponse>(
                   Task.FromResult(new IncrementByResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.IncrementByAsync("dynamo", "ntesla", 5);
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<IncrementByRequest, IncrementByResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<IncrementByRequest, IncrementByResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DecrementAsync()
        {
            Expression<Func<DecrementRequest, bool>> expectedEntry =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<DecrementRequest, DecrementResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DecrementRequest, DecrementResponse>>()))
               .Returns(() => new AsyncUnaryCall<DecrementResponse>(
                   Task.FromResult(new DecrementResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.DecrementAsync("dynamo", "ntesla");
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<DecrementRequest, DecrementResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<DecrementRequest, DecrementResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DecrementByAsync()
        {
            Expression<Func<DecrementByRequest, bool>> expectedEntry =
                 x => x.PartitionKey == "dynamo" && x.Key == "ntesla" && x.Value == 5;

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedEntry),
                    It.IsAny<ClientInterceptorContext<DecrementByRequest, DecrementByResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DecrementByRequest, DecrementByResponse>>()))
               .Returns(() => new AsyncUnaryCall<DecrementByResponse>(
                   Task.FromResult(new DecrementByResponse()), null, null, null, null));

            await using (var cacheNodeChannel = CreateNodeChannel())
            {
                var result = await cacheNodeChannel.DecrementByAsync("dynamo", "ntesla", 5);
                Assert.True(result.Success);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<DecrementByRequest, DecrementByResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<DecrementByRequest, DecrementByResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        //[Fact]
        //public async Task ScanKeysAsync()
        //{
        //    Expression<Func<ScanKeysRequest, bool>> expectedRequest =
        //        x => x.PartitionKey == "dynamo" && x.Pattern == "*" && x.Count == 1;

        //    _mock
        //       .Setup(x => x.AsyncServerStreamingCall(
        //            It.Is(expectedRequest),
        //            It.IsAny<ClientInterceptorContext<ScanKeysRequest, ScanKeysResponse>>(),
        //            It.IsAny<AsyncServerStreamingCallContinuation<ScanKeysRequest, ScanKeysResponse>>()))
        //       .Returns(() => new AsyncServerStreamingCall<ScanKeysResponse>(
        //           new StubAsyncStreamReader<ScanKeysResponse>(new[] { new ScanKeysResponse() }), null, null, null, null)); ;

        //    using (var cacheNodeChannel = CreateNodeChannel())
        //    {
        //        await foreach (var _ in cacheNodeChannel.ScanKeysAsync("dynamo", "*", 1))
        //        { }
        //    }

        //    _mock.Verify(x => x.AsyncServerStreamingCall(
        //        It.Is(expectedRequest),
        //        It.IsAny<ClientInterceptorContext<ScanKeysRequest, ScanKeysResponse>>(),
        //        It.IsAny<AsyncServerStreamingCallContinuation<ScanKeysRequest, ScanKeysResponse>>()),
        //    Times.Once());

        //    _mock.VerifyNoOtherCalls();
        //}

        //[Fact]
        //public async Task ScanPartitionsAsync()
        //{
        //    Expression<Func<ScanPartitionsRequest, bool>> expectedRequest =
        //        x => x.Pattern == "*" && x.Count == 1;

        //    _mock
        //       .Setup(x => x.AsyncServerStreamingCall(
        //            It.Is(expectedRequest),
        //            It.IsAny<ClientInterceptorContext<ScanPartitionsRequest, ScanPartitionsResponse>>(),
        //            It.IsAny<AsyncServerStreamingCallContinuation<ScanPartitionsRequest, ScanPartitionsResponse>>()))
        //       .Returns(() => new AsyncServerStreamingCall<ScanPartitionsResponse>(
        //           new StubAsyncStreamReader<ScanPartitionsResponse>(new[] { new ScanPartitionsResponse() }), null, null, null, null));

        //    using (var cacheNodeChannel = CreateNodeChannel())
        //    {
        //        await foreach (var _ in cacheNodeChannel.ScanPartitionsAsync("*", 1))
        //        { }
        //    }

        //    _mock.Verify(x => x.AsyncServerStreamingCall(
        //        It.Is(expectedRequest),
        //        It.IsAny<ClientInterceptorContext<ScanPartitionsRequest, ScanPartitionsResponse>>(),
        //        It.IsAny<AsyncServerStreamingCallContinuation<ScanPartitionsRequest, ScanPartitionsResponse>>()),
        //    Times.Once());

        //    _mock.VerifyNoOtherCalls();
        //}

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
