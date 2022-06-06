using System;
using System.Collections.Generic;
using System.Linq.Expressions;
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
using Mackerel.RemoteCache.Client.Tests.Util;
using Moq;
using Xunit;
using static Grpc.Core.Interceptors.Interceptor;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;
using static Mackerel.RemoteCache.Api.V1.MackerelCacheService;
using static Mackerel.RemoteCache.Api.V1.WatchService;

namespace Mackerel.RemoteCache.Client.Tests
{
    public class CacheTests
    {
        private readonly Mock<Interceptor> _mock;
        private readonly Mock<IAsyncStreamReader<ScanKeysResponse>> _mockStreamReader;

        public CacheTests()
        {
            _mock = new Mock<Interceptor>();
            _mockStreamReader = new Mock<IAsyncStreamReader<ScanKeysResponse>>();

            _mock
              .Setup(x => x.AsyncServerStreamingCall(
                  It.IsAny<ScanKeysRequest>(),
                  It.IsAny<ClientInterceptorContext<ScanKeysRequest, ScanKeysResponse>>(),
                  It.IsAny<AsyncServerStreamingCallContinuation<ScanKeysRequest, ScanKeysResponse>>()))
              .Returns(() => TestCalls.AsyncServerStreamingCall(
                    _mockStreamReader.Object,
                    Task.FromResult(new Metadata()),
                    () => Status.DefaultSuccess,
                    () => new Metadata(),
                    () => { }));
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
                   Task.FromResult(new DeleteResponse { Result = WriteResult.KeyDoesNotExist }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                var ex = await Assert.ThrowsAsync<CacheException>(() => cache.DeleteAsync("dynamo", "ntesla"));
                Assert.Equal(WriteResult.KeyDoesNotExist, ex.Result);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
               It.Is(expectedRequest),
               It.IsAny<ClientInterceptorContext<DeleteRequest, DeleteResponse>>(),
               It.IsAny<AsyncUnaryCallContinuation<DeleteRequest, DeleteResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DeleteAsync_NullKey()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("key", () => cache.DeleteAsync("dynamo", default(string)));
            }

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DeleteManyAsync()
        {
            Expression<Func<DeleteManyRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Keys[0] == "ntesla" && x.Keys[1] == "jbramah";

            _mock
                .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<DeleteManyRequest, DeleteManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DeleteManyRequest, DeleteManyResponse>>()))
                .Returns(() => new AsyncUnaryCall<DeleteManyResponse>(
                    Task.FromResult(new DeleteManyResponse() { Deleted = 2 }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                var result = await cache.DeleteAsync("dynamo", new[] { "ntesla", "jbramah" });
                Assert.Equal(2, result);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<DeleteManyRequest, DeleteManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DeleteManyRequest, DeleteManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DeleteManyAsync_NullKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("keys", () => cache.DeleteAsync("dynamo", default(string[])));
            }

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DeleteManyAsync_EmptyKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await cache.DeleteAsync("dynamo", Array.Empty<string>());
            }

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
                   Task.FromResult(new GetResponse { Value = ByteString.CopyFromUtf8("scientist") }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                var val = await cache.GetAsync("dynamo", "ntesla");
                Assert.Equal("scientist", val);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<GetRequest, GetResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<GetRequest, GetResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetAsync_NullKey()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("key", () => cache.GetAsync("dynamo", default(string)));
            }

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

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                var val = await cache.GetAsync("dynamo", new[] { "ntesla", "jbramah" });
                Assert.Empty(val);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
               It.Is(expectedRequest),
               It.IsAny<ClientInterceptorContext<GetManyRequest, GetManyResponse>>(),
               It.IsAny<AsyncUnaryCallContinuation<GetManyRequest, GetManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetManyAsync_NullKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("keys", () => cache.GetAsync("dynamo", default(string[])));
            }

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task GetManyAsync_EmptyKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                var val = await cache.GetAsync("dynamo", Array.Empty<string>());
                Assert.Empty(val);
            }

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

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await cache.PutAsync("dynamo", "ntesla", "scientist");
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutRequest, PutResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutRequest, PutResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutAsync_Null()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("key", () => cache.PutAsync("dynamo", default, ""));
                await Assert.ThrowsAsync<ArgumentNullException>("value", () => cache.PutAsync("dynamo", "", null));
            }

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

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await cache.PutAsync("dynamo", new[] {
                    new KeyValuePair<string, string>("ntesla", "scientist"),
                    new KeyValuePair<string, string>("jbramah", "inventor")
                });

            }

            _mock.Verify(x => x.AsyncUnaryCall(
               It.Is(expectedEntry),
               It.IsAny<ClientInterceptorContext<PutManyRequest, PutManyResponse>>(),
               It.IsAny<AsyncUnaryCallContinuation<PutManyRequest, PutManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutManyAsync_NullKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("items", () => cache.PutAsync("dynamo", default));
                await cache.PutAsync("dynamo", new[]
                {
                    new KeyValuePair<string, string>(null, null)
                });
            }

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutManyAsync_EmptyKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await cache.PutAsync("dynamo", new KeyValuePair<string, string>[] { });
            }

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
                   Task.FromResult(new PutIfExistsResponse { Result = WriteResult.KeyDoesNotExist }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                var ex = await Assert.ThrowsAsync<CacheException>(() => cache.PutIfExistsAsync("dynamo", "ntesla", "scientist"));
                Assert.Equal(WriteResult.KeyDoesNotExist, ex.Result);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutIfExistsRequest, PutIfExistsResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutIfExistsRequest, PutIfExistsResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfExistsAsync_NullKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("key", () => cache.PutIfExistsAsync("dynamo", default, ""));
                await Assert.ThrowsAsync<ArgumentNullException>("value", () => cache.PutIfExistsAsync("dynamo", "", null));
            }

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

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                await cache.PutIfExistsAsync("dynamo", new[]
                {
                    new KeyValuePair<string, string>("ntesla","scientist"),
                    new KeyValuePair<string, string>("jbramah", "inventor"),
                });
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutIfExistsManyRequest, PutIfExistsManyResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutIfExistsManyRequest, PutIfExistsManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfExistsManyAsync_NullKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("items", () => cache.PutIfExistsAsync("dynamo", default));
                await cache.PutIfExistsAsync("dynamo", new KeyValuePair<string, string>[]
                {
                    new KeyValuePair<string, string>(null, null)
                });
            }

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfExistsManyAsync_EmptyKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                await cache.PutIfExistsAsync("dynamo", new KeyValuePair<string, string>[] { });
            }

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
                   Task.FromResult(new PutIfNotExistsResponse { Result = WriteResult.Success }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await cache.PutIfNotExistsAsync("dynamo", "ntesla", "scientist");
            }

            _mock.Verify(x => x.AsyncUnaryCall(
               It.Is(expectedEntry),
               It.IsAny<ClientInterceptorContext<PutIfNotExistsRequest, PutIfNotExistsResponse>>(),
               It.IsAny<AsyncUnaryCallContinuation<PutIfNotExistsRequest, PutIfNotExistsResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfNotExistsAsync_Null()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("key", () => cache.PutIfNotExistsAsync("dynamo", default, ""));
                await Assert.ThrowsAsync<ArgumentNullException>("value", () => cache.PutIfNotExistsAsync("dynamo", "", null));
            }

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

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await cache.PutIfNotExistsAsync("dynamo", new[] {
                    new KeyValuePair<string, string>("ntesla", "scientist"),
                    new KeyValuePair<string, string>("jbramah", "inventor")
                });
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedEntry),
                It.IsAny<ClientInterceptorContext<PutIfNotExistsManyRequest, PutIfNotExistsManyResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<PutIfNotExistsManyRequest, PutIfNotExistsManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfNotExistsManyAsync_NullKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                await Assert.ThrowsAsync<ArgumentNullException>("items", () => cache.PutIfNotExistsAsync("dynamo", default));
                await cache.PutIfNotExistsAsync("dynamo", new KeyValuePair<string, string>[]
                {
                    new KeyValuePair<string, string>(null, null)
                });
            }

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task PutIfNotExistsManyAsync_EmptyKeys()
        {
            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await cache.PutIfNotExistsAsync("dynamo", new KeyValuePair<string, string>[] { });
            }

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task ScanKeysAsync()
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
                .Returns(new ScanKeysResponse
                {
                    Key = "k1",
                    Value = ByteString.CopyFromUtf8("v1")
                });

            await using var connection = new CacheConnection(new CacheClientOptions
            {
                SessionTimeoutMilliseconds = (int)TimeSpan.FromMinutes(1).TotalMilliseconds
            }, new[] { CreateNodeChannel() });

            int messagesScanned = 0;
            var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
            await foreach (var (Key, Value, Offset) in cache.ScanKeysAsync("dynamo", "*", 2, 0))
            {
                Assert.Equal("k1", Key);
                Assert.Equal("v1", Value);
                Assert.Equal(messagesScanned, Offset);
                messagesScanned++;
            }

            Assert.Equal(messagesSent, messagesScanned);
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
                   Task.FromResult(new TtlResponse { ValueMs = 1000 }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                var val = await cache.TtlAsync("dynamo", "ntesla");
                Assert.Equal(TimeSpan.FromSeconds(1), val);
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

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                var val = await cache.TtlAsync("dynamo", new[] { "ntesla", "jbramah" });
                Assert.Empty(val);
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
            Expression<Func<TouchRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<TouchRequest, TouchResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<TouchRequest, TouchResponse>>()))
               .Returns(() => new AsyncUnaryCall<TouchResponse>(
                   Task.FromResult(new TouchResponse() { Result = WriteResult.Success }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                await cache.TouchAsync("dynamo", "ntesla");
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<TouchRequest, TouchResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<TouchRequest, TouchResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task TouchManyAsync()
        {
            Expression<Func<TouchManyRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Keys[0] == "ntesla" && x.Keys[1] == "jbramah";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<TouchManyRequest, TouchManyResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<TouchManyRequest, TouchManyResponse>>()))
               .Returns(() => new AsyncUnaryCall<TouchManyResponse>(
                   Task.FromResult(new TouchManyResponse { Touched = 2 }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                var result = await cache.TouchAsync("dynamo", new[] { "ntesla", "jbramah" });
                Assert.Equal(2, result);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
               It.Is(expectedRequest),
               It.IsAny<ClientInterceptorContext<TouchManyRequest, TouchManyResponse>>(),
               It.IsAny<AsyncUnaryCallContinuation<TouchManyRequest, TouchManyResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task IncrementAsync()
        {
            Expression<Func<IncrementRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<IncrementRequest, IncrementResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<IncrementRequest, IncrementResponse>>()))
               .Returns(() => new AsyncUnaryCall<IncrementResponse>(
                   Task.FromResult(new IncrementResponse { Value = 1, Result = WriteResult.Success }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                var val = await cache.IncrementAsync("dynamo", "ntesla");
                Assert.Equal(1, val);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<IncrementRequest, IncrementResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<IncrementRequest, IncrementResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task IncrementByAsync()
        {
            Expression<Func<IncrementByRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla" && x.Value == 1;

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<IncrementByRequest, IncrementByResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<IncrementByRequest, IncrementByResponse>>()))
               .Returns(() => new AsyncUnaryCall<IncrementByResponse>(
                   Task.FromResult(new IncrementByResponse { Value = 1, Result = WriteResult.Success }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                var val = await cache.IncrementByAsync("dynamo", "ntesla", 1);
                Assert.Equal(1, val);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<IncrementByRequest, IncrementByResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<IncrementByRequest, IncrementByResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DecrementAsync()
        {
            Expression<Func<DecrementRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla";

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<DecrementRequest, DecrementResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DecrementRequest, DecrementResponse>>()))
               .Returns(() => new AsyncUnaryCall<DecrementResponse>(
                   Task.FromResult(new DecrementResponse { Value = -1, Result = WriteResult.Success }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                var val = await cache.DecrementAsync("dynamo", "ntesla");
                Assert.Equal(-1, val);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<DecrementRequest, DecrementResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<DecrementRequest, DecrementResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task DecrementByAsync()
        {
            Expression<Func<DecrementByRequest, bool>> expectedRequest =
                x => x.PartitionKey == "dynamo" && x.Key == "ntesla" && x.Value == 1;

            _mock
               .Setup(x => x.AsyncUnaryCall(
                    It.Is(expectedRequest),
                    It.IsAny<ClientInterceptorContext<DecrementByRequest, DecrementByResponse>>(),
                    It.IsAny<AsyncUnaryCallContinuation<DecrementByRequest, DecrementByResponse>>()))
               .Returns(() => new AsyncUnaryCall<DecrementByResponse>(
                   Task.FromResult(new DecrementByResponse { Value = -1, Result = WriteResult.Success }), null, null, null, null));

            await using (var connection = new CacheConnection(new CacheClientOptions(), new[] { CreateNodeChannel() }))
            {
                var cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new PartitionRouter());
                var val = await cache.DecrementByAsync("dynamo", "ntesla", 1);
                Assert.Equal(-1, val);
            }

            _mock.Verify(x => x.AsyncUnaryCall(
                It.Is(expectedRequest),
                It.IsAny<ClientInterceptorContext<DecrementByRequest, DecrementByResponse>>(),
                It.IsAny<AsyncUnaryCallContinuation<DecrementByRequest, DecrementByResponse>>()),
            Times.Once());

            _mock.VerifyNoOtherCalls();
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
