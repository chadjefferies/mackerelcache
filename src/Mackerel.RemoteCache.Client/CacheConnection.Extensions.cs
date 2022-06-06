using System.Collections.Generic;
using System.Linq;
using Grpc.Net.Client;
using Mackerel.RemoteCache.Client.Configuration;
using Mackerel.RemoteCache.Client.Encoding;
using Mackerel.RemoteCache.Client.Routing;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;
using static Mackerel.RemoteCache.Api.V1.MackerelCacheService;
using static Mackerel.RemoteCache.Api.V1.WatchService;

namespace Mackerel.RemoteCache.Client
{
    public partial class CacheConnection
    {
        /// <summary>
        /// Creates a new <see cref="ICacheConnection"/> from a connection string.
        /// </summary>
        public static ICacheConnection Create(string connectionString)
        {
            return Create(connectionString, CacheClientOptions.GetDefaultGrpcChannelOptions());
        }

        /// <summary>
        /// Creates a new <see cref="ICacheConnection"/> from a connection string.
        /// </summary>
        public static ICacheConnection Create(string connectionString, GrpcChannelOptions grpcConfig)
        {
            var options = CacheClientOptions.Parse(connectionString);
            return Create(options, grpcConfig);
        }

        /// <summary>
        /// Creates a new <see cref="ICacheConnection"/> from a configuration object.
        /// </summary>
        public static ICacheConnection Create(CacheClientOptions config, GrpcChannelOptions grpcConfig)
        {
            var nodeChannels = new List<CacheNodeChannelImpl>();

            foreach (var address in config.Endpoints.Distinct())
            {
                var uri = CacheClientOptions.ParseNode(address);
                var channel = GrpcChannel.ForAddress(uri, grpcConfig);
                var serviceClient = new ServiceClient(
                    new MackerelCacheServiceClient(channel),
                    new WatchServiceClient(channel),
                    new MaintenanceServiceClient(channel));

                nodeChannels.Add(new CacheNodeChannelImpl(config, uri.Authority, channel, serviceClient));
            }

            return new CacheConnection(config, nodeChannels);
        }

        /// <summary>
        /// Creates a new <see cref="ICache{T}"/> used to access the key-value operations of the cache.
        /// </summary>
        /// <param name="codec">The encoding/decoding object ot use for this cache reference.</param>
        /// <param name="hashFunction">The hash function to use for this cache reference.</param>
        /// <param name="router">The router to use for this cache reference.</param>
        public ICache<T> GetCache<T>(ICacheCodec<T> codec, IHashFunction hashFunction, IRouter router)
        {
            return new CacheImpl<T>(this, hashFunction, router, codec, Options);
        }
    }
}
