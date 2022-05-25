using System;
using Grpc.Net.Client;
using Mackerel.RemoteCache.Client.Configuration;
using Mackerel.RemoteCache.Client.Encoding;
using Mackerel.RemoteCache.Client.Extensions.Internal;
using Mackerel.RemoteCache.Client.Routing;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Client.Extensions
{
    /// <summary>
    /// Extension methods for setting up Mackerel remote cache related services in an <see cref="IServiceCollection" />.
    /// </summary>
    public static class RemoteCacheServiceCollectionExtensions
    {
        /// <summary>
        /// Adds a <see cref="CacheClientOptions" /> to the specified <see cref="IServiceCollection" />. 
        /// </summary>
        /// <remarks>
        /// Calling <see cref="AddRemoteCache{T}(IServiceCollection)"/> automatically adds cache options. 
        /// </remarks>
        /// <returns></returns>
        public static IServiceCollection AddCacheClientOptions(this IServiceCollection services, Action<CacheClientOptions> setupClientAction, Action<GrpcChannelOptions> setupGrpcAction)
        {
            services.AddOptions();
            services.TryAddTransient<IConfigureOptions<CacheClientOptions>, ConfigureCacheClientOptions>();
            services.TryAddTransient<IConfigureOptions<GrpcChannelOptions>, ConfigureGrpcChannelOptions>();
            if (setupClientAction != null)
                services.PostConfigure(setupClientAction);
            if (setupGrpcAction != null)
                services.PostConfigure(setupGrpcAction);
            return services;
        }


        /// <summary>
        /// Adds a <see cref="ICacheConnection"/> to the specified <see cref="IServiceCollection" />.
        /// </summary>
        /// <remarks>
        /// Calling <see cref="AddRemoteCache{T}(IServiceCollection)"/> automatically adds an <see cref="ICacheConnection"/>. 
        /// </remarks>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddRemoteCacheConnection(this IServiceCollection services, Action<CacheClientOptions> setupClientAction, Action<GrpcChannelOptions> setupGrpcAction)
        {
            services.AddCacheClientOptions(setupClientAction, setupGrpcAction);
            services.TryAddSingleton(sp =>
            {
                var options = sp.GetRequiredService<IOptions<CacheClientOptions>>();
                var grpcConfig = sp.GetService<IOptions<GrpcChannelOptions>>();
                var logger = sp.GetService<ILogger<ICacheConnection>>();
                var connection = CacheConnection.Create(options.Value, grpcConfig?.Value ?? CacheClientOptions.GetDefaultGrpcChannelOptions());
                if (logger != null)
                    connection.ErrorHandler += logger.CacheError;
                return connection;
            });
            return services;
        }

        /// <summary>
        /// Adds a <see cref="ICacheConnection"/> to the specified <see cref="IServiceCollection" />.
        /// </summary>
        /// <remarks>
        /// Calling <see cref="AddRemoteCache{T}(IServiceCollection)"/> automatically adds an <see cref="ICacheConnection"/>. 
        /// </remarks>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddRemoteCacheConnection(this IServiceCollection services, Action<CacheClientOptions> setupAction)
        {
            services.AddRemoteCacheConnection(setupAction, _ => { });
            return services;
        }

        /// <summary>
        /// Adds a <see cref="ICacheConnection"/> to the specified <see cref="IServiceCollection" />.
        /// </summary>
        /// <remarks>
        /// Calling <see cref="AddRemoteCache{T}(IServiceCollection)"/> automatically adds an <see cref="ICacheConnection"/>. 
        /// </remarks>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddRemoteCacheConnection(this IServiceCollection services)
        {
            services.AddRemoteCacheConnection(_ => { }, _ => { });
            return services;
        }


        /// <summary>
        /// Adds a <see cref="ICache{T}"/> to the specified <see cref="IServiceCollection" />.
        /// </summary>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddRemoteCache<T>(this IServiceCollection services, Action<CacheClientOptions> setupClientAction, Action<GrpcChannelOptions> setupGrpcAction)
        {
            services.AddRemoteCacheConnection(setupClientAction, setupGrpcAction);
            services.TryAddSingleton<IHashFunction, ConsistentHashFunction>();
            services.TryAddSingleton<IRouter, KeyRouter>();
            services.TryAddSingleton<ICache<T>>(sp =>
            {
                var options = sp.GetRequiredService<IOptions<CacheClientOptions>>();
                var conn = sp.GetRequiredService<ICacheConnection>();
                var hashFunction = sp.GetRequiredService<IHashFunction>();
                var router = sp.GetRequiredService<IRouter>();
                var codec = sp.GetRequiredService<ICacheCodec<T>>();
                return new CacheImpl<T>(conn, hashFunction, router, codec, options.Value);
            });
            return services;
        }

        /// <summary>
        /// Adds a <see cref="ICache{T}"/> to the specified <see cref="IServiceCollection" />.
        /// </summary>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddRemoteCache<T>(this IServiceCollection services, Action<CacheClientOptions> setupAction)
        {
            services.AddRemoteCache<T>(setupAction, _ => { });
            return services;
        }

        /// <summary>
        /// Adds a <see cref="ICache{T}"/> to the specified <see cref="IServiceCollection" />.
        /// </summary>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddRemoteCache<T>(this IServiceCollection services)
        {
            services.AddRemoteCache<T>(_ => { }, _ => { });
            return services;
        }


        /// <summary>
        /// Adds an implementation of <see cref="IDistributedCache" /> that connects to a Mackerel remote caching cluster.
        /// </summary>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddMackerelRemoteDistributedCache(this IServiceCollection services, Action<MackerelDistributedCacheOptions> setupAction, Action<CacheClientOptions> setupClientAction, Action<GrpcChannelOptions> setupGrpcAction)
        {
            services.AddMemoryCache();
            services.AddSingleton<ISystemClock, SystemClock>();
            services.AddRemoteCacheConnection(setupClientAction, setupGrpcAction);
            services.AddSingleton<ICacheCodec<byte[]>, BinaryCacheCodec>();
            services.AddRemoteCache<byte[]>();
            services.TryAddTransient<IConfigureOptions<MackerelDistributedCacheOptions>, ConfigureMackerelDistributedCacheOptions>();
            if (setupAction != null)
                services.PostConfigure(setupAction);
            services.AddSingleton<IDistributedCache, MackerelDistributedCache>();
            return services;
        }

        /// <summary>
        /// Adds an implementation of <see cref="IDistributedCache" /> that connects to a Mackerel remote caching cluster.
        /// </summary>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddMackerelRemoteDistributedCache(this IServiceCollection services, Action<MackerelDistributedCacheOptions> setupAction, Action<CacheClientOptions> setupClientAction)
        {
            services.AddMackerelRemoteDistributedCache(setupAction, setupClientAction, _ => { });
            return services;
        }

        /// <summary>
        /// Adds an implementation of <see cref="IDistributedCache" /> that connects to a Mackerel remote caching cluster.
        /// </summary>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddMackerelRemoteDistributedCache(this IServiceCollection services, Action<MackerelDistributedCacheOptions> setupAction)
        {
            services.AddMackerelRemoteDistributedCache(setupAction, _ => { }, _ => { });
            return services;
        }

        /// <summary>
        /// Adds an implementation of <see cref="IDistributedCache" /> that connects to a Mackerel remote caching cluster.
        /// </summary>
        /// <returns>The <see cref="IServiceCollection"/> so that additional calls can be chained.</returns>
        public static IServiceCollection AddMackerelRemoteDistributedCache(this IServiceCollection services)
        {
            services.AddMackerelRemoteDistributedCache(_ => { }, _ => { }, _ => { });
            return services;
        }
    }
}