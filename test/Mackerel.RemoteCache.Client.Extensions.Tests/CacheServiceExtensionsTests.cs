using System.Linq;
using Mackerel.RemoteCache.Client.Encoding;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Mackerel.RemoteCache.Client.Extensions.Tests
{
    public class CacheServiceExtensionsTests
    {
        [Fact]
        public void AddRemoteCacheConnection_RegistersDistributedCacheAsSingleton()
        {
            var configuration = new ConfigurationBuilder()
                 .Build();
            var services = new ServiceCollection();
            services.AddScoped(typeof(IConfiguration), sp => configuration);
            services.AddRemoteCacheConnection();
            var connection = services.FirstOrDefault(desc => desc.ServiceType == typeof(ICacheConnection));
            var serviceProvider = services.BuildServiceProvider();

            Assert.NotNull(connection);
            Assert.Equal(ServiceLifetime.Singleton, connection.Lifetime);
            Assert.IsType<CacheConnection>(serviceProvider.GetRequiredService<ICacheConnection>());
        }

        [Fact]
        public void AddRemoteCacheConnection_DoesNotReplacePreviouslyUserRegisteredServices()
        {
            var configuration = new ConfigurationBuilder()
                 .Build();
            var services = new ServiceCollection();
            services.AddScoped(typeof(IConfiguration), sp => configuration);
            services.AddScoped(typeof(ILogger<ICacheConnection>), sp => Mock.Of<ILogger<ICacheConnection>>());
            services.AddScoped(typeof(ICacheConnection), sp => Mock.Of<ICacheConnection>());

            services.AddRemoteCacheConnection();

            var serviceProvider = services.BuildServiceProvider();
            var connection = services.FirstOrDefault(desc => desc.ServiceType == typeof(ICacheConnection));

            Assert.NotNull(connection);
            Assert.Equal(ServiceLifetime.Scoped, connection.Lifetime);
            Assert.IsNotType<CacheConnection>(serviceProvider.GetRequiredService<ICacheConnection>());
        }

        [Fact]
        public void AddRemoteCache_RegistersDistributedCacheAsSingleton()
        {
            var configuration = new ConfigurationBuilder()
                 .Build();
            var services = new ServiceCollection();
            services.AddScoped(typeof(IConfiguration), sp => configuration);
            services.AddSingleton<ICacheCodec<string>, StringCacheCodec>();
            services.AddRemoteCache<string>();
            var cache = services.FirstOrDefault(desc => desc.ServiceType == typeof(ICache<string>));
            var serviceProvider = services.BuildServiceProvider();

            Assert.NotNull(cache);
            Assert.Equal(ServiceLifetime.Singleton, cache.Lifetime);
            Assert.IsType<CacheImpl<string>>(serviceProvider.GetRequiredService<ICache<string>>());
        }

        [Fact]
        public void AddRemoteCache_DoesNotReplacePreviouslyUserRegisteredServices()
        {
            var configuration = new ConfigurationBuilder()
                 .Build();
            var services = new ServiceCollection();
            services.AddScoped(typeof(IConfiguration), sp => configuration);
            services.AddScoped(typeof(ILogger<ICacheConnection>), sp => Mock.Of<ILogger<ICacheConnection>>());
            services.AddScoped(typeof(ICache<string>), sp => Mock.Of<ICache<string>>());

            services.AddRemoteCache<string>();

            var serviceProvider = services.BuildServiceProvider();
            var connection = services.FirstOrDefault(desc => desc.ServiceType == typeof(ICache<string>));

            Assert.NotNull(connection);
            Assert.Equal(ServiceLifetime.Scoped, connection.Lifetime);
            Assert.IsNotType<CacheImpl<string>>(serviceProvider.GetRequiredService<ICache<string>>());
        }

        [Fact]
        public void AddMackerelRemoteDistributedCache_RegistersDistributedCacheAsSingleton()
        {
            var services = new ServiceCollection();
            services.AddMackerelRemoteDistributedCache();
            var distributedCache = services.FirstOrDefault(desc => desc.ServiceType == typeof(IDistributedCache));

            Assert.NotNull(distributedCache);
            Assert.Equal(ServiceLifetime.Singleton, distributedCache.Lifetime);
        }

        [Fact]
        public void AddMackerelRemoteDistributedCache_ReplacesPreviouslyUserRegisteredServices()
        {
            var configuration = new ConfigurationBuilder()
                 .Build();
            var services = new ServiceCollection();
            services.AddScoped(typeof(IConfiguration), sp => configuration);
            services.AddScoped(typeof(ILogger<ICacheConnection>), sp => Mock.Of<ILogger<ICacheConnection>>());
            services.AddScoped(typeof(ILogger<MackerelDistributedCache>), sp => Mock.Of<ILogger<MackerelDistributedCache>>());
            services.AddScoped(typeof(IDistributedCache), sp => Mock.Of<IDistributedCache>());

            services.AddMackerelRemoteDistributedCache();

            var serviceProvider = services.BuildServiceProvider();
            var distributedCache = services.FirstOrDefault(desc => desc.ServiceType == typeof(IDistributedCache));

            Assert.NotNull(distributedCache);
            Assert.Equal(ServiceLifetime.Scoped, distributedCache.Lifetime);
            Assert.IsType<MackerelDistributedCache>(serviceProvider.GetRequiredService<IDistributedCache>());
        }

        [Fact]
        public void AddMackerelRemoteDistributedCache_Allows_Chaining()
        {
            var services = new ServiceCollection();
            Assert.Same(services, services.AddMackerelRemoteDistributedCache());
        }
    }
}