using Mackerel.RemoteCache.Client.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Client.Extensions.Internal
{
    internal class ConfigureCacheClientOptions : IConfigureOptions<CacheClientOptions>
    {
        private readonly IConfiguration _configuration;

        public ConfigureCacheClientOptions(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Configure(CacheClientOptions options)
        {
            _configuration.Bind("MackerelCache", options);
        }
    }
}
