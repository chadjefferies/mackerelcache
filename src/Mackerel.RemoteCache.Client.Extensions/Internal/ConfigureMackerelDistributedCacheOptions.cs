using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Client.Extensions.Internal
{
    internal class ConfigureMackerelDistributedCacheOptions : IConfigureOptions<MackerelDistributedCacheOptions>
    {
        private readonly IConfiguration _configuration;

        public ConfigureMackerelDistributedCacheOptions(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Configure(MackerelDistributedCacheOptions options)
        {
            _configuration.Bind("RemoteCache:DistributedCache", options);
        }
    }
}
