using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Server
{
    public class ConfigureCacheServerOptions : IConfigureOptions<CacheServerOptions>
    {
        private readonly IConfiguration _configuration;

        public ConfigureCacheServerOptions(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Configure(CacheServerOptions options)
        {
            _configuration.Bind("CacheServer", options);
            options.Validate();
        }
    }
}
