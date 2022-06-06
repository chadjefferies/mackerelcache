using Grpc.Net.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Client.Extensions.Internal
{
    internal class ConfigureGrpcChannelOptions : IConfigureOptions<GrpcChannelOptions>
    {
        private readonly IConfiguration _configuration;

        public ConfigureGrpcChannelOptions(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Configure(GrpcChannelOptions options)
        {
            _configuration.Bind("MackerelCache:Grpc", options);
        }
    }
}
