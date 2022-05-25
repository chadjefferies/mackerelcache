using Grpc.AspNetCore.Server;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Server
{
    internal class ConfigureGrpcServiceOptions : IConfigureOptions<GrpcServiceOptions>
    {
        private readonly IConfiguration _configuration;

        public ConfigureGrpcServiceOptions(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Configure(GrpcServiceOptions options)
        {
            _configuration.Bind("Grpc", options);
        }
    }
}
