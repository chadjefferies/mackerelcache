using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Hosting;
using NLog.Extensions.Logging;

namespace Mackerel.RemoteCache.Server
{
    [ExcludeFromCodeCoverage]
    class Program
    {
        static async Task Main(string[] args)
        {
            IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                })
                .ConfigureLogging((context, builder) =>
                {
                    var config = context.Configuration.GetSection("NLog");
                    NLog.LogManager.Configuration = new NLogLoggingConfiguration(config);
                    builder.ClearProviders();
                })
                .UseNLog()
                .UseWindowsService()
                .Build();

                await host.RunAsync();
        }
    }
}
