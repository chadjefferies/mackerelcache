using System;
using System.Diagnostics.CodeAnalysis;
using Grpc.AspNetCore.Server;
using Mackerel.RemoteCache.Server.Rpc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Options;

namespace Mackerel.RemoteCache.Server
{
    [ExcludeFromCodeCoverage]
    public class Startup
    {
        private readonly IConfiguration _config;

        public Startup(IConfiguration config)
        {
            _config = config;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.Configure<HostOptions>(opt => opt.ShutdownTimeout = TimeSpan.FromMinutes(1));
            services.AddSingleton<ISystemClock, SystemClock>();
            services.AddSingleton<IConfigureOptions<CacheServerOptions>, ConfigureCacheServerOptions>();
            services.AddSingleton<IConfigureOptions<GrpcServiceOptions>, ConfigureGrpcServiceOptions>();
            services.AddCacheRuntime();
            services.AddGrpc();
            services.AddGrpcReflection();
            services.AddQuartzScheduler(_config);
            services.AddHostedService<CacheHost>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGrpcService<MaintenanceServiceHandler>(); 
                endpoints.MapGrpcService<MackerelCacheServiceHandler>();
                endpoints.MapGrpcService<WatchServiceHandler>();
                endpoints.MapGrpcReflectionService();
            });
        }
    }
}
