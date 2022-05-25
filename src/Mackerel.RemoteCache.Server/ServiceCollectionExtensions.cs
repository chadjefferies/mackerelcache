using System;
using System.Diagnostics.CodeAnalysis;
using Mackerel.RemoteCache.Server.Eviction;
using Mackerel.RemoteCache.Server.Expiration;
using Mackerel.RemoteCache.Server.Persistence;
using Mackerel.RemoteCache.Server.Runtime;
using Mackerel.RemoteCache.Server.Statistics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Options;
using Quartz;

namespace Mackerel.RemoteCache.Server
{
    [ExcludeFromCodeCoverage]
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddCacheRuntime(this IServiceCollection services)
        {
            services.AddSingleton(sp => RuntimeStatistics.Create(sp.GetRequiredService<IOptions<CacheServerOptions>>(), sp.GetService<ISystemClock>()));
            services.AddStorage();
            services.AddSingleton<MemoryStore>();
            return services;
        }

        public static IServiceCollection AddStorage(this IServiceCollection services)
        {
            services.AddSingleton<IPartitionStorage>(sp => FileSystemPartitionStorage.Create(sp.GetRequiredService<IOptions<CacheServerOptions>>()));
            return services;
        }

        public static IServiceCollection AddQuartzScheduler(this IServiceCollection services, IConfiguration config)
        {
            services.Configure<QuartzOptions>(config.GetSection("Quartz"));
            services.AddQuartz(qtz =>
            {
                qtz.UseMicrosoftDependencyInjectionScopedJobFactory();
                qtz.UseSimpleTypeLoader();

                var statsJobKey = new JobKey(nameof(StatsJob));
                qtz.AddJob<StatsJob>(j => j
                    .WithIdentity(statsJobKey));

                var expJobKey = new JobKey(nameof(EagerExpirationJob));
                qtz.AddJob<EagerExpirationJob>(j => j
                    .WithIdentity(expJobKey));

                var compactJobKey = new JobKey(nameof(PartitionCompactionJob));
                qtz.AddJob<PartitionCompactionJob>(j => j
                    .WithIdentity(compactJobKey)
                    .StoreDurably());

                qtz.AddTrigger(t => t
                    .WithPriority(2)
                    .ForJob(statsJobKey)
                    .WithSimpleSchedule(x => x
                        .WithInterval(config.GetValue<TimeSpan>("CacheServer:StatsInterval"))
                        .RepeatForever()
                        .WithMisfireHandlingInstructionNowWithRemainingCount()));

                qtz.AddTrigger(t => t
                    .WithPriority(3)
                    .ForJob(expJobKey)
                    .WithSimpleSchedule(x => x
                        .WithInterval(config.GetValue<TimeSpan>("CacheServer:EagerExpirationInterval"))
                        .RepeatForever()
                        .WithMisfireHandlingInstructionNowWithRemainingCount()));
            });

            services.AddQuartzServer(opt =>
            {
                opt.WaitForJobsToComplete = true;
            });

            return services;
        }
    }
}
