using System;
using System.Reflection;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Server.Util;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Mackerel.RemoteCache.Server.Statistics
{
    [DisallowConcurrentExecution]
    public class StatsJob : IJob
    {
        private static readonly PropertyInfo[] _props =
            typeof(RuntimeStatistics).GetProperties(BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

        private readonly ILogger<StatsJob> _logger;
        private readonly RuntimeStatistics _statistics;

        public StatsJob(ILogger<StatsJob> logger, RuntimeStatistics statistics)
        {
            _logger = logger;
            _statistics = statistics;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogTrace("Running the StatsJob");

                _statistics.Crunch();

                var logEvent = new LogData("Stats");
                for (int i = 0; i < _props.Length; i++)
                {
                    logEvent.AddProperty(_props[i].Name, _props[i].GetValue(_statistics, null));
                }

                _logger.Log(LogLevel.Information, default, logEvent, null, LogData.Formatter);

                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                throw new JobExecutionException(e, false);
            }
        }
    }
}
