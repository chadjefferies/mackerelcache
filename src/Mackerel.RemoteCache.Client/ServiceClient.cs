using static Mackerel.RemoteCache.Api.V1.WatchService;
using static Mackerel.RemoteCache.Api.V1.MackerelCacheService;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;

namespace Mackerel.RemoteCache.Client
{
    internal class ServiceClient
    {
        public MackerelCacheServiceClient Cache { get; }
        public WatchServiceClient Watch { get; }
        public MaintenanceServiceClient Maintenance { get; }

        public ServiceClient(
            MackerelCacheServiceClient cacheClient,
            WatchServiceClient watchClient,
            MaintenanceServiceClient maintenanceClient)
        {
            Cache = cacheClient;
            Watch = watchClient;
            Maintenance = maintenanceClient;
        }
    }
}
