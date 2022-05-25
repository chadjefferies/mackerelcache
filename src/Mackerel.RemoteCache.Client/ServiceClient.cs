using static Mackerel.RemoteCache.Api.V1.WatchService;
using static Mackerel.RemoteCache.Api.V1.RemoteCacheService;
using static Mackerel.RemoteCache.Api.V1.MaintenanceService;

namespace Mackerel.RemoteCache.Client
{
    internal class ServiceClient
    {
        public RemoteCacheServiceClient Cache { get; }
        public WatchServiceClient Watch { get; }
        public MaintenanceServiceClient Maintenance { get; }

        public ServiceClient(
            RemoteCacheServiceClient cacheClient,
            WatchServiceClient watchClient,
            MaintenanceServiceClient maintenanceClient)
        {
            Cache = cacheClient;
            Watch = watchClient;
            Maintenance = maintenanceClient;
        }
    }
}
