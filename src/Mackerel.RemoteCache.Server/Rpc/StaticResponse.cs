using System.Threading.Tasks;
using Mackerel.RemoteCache.Api.V1;

namespace Mackerel.RemoteCache.Server.Rpc
{
    public static class StaticResponse
    {
        public static readonly Task<PongResponse> Pong = Task.FromResult(new PongResponse { Result = "PONG" });
        public static readonly Task<FlushPartitionResponse> FlushPartition = Task.FromResult(new FlushPartitionResponse());
        public static readonly Task<FlushAllResponse> FlushAll = Task.FromResult(new FlushAllResponse());
        public static readonly Task<InvokeGCResponse> InvokeGC = Task.FromResult(new InvokeGCResponse());
        public static readonly Task<GetResponse> EmptyGet = Task.FromResult(new GetResponse());
    }
}
