using System;
using System.Linq;
using Grpc.Core;

namespace Mackerel.RemoteCache.Server.Rpc
{
    public static class GrpcRequestExtensions
    {
        public const string SESSION_TIMEOUT = "session_timeout";

        public static int GetSessionTimeout(this Metadata metadata)
        {
            var headerValue = metadata.SingleOrDefault(e => e.Key == SESSION_TIMEOUT);
            if (headerValue != null)
            {
                var sessionTimeout = Convert.ToInt32(metadata.SingleOrDefault(e => e.Key == SESSION_TIMEOUT).Value);
                // add a tolerance buffer
                return (int)Math.Ceiling(sessionTimeout * 1.5);
            }

            return 5000;
        }
    }
}
