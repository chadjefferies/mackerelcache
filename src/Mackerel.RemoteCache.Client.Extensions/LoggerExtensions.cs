using System;
using Microsoft.Extensions.Logging;

namespace Mackerel.RemoteCache.Client.Extensions
{
    public static class LoggerExtensions
    {
        private static readonly Action<ILogger, Exception> _cacheError;

        static LoggerExtensions()
        {
            _cacheError = LoggerMessage.Define(
                eventId: new EventId(1, "CacheError"),
                logLevel: LogLevel.Warning,
                formatString: "A cache error has occured");
        }

        public static void CacheError(this ILogger logger, Exception ex)
        {
            _cacheError(logger, ex);
        }
    }
}

