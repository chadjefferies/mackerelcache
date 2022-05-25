using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Mackerel.RemoteCache.Client.Util;

namespace Mackerel.RemoteCache.Client.Configuration
{
    public class CacheClientOptions
    {
        // TODO: this regex works, but needs some love
        internal static Regex _addressRegex = new Regex(@"(https?://)?([^:^/]*)(:\\d*)?(.*)?", RegexOptions.IgnoreCase | RegexOptions.Singleline);

        internal const string DefaultPort = ":11211";
        internal const string DefaultScheme = "http://";
        internal const string Timeout = "timeout";
        internal const string Session = "sessionTimeout";

        public int TimeoutMilliseconds { get; set; } = 10000;
        public int SessionTimeoutMilliseconds { get; set; } = 15000;
        public IList<string> Endpoints { get; set; } = new List<string>();

        private static readonly IReadOnlyDictionary<string, string> _options = new[]
        {
            Timeout,
            Session,
        }.ToDictionary(x => x, StringComparer.OrdinalIgnoreCase);

        internal static string ParseOption(string option)
        {
            if (!string.IsNullOrWhiteSpace(option) && _options.TryGetValue(option, out var result))
            {
                return result;
            }

            return string.Empty;
        }

        public static CacheClientOptions Parse(string connectionString)
        {
            CachePreconditions.CheckNotNull(connectionString, nameof(connectionString));
            var options = new CacheClientOptions();
            var arr = connectionString.Split(',');

            foreach (var paddedOption in arr)
            {
                var option = paddedOption.Trim();

                if (string.IsNullOrWhiteSpace(option)) continue;

                int idx = option.IndexOf('=');
                if (idx > 0)
                {
                    var key = option.Substring(0, idx).Trim();
                    var value = option.Substring(idx + 1).Trim();

                    switch (ParseOption(key))
                    {
                        case Timeout:
                            options.TimeoutMilliseconds = Convert.ToInt32(value);
                            break;
                        case Session:
                            options.SessionTimeoutMilliseconds = Convert.ToInt32(value);
                            break;
                    }
                }
                else
                {
                    var node = ParseNode(option)?.ToString();
                    if (node != null && !options.Endpoints.Contains(node))
                    {
                        options.Endpoints.Add(node);
                    }
                }
            }
            return options;
        }

        internal static Uri ParseNode(string address)
        {
            if (!string.IsNullOrEmpty(address))
            {
                var match = _addressRegex.Match(address);
                if (match.Success)
                {
                    var scheme = (string.IsNullOrWhiteSpace(match.Groups[1].Value)) ? DefaultScheme : match.Groups[1].Value;
                    var host = match.Groups[2].Value;
                    var port = (string.IsNullOrWhiteSpace(match.Groups[4].Value)) ? DefaultPort : match.Groups[4].Value;

                    return new Uri($"{scheme}{host}{port}", UriKind.Absolute);
                }
            }

            return null;
        }

        internal static GrpcChannelOptions GetDefaultGrpcChannelOptions()
        {
            var defaultMethodConfig = new MethodConfig
            {
                Names = { MethodName.Default },
                RetryPolicy = new RetryPolicy
                {
                    MaxAttempts = 5,
                    InitialBackoff = TimeSpan.FromSeconds(1),
                    MaxBackoff = TimeSpan.FromSeconds(5),
                    BackoffMultiplier = 1.5,
                    RetryableStatusCodes = 
                    { 
                        StatusCode.Unavailable, 
                        StatusCode.Aborted, 
                        StatusCode.DeadlineExceeded,
                        StatusCode.Internal
                    }
                }
            };

            return new GrpcChannelOptions
            {
                ServiceConfig = new ServiceConfig
                {
                    MethodConfigs =
                    {
                        defaultMethodConfig
                    }
                }
            };
        }
    }
}
