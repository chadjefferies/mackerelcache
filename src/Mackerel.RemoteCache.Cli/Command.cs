using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Client;

namespace Mackerel.RemoteCache.Cli
{
    public class Command
    {
        public string Name { get; set; }
        public string[] Arguments { get; set; } = new string [0];
        public bool HasOptionalArgs { get; set; } = false;
        public bool IsLocal { get; set; } = false;
        public string Description { get; set; } = "Description unavailable.";
        public Func<ICacheConnection, ICache<string>, IReadOnlyList<string>, Task<(bool,bool)>> Execute { get; set; }
    }
}
