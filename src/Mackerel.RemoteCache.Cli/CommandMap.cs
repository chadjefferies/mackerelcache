using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Api.V1;
using Mackerel.RemoteCache.Client;

namespace Mackerel.RemoteCache.Cli
{
    public class CommandMap
    {
        public readonly IDictionary<string, Command> Commands;

        public CommandMap()
        {
            Commands = new SortedDictionary<string, Command>(StringComparer.OrdinalIgnoreCase)
            {
                {
                    "PING",
                    new Command
                    {
                        Name = "PING",
                        Description = "Ping available cache nodes.",
                        Execute = async (conn, cache, args) =>
                        {
                            foreach (var node in conn.GetNodes())
                            {
                                var result = await node.PingAsync();
                                Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                            }

                            return await Task.FromResult((true, true));
                        }
                    }
                },
                {
                    "GET",
                    new Command
                    {
                        Name = "GET",
                        Arguments = new[] { "partition", "key", "[key key ...]" },
                        HasOptionalArgs = true,
                        Description = "Gets the value of a key.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count > 1)
                            {
                                if(args.Count == 2)
                                {
                                    var result = await cache.GetAsync(args[0], args[1]);
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                }
                                else 
                                {
                                    var keys = new List<string>();
                                    for (int i = 1; i < args.Count; i++)
                                    {
                                        keys.Add(args[i]);
                                    }
                                    var result = await cache.GetAsync(args[0], keys);
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                }
                                return await Task.FromResult((true, true));

                            }
                            else
                            {
                                return await Task.FromResult((false, false));
                            }
                        }
                    }
                },
                {
                    "PUT",
                    new Command
                    {
                        Name = "PUT",
                        Arguments = new[] { "partition", "key value", "[key value ...]" },
                        HasOptionalArgs = true,
                        Description = "Sets the value of a key.",
                        Execute = async (conn, cache, args) =>
                        {
                            if ((args.Count - 1) % 2 == 0)
                            {
                                if (args.Count == 3)
                                {
                                    await cache.PutAsync(args[0], args[1], args[2]);
                                    Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                }
                                else
                                {
                                    var kvs = new List<KeyValuePair<string, string>>();
                                    for (int i = 1; i < args.Count; i+=2)
                                    {
                                        kvs.Add(new KeyValuePair<string, string>(args[i], args[i+1]));
                                    }
                                    await cache.PutAsync(args[0], kvs);
                                    Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                }                                
                                return await Task.FromResult((true, true));
                            }
                            else
                            {
                                return await Task.FromResult((false, false));
                            }
                        }
                    }
                },
                {
                    "PUTEX",
                    new Command
                    {
                        Name = "PUTEX",
                        Arguments = new[] { "partition", "key value", "[key value ...]" },
                        HasOptionalArgs = true,
                        Description = "Sets the value of a key only if it exists.",
                        Execute = async (conn, cache, args) =>
                        {
                            if ((args.Count - 1) % 2 == 0)
                            {
                                if (args.Count == 3)
                                {
                                    await cache.PutIfExistsAsync(args[0], args[1], args[2]);
                                    Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                }
                                else
                                {
                                    var kvs = new List<KeyValuePair<string, string>>();
                                    for (int i = 1; i < args.Count; i+=2)
                                    {
                                        kvs.Add(new KeyValuePair<string, string>(args[i], args[i+1]));
                                    }
                                    await cache.PutIfExistsAsync(args[0], kvs);
                                    Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                }
                                return await Task.FromResult((true, true));
                            }
                            else
                            {
                                return await Task.FromResult((false, false));
                            }
                        }
                    }
                },
                {
                    "PUTNX",
                    new Command
                    {
                        Name = "PUTNX",
                        Arguments = new[] { "partition", "key value", "[key value ...]" },
                        HasOptionalArgs = true,
                        Description = "Sets the value of a key only if it does not exist.",
                        Execute = async (conn, cache, args) =>
                        {
                            if ((args.Count - 1) % 2 == 0)
                            {
                                if (args.Count == 3)
                                {
                                    await cache.PutIfNotExistsAsync(args[0], args[1], args[2]);
                                    Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                }
                                else
                                {
                                    var kvs = new List<KeyValuePair<string, string>>();
                                    for (int i = 1; i < args.Count; i+=2)
                                    {
                                        kvs.Add(new KeyValuePair<string, string>(args[i], args[i+1]));
                                    }
                                    await cache.PutIfNotExistsAsync(args[0], kvs);
                                    Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                }
                                return await Task.FromResult((true, true));
                            }
                            else
                            {
                                return await Task.FromResult((false, false));
                            }
                        }
                    }
                },
                {
                    "PPUT",
                    new Command
                    {
                        Name = "PPUT",
                        Arguments = new[] { "partition", "cacheSize", "expiration ([d.]hh:mm:ss)", "expiration_type (Absolute|Sliding)", "eviction_policy (Lru|NoEviction)", "persist_metadata (True|False)" },
                        Description = "Creates or updates a partition.",
                        Execute = async (conn, cache, args) =>
                        {
                            if(long.TryParse(args[1], out var cacheSize))
                            {
                                if(TimeSpan.TryParse(args[2], out var expiration))
                                {
                                    if(Enum.TryParse<ExpirationType>(args[3], true, out var expirationType))
                                    {
                                        if(Enum.TryParse<EvictionPolicy>(args[4], true, out var evictionPolicy))
                                        {
                                            if(bool.TryParse(args[5], out var persist))
                                            {
                                                await conn.PutPartitionAsync(args[0], expiration, expirationType, persist, evictionPolicy, cacheSize);
                                                Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                                return await Task.FromResult((true, true));
                                            }
                                        }
                                    }
                                }
                            }

                            return await Task.FromResult((false, false));
                        }
                    }
                },
                {
                    "DEL",
                    new Command
                    {
                        Name = "DEL",
                        Arguments = new[] { "partition", "key", "[key key ...]" },
                        HasOptionalArgs = true,
                        Description = "Deletes a key from the cache.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count > 1)
                            {
                                if(args.Count == 2)
                                {
                                    await cache.DeleteAsync(args[0], args[1]);
                                    Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                }
                                else
                                {
                                    var keys = new List<string>();
                                    for (int i = 1; i < args.Count; i++)
                                    {
                                        keys.Add(args[i]);
                                    }
                                    var result = await cache.DeleteAsync(args[0], keys);
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                }
                                return await Task.FromResult((true, true));
                            }
                            else
                            {
                                return await Task.FromResult((false, false));
                            }
                        }
                    }
                },
                {
                    "PDEL",
                    new Command
                    {
                        Name = "PDEL",
                        Arguments = new[] { "partition" },
                        Description = "Delete an entire partition from the cache.",
                        Execute = async (conn, cache, args) =>
                        {
                            await conn.DeletePartitionAsync(args[0]);
                            Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                            return await Task.FromResult((true, true));
                        }
                    }
                },
                {
                    "FLUSH",
                    new Command
                    {
                        Name = "FLUSH",
                        Arguments = new[] { "partition" },
                        Description = "Remove all keys from a partition.",
                        Execute = async (conn, cache, args) =>
                        {
                            await conn.FlushPartitionAsync(args[0]);
                            Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                            return await Task.FromResult((true, true));
                        }
                    }
                },
                {
                    "FLUSHALL",
                    new Command
                    {
                        Name = "FLUSHALL",
                        Description = "Remove all keys from all partitions.",
                        Execute = async (conn, cache, args) =>
                        {
                            foreach (var node in conn.GetNodes())
                            {
                                var result = await node.FlushAllAsync();
                                Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                            }

                            return await Task.FromResult((true, true));
                        }
                    }
                },
                {
                    "STATS",
                    new Command
                    {
                        Name = "STATS",
                        Arguments = new[] { "partition [OPTIONAL]" },
                        HasOptionalArgs = true,
                        Description = "Returns statistics for the cache.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count == 1 )
                            {
                                var result = await conn.GetPartitionStatsAsync(args[0]);
                                Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));

                                return await Task.FromResult((true, true));
                            }
                            else if(args.Count == 0)
                            {
                                var stats = await conn.GetStatsAsync();
                                Console.WriteLine(JsonSerializer.Serialize(stats, Constants.JsonSettings));

                                return await Task.FromResult((true, true));
                            }

                            return await Task.FromResult((false, false));

                        }
                    }
                },
                {
                    "NSTATS",
                    new Command
                    {
                        Name = "NSTATS",
                        Arguments = new[] { "partition [OPTIONAL]" },
                        HasOptionalArgs = true,
                        Description = "Returns statistics broken out for each node in the cache.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count == 1 )
                            {
                                foreach (var node in conn.GetNodes())
                                {
                                    var result = await node.GetPartitionStatsAsync(args[0]);
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                }

                                return await Task.FromResult((true, true));
                            }
                            else if (args.Count == 0)
                            {
                                foreach (var node in conn.GetNodes())
                                {
                                    var stats = await node.GetStatsAsync();
                                    Console.WriteLine(JsonSerializer.Serialize(stats, Constants.JsonSettings));
                                }

                                return await Task.FromResult((true, true));
                            }

                            return await Task.FromResult((false, false));

                        }
                    }
                },
                {
                    "CONFIG",
                    new Command
                    {
                        Name = "CONFIG",
                        Description = "Returns configuration info for the cache.",
                        Execute = async (conn, cache, args) =>
                        {
                            foreach (var node in conn.GetNodes())
                            {
                                var result = await node.GetConfAsync();
                                Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                            }

                            return await Task.FromResult((true, true));
                        }
                    }
                },
                {
                    "GC",
                    new Command
                    {
                        Name = "GC",
                        Description = "Forces a garbage collection run. ADVANCED USE ONLY",
                        Execute = async (conn, cache, args) =>
                        {
                            foreach (var node in conn.GetNodes())
                            {
                                var result = await node.InvokeGCAsync();
                                Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                            }

                            return await Task.FromResult((true, true));
                        }
                    }
                },
                {
                    "SCAN",
                    new Command
                    {
                        Name = "SCAN",
                        Arguments = new[] { "partition [OPTIONAL]", "pattern", "count", "offset" },
                        HasOptionalArgs = true,
                        Description = "Iterate the cache and return matches. If a partition is supplied, matching keys are returned, otherwise matching partitions are returned.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count == 2)
                            {
                                await foreach (var result in conn.ScanPartitionsAsync(args[0], Convert.ToInt32(args[1])))
                                {
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                }

                                return await Task.FromResult((true, true));

                            }
                            else if (args.Count == 4)
                            {
                                await foreach (var (Key, Value, Offset) in cache.ScanKeysAsync(args[0], args[1], Convert.ToInt32(args[2]), Convert.ToInt32(args[3])))
                                {
                                    Console.WriteLine("{");
                                    Console.WriteLine($"  \"Key\": {Key}");
                                    Console.WriteLine($"  \"Value\": {Value}");
                                    Console.WriteLine($"  \"Offset\": {Offset}");
                                    Console.WriteLine("}");
                                }

                                return await Task.FromResult((true, true));
                            }

                            return await Task.FromResult((false, false));
                        }
                    }
                },
                {
                    "WATCH",
                    new Command
                    {
                        Name = "WATCH",
                        Arguments = new[] { "partition", "key [OPTIONAL]", "[filters ... Write Delete Evict Expire)] [OPTIONAL]" },
                        HasOptionalArgs = true,
                        Description = "Watch for changes to keys.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count > 0)
                            {
                                var filters = new List<WatchEventType>();
                                string key = null;
                                for (int i = 1; i < args.Count ; i++)
                                {
                                    if(Enum.TryParse<WatchEventType>(args[i], true, out var f))
                                    {
                                        filters.Add(f);
                                    }
                                    else
                                    {
                                        if (key != null)
                                        {
                                            return await Task.FromResult((false, false));
                                        }

                                        key = args[i];
                                    }
                                }

                                await cache.WatchAsync(
                                    watchId: "cli-watch",
                                    partitionKey: args[0],
                                    key: key,
                                    filters,
                                    m => Console.WriteLine(JsonSerializer.Serialize(m, Constants.JsonSettings)));

                                Console.WriteLine("Press enter to end watch...");
                                Console.ReadLine();
                                await cache.CancelAsync("cli-watch", args[0]);
                                return await Task.FromResult((true, false));
                            }

                            return await Task.FromResult((false, false));
                        }
                    }
                },
                {
                    "TTL",
                    new Command
                    {
                        Name = "TTL",
                        Arguments = new[] { "partition", "key", "[key key ...]" },
                        HasOptionalArgs = true,
                        Description = "Returns the remaining time to live of a key.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count > 1)
                            {
                                if (args.Count == 2)
                                {
                                    var result = await cache.TtlAsync(args[0], args[1]);
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                }
                                else
                                {
                                    var keys = new List<string>();
                                    for (int i = 1; i < args.Count; i++)
                                    {
                                        keys.Add(args[i]);
                                    }
                                    var results = await cache.TtlAsync(args[0], keys);
                                    Console.WriteLine(JsonSerializer.Serialize(results, Constants.JsonSettings));
                                }
                                return await Task.FromResult((true, true));
                            }
                            else
                            {
                                return await Task.FromResult((false, false));
                            }
                        }
                    }
                },
                {
                    "TOUCH",
                    new Command
                    {
                        Name = "TOUCH",
                        Arguments = new[] { "partition", "key", "[key key ...]" },
                        HasOptionalArgs = true,
                        Description = "Updates the last access time of a key.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count > 1)
                            {
                                if (args.Count == 2 )
                                {
                                    await cache.TouchAsync(args[0], args[1]);
                                    Console.WriteLine(JsonSerializer.Serialize("OK", Constants.JsonSettings));
                                }
                                else
                                {
                                    var keys = new List<string>();
                                    for (int i = 1; i < args.Count; i++)
                                    {
                                        keys.Add(args[i]);
                                    }
                                    var result = await cache.TouchAsync(args[0], keys);
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                }
                                return await Task.FromResult((true, true));
                            }
                            else
                            {
                                return await Task.FromResult((false, false));
                            }
                        }
                    }
                },
                {
                    "INCR",
                    new Command
                    {
                        Name = "INCR",
                        Arguments = new[] { "partition", "key", "value" },
                        HasOptionalArgs = true,
                        Description = "Increments a number stored at a key.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count == 2)
                            {
                                var result = await cache.IncrementAsync(args[0], args[1]);
                                Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                return await Task.FromResult((true, true));
                            }
                            else if (args.Count == 3)
                            {
                                if (long.TryParse(args[2], out var value))
                                {
                                    var result =await cache.IncrementByAsync(args[0], args[1], value);
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                    return await Task.FromResult((true, true));
                                }
                            }

                            return await Task.FromResult((false, false));
                        }
                    }
                },
                {
                    "DECR",
                    new Command
                    {
                        Name = "DECR",
                        Arguments = new[] { "partition", "key", "value" },
                        HasOptionalArgs = true,
                        Description = "Decrements a number stored at a key.",
                        Execute = async (conn, cache, args) =>
                        {
                            if (args.Count == 2)
                            {
                                var result = await cache.DecrementAsync(args[0], args[1]);
                                Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                return await Task.FromResult((true, true));
                            }
                            else if (args.Count == 3)
                            {
                                if(long.TryParse(args[2], out var value))
                                {
                                    var result =await cache.DecrementByAsync(args[0], args[1], value);
                                    Console.WriteLine(JsonSerializer.Serialize(result, Constants.JsonSettings));
                                    return await Task.FromResult((true, true));
                                }
                            }

                            return await Task.FromResult((false, false));
                        }
                    }
                },
                {
                    "HELP",
                    new Command
                    {
                        Name = "HELP",
                        Arguments = new[] { "command [OPTIONAL]" },
                        HasOptionalArgs = true,
                        IsLocal = true,
                        Description = "Display help information.",
                        Execute = (conn, cache, args) =>
                        {
                            if (args.Count == 1)
                            {
                                PrintCommandHelp(Commands[args[0]]);
                            }
                            else
                            {
                                foreach (var item in Commands)
                                {
                                   PrintCommandHelp(item.Value);
                                }
                            }

                            return Task.FromResult((true, false));
                        }
                    }
                },
            };
        }

        public async Task MatchAsync(string input, ICacheConnection conn, ICache<string> cache)
        {
            if (!string.IsNullOrWhiteSpace(input))
            {
                var inputs = input.Split(' ');
                string command = inputs[0].ToLower().Trim();
                var arguments = new List<string>();
                if (inputs.Length > 1)
                {
                    for (int i = 1; i < inputs.Length; i++)
                    {
                        var trimmedValue = inputs[i].Trim();
                        if (!string.IsNullOrWhiteSpace(trimmedValue))
                        {
                            arguments.Add(trimmedValue);
                        }
                    }
                }

                Console.WriteLine();
                Console.ForegroundColor = Constants.OUTPUT_COLOR;
                if (Commands.TryGetValue(command, out var matchedCommand))
                {
                    if (!matchedCommand.IsLocal && conn == null)
                    {
                        throw new Exception("Not connected. Please issue a CONNECT command first.");
                    }
                    else
                    {
                        if (matchedCommand.HasOptionalArgs
                        || arguments.Count == matchedCommand.Arguments.Length)
                        {
                            var st = Stopwatch.StartNew();
                            var result = await matchedCommand.Execute(conn, cache, arguments);
                            if (!result.Item1)
                            {
                                Console.WriteLine("Bad syntax. Please refer to the following:");
                                PrintCommandHelp(matchedCommand);
                            }
                            if (result.Item2)
                            {
                                Console.WriteLine();
                                Console.ForegroundColor = Constants.OUTPUT_COLOR;
                                Console.WriteLine($"({st.ElapsedMilliseconds}ms)");
                                Console.ForegroundColor = Constants.INPUT_COLOR;
                            }
                        }
                        else
                        {
                            Console.WriteLine("Bad syntax. Please refer to the following:");
                            PrintCommandHelp(matchedCommand);
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Unknown command. Type HELP for more info or press TAB for auto complete.");
                }
                Console.WriteLine();
                Console.ForegroundColor = Constants.INPUT_COLOR;
            }
        }

        static void PrintCommandHelp(Command cmd)
        {
            Console.WriteLine();
            Console.Write(" ");
            Console.ForegroundColor = Constants.INPUT_COLOR;
            Console.Write(cmd.Name);
            Console.ForegroundColor = Constants.OUTPUT_COLOR;
            Console.Write(' ');
            Console.Write(string.Join(" ", cmd.Arguments));
            Console.WriteLine();
            Console.ForegroundColor = Constants.HIGHLIGHT_COLOR;
            Console.Write(" ");
            Console.Write("description: ");
            Console.ResetColor();
            Console.Write(cmd.Description);
            Console.ForegroundColor = Constants.INPUT_COLOR;
            Console.WriteLine();
        }
    }
}
