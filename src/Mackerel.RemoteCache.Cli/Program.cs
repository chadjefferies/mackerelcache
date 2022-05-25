using System;
using System.Linq;
using System.Threading.Tasks;
using Mackerel.RemoteCache.Client;
using Mackerel.RemoteCache.Client.Encoding;
using Mackerel.RemoteCache.Client.Routing;

namespace Mackerel.RemoteCache.Cli
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ICacheConnection connection = default;
            ICache<string> cache = default;

            var commandMap = new CommandMap();
            // add "admin" commands here
            commandMap.Commands.Add("CONNECT", new Command
            {
                Name = "CONNECT",
                Arguments = new[] { "connectionString" },
                HasOptionalArgs = true,
                IsLocal = true,
                Description = "Connects using a connection string. If empty, defaults to localhost.",
                Execute = async (conn, c, localArgs) =>
                {
                    string connectionString = "localhost";
                    if (localArgs.Count == 1)
                    {
                        connectionString = localArgs[0];
                    }
                    if (connection != null) await connection.DisposeAsync();
                    connection = CacheConnection.Create(connectionString);
                    connection.ErrorHandler += PrintError;
                    cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
                     return await Task.FromResult((true, false));
                }
            });
            commandMap.Commands.Add("EXIT", new Command
            {
                Name = "EXIT",
                Description = "Disconnects and exits the inteface.",
                IsLocal = true,
                Execute = async (conn, cache, localArgs) =>
                {
                    Console.WriteLine("Shutting down...");
                    if (connection != null) await connection.DisposeAsync();
                    Environment.Exit(0);
                    return await Task.FromResult((true, false));
                }
            });
            commandMap.Commands.Add("CLEAR", new Command
            {
                Name = "CLEAR",
                Description = "Clears the console window.",
                IsLocal = true,
                Execute = (conn, cache, localArgs) =>
                {
                    Console.Clear();
                    return Task.FromResult((true, false));
                }
            });

            ReadLine.AutoCompletionHandler = new AutoCompletionHandler(commandMap);
            ReadLine.HistoryEnabled = true;

            PrintWelcomeMessage();

            if (args.Length == 1)
            {
                try
                {
                    await commandMap.MatchAsync("CONNECT", connection, cache);
                }
                catch (Exception e)
                {
                    PrintError(e);
                }
            }


            while (true)
            {
                try
                {
                    Console.ForegroundColor = Constants.INPUT_COLOR;
                    string prompt = GetPrompt(connection);
                    string command = ReadLine.Read(prompt).Trim();
                    Console.ForegroundColor = Constants.OUTPUT_COLOR;
                    await commandMap.MatchAsync(command, connection, cache);
                }
                catch (Exception e)
                {
                    PrintError(e);
                }
            }
        }
        static readonly object _lock = new object();
        static void PrintError(Exception e)
        {
            lock (_lock)
            {
                Console.WriteLine();
                Console.ForegroundColor = Constants.ERROR_COLOR;
                Console.Write(" ");
                Console.Write("ERROR: ");
                Console.ForegroundColor = Constants.INPUT_COLOR;
                Console.WriteLine(e.InnerException?.Message ?? e.Message);
                Console.ForegroundColor = Constants.OUTPUT_COLOR;
                Console.WriteLine();
            }
        }

        static void PrintWelcomeMessage()
        {
            Console.WriteLine("WELCOME to the Remote Cache CLI");
            Console.WriteLine("================================");
            Console.WriteLine();
            Console.WriteLine("type HELP for more info");
            Console.WriteLine();
        }

        static string GetPrompt(ICacheConnection connection)
        {
            string prompt = "(not connected)> ";

            if (connection != null)
            {
                var nodes = connection
                    .GetNodes()
                    .Select(x => x.Address)
                    .ToList();

                prompt = $"({nodes[0]}";
                if (nodes.Count > 1)
                {
                    prompt += $" +{nodes.Count - 1} more";
                }
                prompt += ")> ";
            }

            return prompt;
        }
    }
}
