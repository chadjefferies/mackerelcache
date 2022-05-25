using System;
using System.Linq;

namespace Mackerel.RemoteCache.Cli
{
    class AutoCompletionHandler : IAutoCompleteHandler
    {
        private readonly CommandMap _commandMap;

        public char[] Separators { get; set; } = new char[] { ' ' };

        public AutoCompletionHandler(CommandMap commandMap)
        {
            _commandMap = commandMap;
        }

        public string[] GetSuggestions(string text, int index)
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                return _commandMap.Commands.Keys.ToArray();
            }

            return _commandMap.Commands
                .Where(x => x.Key.StartsWith(text, StringComparison.OrdinalIgnoreCase))
                .Select(x => x.Key)
                .ToArray();
        }
    }
}
