using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Mackerel.RemoteCache.Cli
{
    public static class Constants
    {
        public const ConsoleColor ERROR_COLOR = ConsoleColor.DarkRed;
        public const ConsoleColor INPUT_COLOR = ConsoleColor.White;
        public const ConsoleColor OUTPUT_COLOR = ConsoleColor.Gray;
        public const ConsoleColor HIGHLIGHT_COLOR = ConsoleColor.DarkYellow;

        public static JsonSerializerOptions JsonSettings = new JsonSerializerOptions
        {
            WriteIndented = true,
            IgnoreNullValues = true
        };

        static Constants()
        {
            JsonSettings.Converters.Add(new DurationConverter());
            JsonSettings.Converters.Add(new TimestampConverter());
            JsonSettings.Converters.Add(new JsonStringEnumConverter());
            JsonSettings.Converters.Add(new TimeSpanConverter());
        }
    }
}
