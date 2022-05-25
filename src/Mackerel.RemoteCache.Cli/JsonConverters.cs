using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Mackerel.RemoteCache.Cli
{
    public class DurationConverter : JsonConverter<Google.Protobuf.WellKnownTypes.Duration>
    {
        public override Google.Protobuf.WellKnownTypes.Duration Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => throw new NotImplementedException();

        public override void Write(Utf8JsonWriter writer, Google.Protobuf.WellKnownTypes.Duration durationValue, JsonSerializerOptions options)
            => writer.WriteStringValue(durationValue.ToTimeSpan().ToString());
    }

    public class TimestampConverter : JsonConverter<Google.Protobuf.WellKnownTypes.Timestamp>
    {
        public override Google.Protobuf.WellKnownTypes.Timestamp Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => throw new NotImplementedException();

        public override void Write(Utf8JsonWriter writer, Google.Protobuf.WellKnownTypes.Timestamp timestampValue, JsonSerializerOptions options)
            => writer.WriteStringValue(timestampValue.ToDateTime().ToLocalTime().ToString("MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture));
    }

    // TODO: should be able to remove once upgrade to dotnet5 is complete
    // https://github.com/dotnet/runtime/issues/29932
    public class TimeSpanConverter : JsonConverter<TimeSpan>
    {
        public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => throw new NotImplementedException();

        public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options)
            => writer.WriteStringValue(value.ToString());
    }
}
