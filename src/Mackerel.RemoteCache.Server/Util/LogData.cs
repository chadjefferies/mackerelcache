using System;
using System.Collections;
using System.Collections.Generic;

namespace Mackerel.RemoteCache.Server.Util
{
    public class LogData : IEnumerable<KeyValuePair<string, object>>
    {
        private readonly List<KeyValuePair<string, object>> _properties;

        public string Message { get; }

        public LogData(string message, params (string, object)[] properties)
        {
            Message = message;
            _properties = new List<KeyValuePair<string, object>>();
            foreach (var prop in properties)
            {
                _properties.Add(new KeyValuePair<string, object>(prop.Item1, prop.Item2));
            }
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator() => _properties.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void AddProperty(string name, object value)
        {
            _properties.Add(new KeyValuePair<string, object>(name, value));
        }

        public static Func<LogData, Exception, string> Formatter { get; } = (l, e) => l.Message;
    }
}
