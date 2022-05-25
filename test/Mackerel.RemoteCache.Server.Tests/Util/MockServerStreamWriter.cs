using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;

namespace Mackerel.RemoteCache.Server.Tests.Util
{
    public class MockServerStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Items { get; } = new List<T>();
        public WriteOptions WriteOptions { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public Task WriteAsync(T message)
        {
            Items.Add(message);
            return Task.CompletedTask;
        }
    }
}
