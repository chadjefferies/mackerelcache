using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Mackerel.RemoteCache.Server.Tests.Util
{
    public class MockAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        private int _idx = -1;
        private readonly IReadOnlyList<T> _responses;

        public T Current => _responses[_idx];


        public MockAsyncStreamReader(IReadOnlyList<T> responses)
        {
            _responses = responses;
        }

        public void Dispose() { }

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            _idx++;
            await Task.CompletedTask;
            return (_responses.Count > _idx);
        }
    }
}
