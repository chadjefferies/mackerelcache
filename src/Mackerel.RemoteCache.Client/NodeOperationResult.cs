using System;

namespace Mackerel.RemoteCache.Client
{
    public readonly struct NodeOperationResult<T>
    {
        public string Node { get; }
        public Exception Exception { get; }
        public bool Success { get; }
        public T Result { get; }

        public NodeOperationResult(string node)
        {
            Node = node;
            Success = true;
            Exception = null;
            Result = default;
        }

        public NodeOperationResult(string node, Exception e)
        {
            Node = node;
            Success = false;
            Exception = e;
            Result = default;
        }

        public NodeOperationResult(string node, T result, Exception e)
        {
            Node = node;
            Success = false;
            Exception = e;
            Result = result;
        }

        public NodeOperationResult(string node, T result)
        {
            Node = node;
            Success = true;
            Exception = null;
            Result = result;
        }
    }

    public readonly struct NodeOperationResult
    {
        public string Node { get; }
        public Exception Exception { get; }
        public bool Success { get; }

        public NodeOperationResult(string node)
        {
            Node = node;
            Success = true;
            Exception = null;
        }

        public NodeOperationResult(string node, Exception e)
        {
            Node = node;
            Success = false;
            Exception = e;
        }
    }
}
