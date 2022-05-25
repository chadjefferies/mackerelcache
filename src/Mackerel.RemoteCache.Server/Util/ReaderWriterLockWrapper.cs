using System;
using System.Threading;

namespace Mackerel.RemoteCache.Server.Util
{
    internal class ReaderWriterLockWrapper : IDisposable
    {
        private readonly ReaderWriterLockSlim _slimLock = new ReaderWriterLockSlim();

        internal WriteLock EnterWriteLock()
        {
            return new WriteLock(_slimLock);
        }

        internal ReadLock EnterReadLock()
        {
            return new ReadLock(_slimLock);
        }

        public void Dispose()
        {
            _slimLock.Dispose();
        }

        internal readonly ref struct ReadLock 
        {
            private readonly ReaderWriterLockSlim _slimLock;

            public ReadLock(ReaderWriterLockSlim slimLock)
            {
                slimLock.EnterReadLock();
                _slimLock = slimLock;
            }

            public void Dispose()
            {
                _slimLock.ExitReadLock();
            }
        }

        internal readonly ref struct WriteLock 
        {
            private readonly ReaderWriterLockSlim _slimLock;

            public WriteLock(ReaderWriterLockSlim slimLock)
            {
                slimLock.EnterWriteLock();
                _slimLock = slimLock;
            }

            public void Dispose()
            {
                _slimLock.ExitWriteLock();
            }
        }
    }
}
