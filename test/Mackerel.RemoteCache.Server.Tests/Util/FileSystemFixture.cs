using System;
using System.IO;
using Xunit;

namespace Mackerel.RemoteCache.Server.Tests.Util
{
    public class FileSystemFixture : IDisposable
    {
        public FileSystemFixture()
        {
            FilePath = Path.GetTempPath() + "/" + Guid.NewGuid().ToString();
            Directory.CreateDirectory(FilePath);
        }

        public void Dispose()
        {
            Directory.Delete(FilePath, true);
        }

        public string FilePath { get; private set; }
    }

    [CollectionDefinition("File system collection")]
    public class FileSystemCollection : ICollectionFixture<FileSystemFixture>
    {

    }
}
