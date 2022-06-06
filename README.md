# Mackerel Cache

### About

A high performance distributed in-memory key-value cache. The design leans heavily on existing industry-standard caching systems, such as [Redis](https://redis.io/), [Memcached](https://memcached.org/), [Apache Geode](https://github.com/apache/geode), [Apache Ignite](https://ignite.apache.org/), [NCache](http://www.alachisoft.com/ncache/), and others.

### Features

* Combines the memory capacity of multiple cache nodes to create a unified and distributed cache.

* Supports standard GET, PUT and DEL commands.

* Supports partitioning for logical separation of key spaces.
    > The partition is the core building block of the cache. All data is organized into partitions and you do all of your data puts, gets, etc. against them.

* A Least-Recently-Used (LRU) algorithm is used to evict items when cache capacity is met. 
    > Ideally host machines would be provisioned with sufficient memory to accommodate the expected peak load of the applications, however situations may arise when resources become over utilized and data needs evicted. 

* Supports key watching
    > Applications can monitor an entire partition or specific keys to be notified of any updates.

* Built on top of [gRPC and protocol buffers](https://grpc.io/) which enables _both high-performance and high-productivity design of distributed applications._ 

### Use Cases

* Store frequently used data in cache to speed up a system or decrease load on a back-end component.

* Useful in the [cache-aside pattern](https://docs.microsoft.com/en-us/azure/architecture/patterns/cache-aside) where applications ensure data in the cache is as up-to-date as possible, but can also handle situations when the data in the cache has become stale or unavailable. 

* Improve the performance and scalability of an ASP.NET Core app by using a distributed cache.


### Client

The cache client is the interface between the application and the caching system. It will handle most of the internals for you, such as connection tolerance, data distribution, parallelism, etc.

_Due to significant .NET gRPC updates introduced, version 2.* and above of the cache client only targets .NET Standard 2.1 and above._

#### Basic Usage

The central object in the client is the `ICacheConnection`; it is designed to be used as a singleton and shared between callers. 

```csharp
ICacheConnection connection = CacheConnection.Create("localhost1,localhost2") 
```

Once you have a connection, accessing a typed cache reference from a connection looks something like:

```csharp
ICache<string> cache = connection.GetCache(new StringCacheCodec(), new ConsistentHashFunction(connection), new KeyRouter());
```

Once you have the `ICache<T>`, the simplest operation would be to PUT and GET a value:

```csharp
await cache.PutAsync("my_partition", "foo", "bar");
...
string val = await cache.GetAsync("my_partition", "foo");
Console.WriteLine(val); // writes "bar"
``` 

Here we are working with simple strings; in order to work with POCO objects you will need to supply your own `ICacheCodec` that handles the encoding and decoding. 

#### Watch for key changes

An `ICache<T>` also allows you to be notified of any updates to keys. You can monitor an entire partition:

```csharp
await cache.WatchAsync("my_watcher", "my_partition", w =>
{
    Console.WriteLine(w);
});
```

Or monitor a specific key:

```csharp
await cache.WatchAsync("my_watcher", "my_partition", "foo", w =>
{
    Console.WriteLine(w);
});
```

#### Routing

Routing allows you to organize your data accordingly by supplying an `IRouter` implementation. Three default implementations are provided out of the box:
* `KeyRouter` - Spreads keys in a partition evenly across the cache nodes. For most cases, this is the best choice.
* `PartitionRouter` - Sends all keys in a partition to the same node.
* `ClientAccountRouter` - Sends all keys from a single client application to the same cache node. Uses the user name the client application is running as.

This can be supplied in your call to `GetCache` or overridden through DI by adding an `IRouter` before your `ICache<T>` instance.

```csharp
...
services.AddSingleton<IRouter, PartitionRouter>();
services.AddMackerelCache<string>();
...
```

#### Accessing individual nodes

For maintenance purposes, it is sometimes necessary to issue node-specific commands:

```csharp
foreach (var node in connection.GetNodes())
{
    var result = await node.PingAsync();
}
```

#### Dependency Injection

If you're using `Microsoft.Extensions.DependencyInjection`, consider adding the `Mackerel.RemoteCache.Client.Extensions` dependency to your application to gain access to DI friendly helpers:

```csharp
// add your encoder/decoder
services.AddSingleton<ICacheCodec<string>, StringCacheCodec>();
// setup the ICache<T>
services.AddMackerelCache<string>();
```

You can configure the client by adding the following to your settings file:
```json
{
  "MackerelCache": {
    "TimeoutMilliseconds": 10000, // optional
    "SessionTimeoutMilliseconds": 15000, // optional
    "Endpoints": [ "localhost1", "localhost2" ], // required
    "Grpc": { // optional
      // see https://docs.microsoft.com/en-us/aspnet/core/grpc/configuration
    }
  }
}
```

### ASP.NET Core Distributed Caching

If you are running an ASP.NET Core application and need a distributed cache to store things such as session data, you can pull in the `Mackerel.RemoteCache.Client.Extensions` package.
This package sits on top of the `Mackerel.RemoteCache.Client` and can be used to add an `Microsoft.Extensions.Caching.Distributed.IDistributedCache` compatible object to your web application:

```csharp
services.AddSession();
...
services.AddMackerelDistributedCache(o =>
{
    o.Partition = "my_partition";
    o.ExpirationType = ExpirationType.Sliding;
    o.Expiration = TimeSpan.FromMinutes(10);
});
...
app.UseSession();
```

You can optionally configure the IDistributedCache by adding the following sub-section to the configuration outlined above:
```json
{
  "MackerelCache": {
   ... // see above for options
   "DistributedCache": {
      "Partition": "my_partition",
      "ExpirationType": "Sliding",
      "Expiration": "00:10:00"
    }
  }
}
```

See the following for more details.
* https://docs.microsoft.com/en-us/aspnet/core/performance/caching/distributed 
* https://docs.microsoft.com/en-us/aspnet/core/fundamentals/app-state


### Considerations

#### Ephemerality
* Mackerel cache is purely in-memory, meaning all data you send to it should be ephemeral (short lived) and is volatile (can disappear). There is no guarantee that the data you cached will be available for retrieval at some point in the future.

#### Partitioning
* Partitions are created when needed and do not need to be configured ahead of time, however it is recommended to explicitly create a partition by calling the PutPartition method. This allows for more finer grain control over a partition's configuration which includes the ability to "reserve" a specific amount of cache capacity.
  ```csharp
  await _connection.PutPartitionAsync("my_partition", TimeSpan.FromMinutes(10), ExpirationType.Absolute, true, EvictionPolicy.Lru, 1048576);
  ``` 

#### Key Watching
* Key watch events are delivered at most once for each watcher. 
* There are no guarantees that a key expired watch event will be sent at the exact time a key expires.
    * Keys are expired one of two ways: passively when a key is accessed or eagerly by a background process that expires keys incrementally.