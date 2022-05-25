# Mackerel Remote Cache CLI

A command line interface (CLI) that allows terminal access to send commands to and read the replies sent from the cache.  

### Usage

To connect to a remote node, you can pass a connection string as a start-up argument:
```console
Mackerel.RemoteCache.Cli nodeA,nodeB
```

Or you can use the `CONNECT` command:
```console
(not connected)> CONNECT nodeA,nodeB
```
_Note: calling `CONNECT` without a connection string will default to localhost_


To get started, type `HELP` to display a list of available commands:
```console
(localhost:11211)> HELP
```
You can also view `HELP` for a particular command:
```console
(localhost:11211)> HELP PUT
```

The following uses the `PUT` command to write a value of `StBernard` to the key `Cujo` in partition `MovieDogs`:
```console
(localhost:11211)> PUT MovieDogs Cujo StBernard

"Success"
```

Then retrieves the key using `GET`:
```console
(localhost:11211)> GET MovieDogs Cujo

"StBernard"
```

`SCAN` will iterate all keys in a partition and return those matching a [glob pattern](https://en.wikipedia.org/wiki/Glob_(programming)):
```console
(localhost:11211)> SCAN MovieDogs * 10

{
  "Key": "Cujo",
  "Value": "StBernard"
}
{
  "Key": "Toto",
  "Value": "Terrier"
}
{
  "Key": "Lassie",
  "Value": "Collie"
}
{
  "Key": "Hooch",
  "Value": "Mastiff"
}
```

And `STATS` will return general statistics for the cache:
```console
(localhost:11211)> STATS

{
  "CurrentItems": 4,
  "TotalItems": 4,
  "Hits": 1,
  "Misses": 0,
  ...
}
```

## Keyboard Shortcuts

| Shortcut                       | Comment                           |
| ------------------------------ | --------------------------------- |
| `Ctrl`+`A` / `HOME`            | Beginning of line                 |
| `Ctrl`+`E` / `END`             | End of line                       |
| `Tab`                          | Command line completion           |
| `Shift`+`Tab`                  | Backwards command line completion |
| `Ctrl`+`L` / `Esc`             | Clear line                        |
| `Ctrl`+`N` / `↓`               | Forward in history                |
| `Ctrl`+`P` / `↑`               | Backward in history               |