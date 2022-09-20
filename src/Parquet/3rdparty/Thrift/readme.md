This code is selectively copied from the [official codebase](https://github.com/apache/thrift/tree/master/lib/netstd) and somewhat modified due to issues not acceptible in this project:
- removed reference to `Microsoft.Extension.Logging`.
- removed references to unused namespaces and pragmas.
- xml comments are fixed because they are invalid, i.e. contain forbidden xml symbols (<, > etc.)
- turned off compiler warnings for generated code