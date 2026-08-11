using Parquet.XTract;
using Spectre.Console.Cli;

var app = new CommandApp<XCommand>();
return await app.RunAsync(args);
