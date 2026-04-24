using System.ComponentModel;
using Parquet.Schema;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Parquet.XTract;

public class XCommandSettings : CommandSettings {
    [CommandOption("-s|--source")]
    [Description("Source MSSQL connection string")]
    public string SourceConnection { get; set; } = string.Empty;
}

public class XCommand : AsyncCommand<XCommandSettings> {

    private async Task WriteAsync(ColumnExtract column, ParquetRowGroupWriter w) {
        // switch on all types
        Type inType = column.Field.ClrType;
        bool isNullable = column.Field.IsNullable;

        if(inType == typeof(int)) {
            if(isNullable) {
                await w.WriteAsync<int>(column.Field, column.Values.Cast<int?>().ToArray());
            } else {
                await w.WriteAsync<int>(column.Field, column.Values.Cast<int>().ToArray());
            }
        } else if(inType == typeof(bool)) {
            if(isNullable) {
                await w.WriteAsync<bool>(column.Field, column.Values.Cast<bool?>().ToArray());
            } else {
                await w.WriteAsync<bool>(column.Field, column.Values.Cast<bool>().ToArray());
            }
        } else if(inType == typeof(byte)) {
            if(isNullable) {
                await w.WriteAsync<byte>(column.Field, column.Values.Cast<byte?>().ToArray());
            } else {
                await w.WriteAsync<byte>(column.Field, column.Values.Cast<byte>().ToArray());
            }
        } else if(inType == typeof(short)) {
            if(isNullable) {
                await w.WriteAsync<short>(column.Field, column.Values.Cast<short?>().ToArray());
            } else {
                await w.WriteAsync<short>(column.Field, column.Values.Cast<short>().ToArray());
            }
        } else if(inType == typeof(float)) {
            if(isNullable) {
                await w.WriteAsync<float>(column.Field, column.Values.Cast<float?>().ToArray());
            } else {
                await w.WriteAsync<float>(column.Field, column.Values.Cast<float>().ToArray());
            }
        } else if(inType == typeof(long)) {
            if(isNullable) {
                await w.WriteAsync<long>(column.Field, column.Values.Cast<long?>().ToArray());
            } else {
                await w.WriteAsync<long>(column.Field, column.Values.Cast<long>().ToArray());
            }
        } else if(inType == typeof(decimal)) {
            if(isNullable) {
                await w.WriteAsync<decimal>(column.Field, column.Values.Cast<decimal?>().ToArray());
            } else {
                await w.WriteAsync<decimal>(column.Field, column.Values.Cast<decimal>().ToArray());
            }
        } else if(inType == typeof(double)) {
            if(isNullable) {
                await w.WriteAsync<double>(column.Field, column.Values.Cast<double?>().ToArray());
            } else {
                await w.WriteAsync<double>(column.Field, column.Values.Cast<double>().ToArray());
            }
        } else if(inType == typeof(DateTime)) {
            if(isNullable) {
                await w.WriteAsync<DateTime>(column.Field, column.Values.Cast<DateTime?>().ToArray());
            } else {
                await w.WriteAsync<DateTime>(column.Field, column.Values.Cast<DateTime>().ToArray());
            }
        } else if(inType == typeof(TimeSpan)) {
            if(isNullable) {
                await w.WriteAsync<TimeSpan>(column.Field, column.Values.Cast<TimeSpan?>().ToArray());
            } else {
                await w.WriteAsync<TimeSpan>(column.Field, column.Values.Cast<TimeSpan>().ToArray());
            }
        } else if(inType == typeof(string)) {

            string?[] values;

            if(column.SourceType == "sql_variant" ||
               column.SourceType.EndsWith(".geography") ||
               column.SourceType.EndsWith(".geometry") ||
               column.SourceType.EndsWith(".hierarchyid")) {
                values = column.Values.Select(o => o?.ToString()).ToArray();
            } else {
                values = column.Values.Cast<string?>().ToArray();
            }
            
            await w.WriteAsync(column.Field, values);
                
        } else if(inType == typeof(Guid)) {
            if(isNullable) {
                await w.WriteAsync<Guid>(column.Field, column.Values.Cast<Guid?>().ToArray());
            } else {
                await w.WriteAsync<Guid>(column.Field, column.Values.Cast<Guid>().ToArray());
            }
        } else if(inType == typeof(byte[])) {
            if(isNullable) {
                await w.WriteAsync(column.Field, column.Values.Cast<byte[]?>().ToArray());
            } else {
                await w.WriteAsync(column.Field, column.Values.Cast<byte[]>().ToArray());
            }
        } else {
            throw new NotImplementedException($"Cannot write column type {inType}");
        }
    }

    private async Task Process(MssqlLoader loader, SourceTable table, ProgressContext ctx) {
        
        var task = ctx
            .AddTask($"[grey]{table.Schema}[/].[yellow]{table.Table}[/]")
            .IsIndeterminate(true);
        
        TableExtract memData = await loader.ExportDataAsync(table);
        
        if(memData.RowCount == 0) {
            AnsiConsole.MarkupLine($"[red]Table is empty, skipping[/]");
            task.Increment(100);
            return;
        }
        
        task.IsIndeterminate(false);
        task.MaxValue = memData.Columns.Count;
        
        AnsiConsole.MarkupLine($"Writing {memData.RowCount} row(s) to disk...");
        var schema = new ParquetSchema(memData.Columns.Select(c => c.Field));
        using var fs = new FileStream($"{table.Schema}.{table.Table}.parquet", FileMode.Create, FileAccess.Write);
        await using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs);
        
        // this demo will use just one row group
        using ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup();

        foreach(ColumnExtract column in memData.Columns) {
            task.Description = $"[grey]{table.Schema}[/].[yellow]{table.Table}[/] :: {column.Field.Name} ({column.SourceType})";
            //AnsiConsole.MarkupLine($"Writing column [yellow]{column.Field.Name}[/] | {column.SourceType}...");
        
            await WriteAsync(column, rowGroupWriter);
            task.Increment(1);
        }

        task.Description = $"[grey]{table.Schema}[/].[yellow]{table.Table}[/]";
    }
    
    protected override async Task<int> ExecuteAsync(CommandContext context, XCommandSettings settings, CancellationToken cancellationToken) {

        await AnsiConsole.Progress()
            .HideCompleted(true)
            .StartAsync(async ctx => {
                var initTask = ctx.AddTask("Initializing");
                var exportTask = ctx.AddTask("Exporting");
                
                using var loader = new MssqlLoader(settings.SourceConnection);
                string dbName = await loader.GetCurrentDbNameAsync();
                await Task.Delay(1000);
                initTask.Increment(50);
                
                AnsiConsole.MarkupLine($"Connected to database [yellow]{dbName}[/], listing tables...");
                IReadOnlyCollection<SourceTable> tableNames = await loader.ListTableNamesAsync();
                
                await Task.Delay(1000);
                initTask.Increment(50);
                
                AnsiConsole.MarkupLine($"Found [red]{tableNames.Count}[/] table(s):");
                foreach(SourceTable table in tableNames) {
                    AnsiConsole.MarkupLine($"- [grey]{table.Schema}[/].[green]{table.Table}[/]");
                }

                foreach(SourceTable sourceTable in tableNames) {
                    await Process(loader, sourceTable, ctx);
                    exportTask.Increment(100 / tableNames.Count);
                }
                
            });
        
        return 0;
    }
}