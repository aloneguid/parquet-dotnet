using System.ComponentModel;
using System.Text.RegularExpressions;
using Parquet.Schema;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Parquet.XTract;

public enum LoaderDialect {
    Mssql,
    Postgres
}

public class XCommandSettings : CommandSettings {
    [CommandOption("-s|--source", isRequired: true)]
    [Description("Source connection string")]
    public string SourceConnection { get; set; } = string.Empty;

    [CommandOption("-d|--dialect", isRequired: true)]
    [Description("Database dialect (Mssql or Postgres)")]
    public LoaderDialect Dialect { get; set; } = LoaderDialect.Mssql;

    [CommandOption("-t|--table-regex")]
    [Description("Regex pattern to filter tables")]
    public string TableFilterRegex { get; set; } = "";
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

    private async Task Process(IRelDbLoader loader, SourceTable table) {

        AnsiConsole.Write(new Rule(table.ToMarkupString()) { Justification = Justify.Right });

        AnsiConsole.MarkupLine($"Selecting rows...");
        TableExtract memData = await loader.ExportDataAsync(table);

        if(memData.RowCount == 0) {
            AnsiConsole.MarkupLine($"[red]empty[/]");
            return;
        }

        AnsiConsole.MarkupLine($"Writing [yellow]{memData.RowCount}[/] row(s) to disk...");
        var schema = new ParquetSchema(memData.Columns.Select(c => c.Field));
        string fileName = $"{table.Schema}.{table.Name}.parquet";
        if(System.IO.File.Exists(fileName)) {
            System.IO.File.Delete(fileName);
        }
        using var fs = new FileStream(fileName, FileMode.Create, FileAccess.Write);
        await using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs);
        
        // this demo will use just one row group
        using ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup();

        foreach(ColumnExtract column in memData.Columns) {
            AnsiConsole.MarkupLine($"Writing column [yellow]{Markup.Escape(column.Field.Name)}[/] | {Markup.Escape(column.SourceType)}...");
            await WriteAsync(column, rowGroupWriter);
        }
    }

    private async Task<IRelDbLoader> CreateLoaderAsync(XCommandSettings settings) {
        switch(settings.Dialect) {
            case LoaderDialect.Mssql:
                return new MssqlLoader(settings.SourceConnection);
            case LoaderDialect.Postgres:
                return new PgLoader(settings.SourceConnection);
            default:
                throw new NotSupportedException($"Unknown dialect: {settings.Dialect}");
        }
    }

    private async Task<IReadOnlyCollection<SourceTable>> ListTablesAsync(IRelDbLoader loader, XCommandSettings settings) {
        IReadOnlyCollection<SourceTable> allTables = await loader.ListTableNamesAsync();
        Regex? tableFilterRegex = string.IsNullOrEmpty(settings.TableFilterRegex)
            ? null
            : new Regex(settings.TableFilterRegex, RegexOptions.Compiled);

        var result = allTables
            .Where(t => tableFilterRegex == null || tableFilterRegex.IsMatch(t.ToString())).ToList();
        AnsiConsole.MarkupLine($"Found [green]{allTables.Count}[/] table(s)" + 
            (allTables.Count == result.Count ? "" : $", [yellow]{result.Count}[/] table(s) match the filter [grey]{settings.TableFilterRegex}[/]") +
            ": " +
            string.Join(", ", result.Select(t => t.ToMarkupString())));
        return result;
    }

    protected override async Task<int> ExecuteAsync(CommandContext context, XCommandSettings settings, CancellationToken cancellationToken) {
        try {
            await AnsiConsole.Status().StartAsync("Initializing...", async ctx => {
                using IRelDbLoader loader = await CreateLoaderAsync(settings);

                ctx.Status("Getting current database name...");
                string dbName = await loader.GetCurrentDbNameAsync();

                ctx.Status("Listing tables...");
                IReadOnlyCollection<SourceTable> tableNames = await ListTablesAsync(loader, settings);

                ctx.Status("Exporting...");
                foreach(SourceTable sourceTable in tableNames) {
                    try {
                        await Process(loader, sourceTable);
                    } catch(Exception ex) {
                        AnsiConsole.WriteException(ex);
                    }
                }
            });

            return 0;
        } catch(Exception ex) {
            AnsiConsole.WriteException(ex);
            return 1;
        }
    }
}