using Parquet.Schema;
using Spectre.Console;

namespace Parquet.XTract;

public class SourceTable {
    public SourceTable(string schema, string name) {
        Schema = schema;
        Name = name;
    }

    public string Schema { get; }
    public string Name { get; }

    public override string ToString() => $"{Schema}.{Name}";

    public string ToMarkupString() {
        return $"[grey]{Markup.Escape(Schema)}[/].[green]{Markup.Escape(Name)}[/]";
    }
}

public record TableExtract(SourceTable Table, List<ColumnExtract> Columns, int RowCount);

public record ColumnExtract(DataField Field, List<object?> Values, string SourceType);

public interface IRelDbLoader : IDisposable {
    Task<string> GetCurrentDbNameAsync();

    Task<IReadOnlyCollection<SourceTable>> ListTableNamesAsync();

    Task<TableExtract> ExportDataAsync(SourceTable table);
}