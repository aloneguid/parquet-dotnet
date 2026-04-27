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
    
    public List<SourceColumn> Columns { get; } = new();

    public string ToMarkupString() {
        return $"[grey]{Markup.Escape(Schema)}[/].[green]{Markup.Escape(Name)}[/]";
    }
}

public class SourceColumn {
    public SourceColumn(string name, Type clrType, bool isNullable, string nativeType) {
        Name = name;
        ClrType = clrType;
        IsNullable = isNullable;
        NativeType = nativeType;
    }
    
    private DataField? _dataField;

    public string Name { get; }
    public Type ClrType { get; }
    public bool IsNullable { get; }
    public string NativeType { get; }

    public DataField ToDataField() {
        if(_dataField == null)
            _dataField = new DataField(Name, ClrType, IsNullable);
        return _dataField;
    }
    
    public override string ToString() =>
        $"{Name}{(IsNullable ? " NULL" : "")} ({ClrType.Name}/{NativeType})";
}

public record TableExtract(SourceTable Table, List<ColumnExtract> Columns, int RowCount);

public record ColumnExtract(SourceColumn Column, List<object?> Values);

public interface IRelDbLoader : IDisposable {
    Task<string> GetCurrentDbNameAsync();

    Task<IReadOnlyCollection<SourceTable>> ListTablesAsync();

    Task<TableExtract> ExportDataAsync(SourceTable table);
}