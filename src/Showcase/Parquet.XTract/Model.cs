using Parquet.Schema;

namespace Parquet.XTract;

public record SourceTable(string Schema, string Name);

public record TableExtract(SourceTable Table, List<ColumnExtract> Columns, int RowCount);

public record ColumnExtract(DataField Field, List<object?> Values, string SourceType);

public interface IRelDbLoader : IDisposable {
    Task<string> GetCurrentDbNameAsync();

    Task<IReadOnlyCollection<SourceTable>> ListTableNamesAsync();

    Task<TableExtract> ExportDataAsync(SourceTable table);
}