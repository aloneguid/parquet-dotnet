using Parquet.Schema;

namespace Parquet.XTract;

public record SourceTable(string Schema, string Table);

public record TableExtract(SourceTable Table, List<ColumnExtract> Columns, int RowCount);

public record ColumnExtract(DataField Field, List<object?> Values, string SourceType);