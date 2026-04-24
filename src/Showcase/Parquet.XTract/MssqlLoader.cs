using System.Collections.ObjectModel;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Parquet.Schema;
using Spectre.Console;

namespace Parquet.XTract;

/// <summary>
/// https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient.sqlconnection?view=sqlclient-dotnet-core-6.1
/// </summary>
class MssqlLoader : IDisposable {
    
    private readonly SqlConnection _connection;
    
    public string ConnectionString { get; }

    public MssqlLoader(string connectionString) {
        ConnectionString = connectionString;
        _connection = new SqlConnection(connectionString);
    }
    
    public async Task<string> GetCurrentDbNameAsync() {
        if(_connection.State != System.Data.ConnectionState.Open)
            await _connection.OpenAsync();

        await using SqlCommand command = _connection.CreateCommand();
        command.CommandText = "SELECT DB_NAME()";

        return await command.ExecuteScalarAsync() as string ?? string.Empty;
    }

    public async Task<IReadOnlyCollection<SourceTable>> ListTableNamesAsync() {
        if(_connection.State != System.Data.ConnectionState.Open)
            await _connection.OpenAsync();

        await using SqlCommand command = _connection.CreateCommand();
        command.CommandText = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """;

        List<SourceTable> tableNames = [];

        await using SqlDataReader reader = await command.ExecuteReaderAsync();
        while(await reader.ReadAsync()) {
            tableNames.Add(new SourceTable(reader.GetString(0), reader.GetString(1)));
        }

        return tableNames;
    }

    private DataField ToParquetDataField(DbColumn column) {
        AnsiConsole.WriteLine($"{column.ColumnName} {column.DataType} {column.AllowDBNull}");
        bool isNullable = column.AllowDBNull ?? false;
        Type? clrType = column.DataType;
        
        if(clrType == null) 
            throw new InvalidOperationException($"Column '{column.ColumnName}' has null data type.");
        
        if(clrType == typeof(DateTimeOffset))
            clrType = typeof(DateTime);
        
        else if(clrType == typeof(object))
            clrType = typeof(string);
        
        else if(clrType == typeof(Microsoft.SqlServer.Types.SqlGeography))
            clrType = typeof(string);

        else if(clrType == typeof(Microsoft.SqlServer.Types.SqlGeometry))
            clrType = typeof(string);
        
        else if(clrType == typeof(Microsoft.SqlServer.Types.SqlHierarchyId))
            clrType = typeof(string);
        
        return new DataField(column.ColumnName, clrType, isNullable);
    }

    public async Task<TableExtract> ExportDataAsync(SourceTable table) {
        if(_connection.State != System.Data.ConnectionState.Open)
            await _connection.OpenAsync();

        await using SqlCommand command = _connection.CreateCommand();
        command.CommandText = $"select * from {table.Schema}.{table.Table}";

        int rowCount = 0;
        await using SqlDataReader reader = await command.ExecuteReaderAsync();
        ReadOnlyCollection<DbColumn> dbSchema = await reader.GetColumnSchemaAsync();
        
        // can't get row count in forward-only data reader
        var columnsData = new List<ColumnExtract>();
        
        while(await reader.ReadAsync()) {
            for(int i = 0; i < dbSchema.Count; i++) {
                DbColumn dbColMeta = dbSchema[i];

                if(i >= columnsData.Count) {
                    DataField df = ToParquetDataField(dbColMeta);
                    columnsData.Add(new ColumnExtract(df, new List<object?>(), dbColMeta.DataTypeName ?? string.Empty));
                }

                ColumnExtract colData = columnsData[i];
                bool isNull = reader.IsDBNull(i);
                if(isNull) {
                    colData.Values.Add(null);
                } else {
                    object? value = reader.GetValue(i);
                    if(value is DateTimeOffset dto) {
                        value = dto.DateTime;
                    }
                    colData.Values.Add(value);
                }
            }  
            
            rowCount++;
        }

        return new TableExtract(table, columnsData, rowCount);
    }
    
    
    public void Dispose() {
        _connection.Dispose();
    }
}