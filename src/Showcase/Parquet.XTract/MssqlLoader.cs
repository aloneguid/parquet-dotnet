using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Types;
using Parquet.Schema;

namespace Parquet.XTract;

/// <summary>
/// https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient.sqlconnection?view=sqlclient-dotnet-core-6.1
/// </summary>
class MssqlLoader : IRelDbLoader {
    
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

    public async Task<IReadOnlyCollection<SourceTable>> ListTablesAsync() {
        if(_connection.State != System.Data.ConnectionState.Open)
            await _connection.OpenAsync();

        await using SqlCommand command = _connection.CreateCommand();
        command.CommandText = """
            SELECT t.TABLE_SCHEMA,
                   t.TABLE_NAME,
                   c.COLUMN_NAME,
                   c.IS_NULLABLE,
                   c.DATA_TYPE
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
                ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
               AND c.TABLE_NAME = t.TABLE_NAME
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
            """;

        List<SourceTable> tables = [];
        Dictionary<(string Schema, string Name), SourceTable> tableByName = [];

        await using SqlDataReader reader = await command.ExecuteReaderAsync();
        while(await reader.ReadAsync()) {
            string schema = reader.GetString(0);
            string tableName = reader.GetString(1);
            (string Schema, string Name) key = (schema, tableName);
            if(!tableByName.TryGetValue(key, out SourceTable? table)) {
                table = new SourceTable(schema, tableName);
                tableByName.Add(key, table);
                tables.Add(table);
            }

            if(reader.IsDBNull(2)) {
                continue;
            }

            string columnName = reader.GetString(2);
            bool isNullable = reader.IsDBNull(3) || string.Equals(reader.GetString(3), "YES", StringComparison.OrdinalIgnoreCase);
            string dataType = reader.GetString(4);

            Type clrType = MapSourceTypeToClrType(dataType);
            table.Columns.Add(new SourceColumn(columnName, clrType, isNullable, dataType));
        }

        return tables;
    }

    internal static Type MapSourceTypeToClrType(string dataType) {
        string sourceType = dataType;

        return sourceType.ToLowerInvariant() switch {
            "bit" => typeof(bool),
            "tinyint" => typeof(byte),
            "smallint" => typeof(short),
            "int" => typeof(int),
            "bigint" => typeof(long),
            "real" => typeof(float),
            "float" => typeof(double),
            "decimal" => typeof(decimal),
            "numeric" => typeof(decimal),
            "money" => typeof(decimal),
            "smallmoney" => typeof(decimal),
            "date" => typeof(DateTime),
            "datetime" => typeof(DateTime),
            "datetime2" => typeof(DateTime),
            "smalldatetime" => typeof(DateTime),
            "datetimeoffset" => typeof(DateTime),
            "time" => typeof(TimeSpan),
            "uniqueidentifier" => typeof(Guid),
            "binary" => typeof(byte[]),
            "varbinary" => typeof(byte[]),
            "image" => typeof(byte[]),
            "timestamp" => typeof(byte[]),
            "rowversion" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    public async Task<TableExtract> ExportDataAsync(SourceTable table) {
        if(_connection.State != System.Data.ConnectionState.Open)
            await _connection.OpenAsync();

        await using SqlCommand command = _connection.CreateCommand();
        command.CommandText = $"select {string.Join(", ", table.Columns.Select(c => c.Name))} from {table.Schema}.{table.Name}";

        int rowCount = 0;
        await using SqlDataReader reader = await command.ExecuteReaderAsync();
        IReadOnlyList<DbColumn> dbSchema = await reader.GetColumnSchemaAsync();
        
        // can't get row count in forward-only data reader
        var columnsData = new List<ColumnExtract>();
        
        while(await reader.ReadAsync()) {
            for(int i = 0; i < dbSchema.Count; i++) {
                SourceColumn sc = table.Columns[i];
                
                if(i >= columnsData.Count) {
                    columnsData.Add(new ColumnExtract(sc, new List<object?>()));
                }

                ColumnExtract colData = columnsData[i];
                bool isNull = reader.IsDBNull(i);
                if(isNull) {
                    colData.Values.Add(null);
                } else {
                    object? value = reader.GetValue(i);
                    if(value is DateTimeOffset dto) {
                        value = dto.DateTime;
                    } else if(value is SqlGeography geog) {
                        value = geog.ToString();
                    } else if(value is SqlGeometry geom) {
                        value = geom.ToString();
                    } else if(value is SqlHierarchyId hid) {
                        value = hid.ToString();
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