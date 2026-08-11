using System;
using System.Collections.Generic;
using System.Data.Common;
using Npgsql;
using Parquet.Schema;

namespace Parquet.XTract;

internal class PgLoader : IRelDbLoader {

    private readonly NpgsqlDataSource _ds;

    public PgLoader(string connectionString) {
        _ds = NpgsqlDataSource.Create(connectionString);
    }

    public async Task<string> GetCurrentDbNameAsync() {
        await using NpgsqlConnection connection = await _ds.OpenConnectionAsync();
        await using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = "SELECT current_database()";

        return await command.ExecuteScalarAsync() as string ?? string.Empty;
    }

    public async Task<IReadOnlyCollection<SourceTable>> ListTablesAsync() {
        await using NpgsqlConnection connection = await _ds.OpenConnectionAsync();
        await using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT t.table_schema,
                   t.table_name,
                   c.column_name,
                   c.is_nullable,
                   c.data_type
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c
                ON c.table_schema = t.table_schema
               AND c.table_name = t.table_name
            WHERE t.table_type = 'BASE TABLE'
              AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY t.table_schema, t.table_name, c.ordinal_position
            """;

        List<SourceTable> tables = [];
        Dictionary<(string Schema, string Name), SourceTable> tableByName = [];

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
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

    private static bool IsParquetSupportedType(Type clrType) {
        return clrType == typeof(int) ||
               clrType == typeof(bool) ||
               clrType == typeof(byte) ||
               clrType == typeof(short) ||
               clrType == typeof(float) ||
               clrType == typeof(long) ||
               clrType == typeof(decimal) ||
               clrType == typeof(double) ||
               clrType == typeof(DateTime) ||
               clrType == typeof(TimeSpan) ||
               clrType == typeof(string) ||
               clrType == typeof(Guid) ||
               clrType == typeof(byte[]);
    }

    internal static Type MapSourceTypeToClrType(string dataType) {
        string sourceType = dataType;

        return sourceType.ToLowerInvariant() switch {
            "boolean" => typeof(bool),
            "bool" => typeof(bool),
            "smallint" => typeof(short),
            "int2" => typeof(short),
            "integer" => typeof(int),
            "int" => typeof(int),
            "int4" => typeof(int),
            "bigint" => typeof(long),
            "int8" => typeof(long),
            "real" => typeof(float),
            "float4" => typeof(float),
            "double precision" => typeof(double),
            "float8" => typeof(double),
            "numeric" => typeof(decimal),
            "decimal" => typeof(decimal),
            "money" => typeof(decimal),
            "date" => typeof(DateTime),
            "timestamp without time zone" => typeof(DateTime),
            "timestamp with time zone" => typeof(DateTime),
            "time without time zone" => typeof(TimeSpan),
            "time with time zone" => typeof(TimeSpan),
            "uuid" => typeof(Guid),
            "bytea" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    internal static bool ResolveIsNullable(DbColumn column, IReadOnlyDictionary<string, bool> nullabilityByColumnName) {
        if(column.AllowDBNull is bool allowDbNull) {
            return allowDbNull;
        }

        if(column.BaseColumnName is string baseColumnName &&
           nullabilityByColumnName.TryGetValue(baseColumnName, out bool baseColumnIsNullable)) {
            return baseColumnIsNullable;
        }

        if(column.ColumnName is string columnName &&
           nullabilityByColumnName.TryGetValue(columnName, out bool columnIsNullable)) {
            return columnIsNullable;
        }

        return true;
    }

    private static DataField ToParquetDataField(DbColumn column, IReadOnlyDictionary<string, bool> nullabilityByColumnName) {
        string columnName = column.ColumnName ?? throw new InvalidOperationException("Column name cannot be null.");
        bool isNullable = ResolveIsNullable(column, nullabilityByColumnName);
        Type clrType = column.DataType ?? throw new InvalidOperationException($"Column '{columnName}' has null data type.");

        if(clrType == typeof(DateTimeOffset)) {
            clrType = typeof(DateTime);
        } else if(clrType == typeof(DateOnly)) {
            clrType = typeof(DateTime);
        } else if(clrType == typeof(TimeOnly)) {
            clrType = typeof(TimeSpan);
        } else if(clrType == typeof(char) || !IsParquetSupportedType(clrType)) {
            clrType = typeof(string);
        }

        return new DataField(columnName, clrType, isNullable);
    }

    private async Task<IReadOnlyDictionary<string, bool>> GetColumnNullabilityAsync(
        NpgsqlConnection connection,
        SourceTable table) {

        await using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = @schema
              AND table_name = @tableName
            """;
        command.Parameters.AddWithValue("schema", table.Schema);
        command.Parameters.AddWithValue("tableName", table.Name);

        Dictionary<string, bool> nullabilityByColumnName = new(StringComparer.OrdinalIgnoreCase);

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
        while(await reader.ReadAsync()) {
            string columnName = reader.GetString(0);
            bool isNullable = string.Equals(reader.GetString(1), "YES", StringComparison.OrdinalIgnoreCase);
            nullabilityByColumnName[columnName] = isNullable;
        }

        return nullabilityByColumnName;
    }

    private static object? NormalizeValue(object? value, Type targetType) {
        if(value == null) {
            return null;
        }

        if(targetType == typeof(DateTime)) {
            if(value is DateTimeOffset dto) {
                return dto.DateTime;
            }

            if(value is DateOnly dateOnly) {
                return dateOnly.ToDateTime(TimeOnly.MinValue);
            }
        }

        if(targetType == typeof(TimeSpan) && value is TimeOnly timeOnly) {
            return timeOnly.ToTimeSpan();
        }

        if(targetType == typeof(string) && value is not string) {
            return value.ToString();
        }

        return value;
    }

    private static string QuoteIdentifier(string identifier) {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    public async Task<TableExtract> ExportDataAsync(SourceTable table) {
        ArgumentNullException.ThrowIfNull(table);

        await using NpgsqlConnection connection = await _ds.OpenConnectionAsync();
        IReadOnlyDictionary<string, bool> nullabilityByColumnName = await GetColumnNullabilityAsync(connection, table);

        await using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {QuoteIdentifier(table.Schema)}.{QuoteIdentifier(table.Name)}";

        int rowCount = 0;
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
        IReadOnlyList<DbColumn> dbSchema = await reader.GetColumnSchemaAsync();
        List<ColumnExtract> columnsData = [];

        while(await reader.ReadAsync()) {
            for(int i = 0; i < dbSchema.Count; i++) {
                SourceColumn sc = table.Columns[i];
                
                if(i >= columnsData.Count) {
                    columnsData.Add(new ColumnExtract(sc, new List<object?>()));
                }
                ColumnExtract colData = columnsData[i];
                object? value = reader.IsDBNull(i)
                    ? null
                    : NormalizeValue(reader.GetValue(i), colData.Column.ClrType);
                colData.Values.Add(value);
            }

            rowCount++;
        }

        return new TableExtract(table, columnsData, rowCount);
    }

    public void Dispose() {
        _ds.Dispose();
    }
}
