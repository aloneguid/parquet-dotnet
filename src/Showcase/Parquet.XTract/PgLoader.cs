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

    public async Task<IReadOnlyCollection<SourceTable>> ListTableNamesAsync() {
        await using NpgsqlConnection connection = await _ds.OpenConnectionAsync();
        await using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name
            """;

        List<SourceTable> tableNames = [];

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
        while(await reader.ReadAsync()) {
            string schema = reader.GetString(0);
            if(schema == "pg_catalog" || schema == "information_schema") { continue; }

            string tableName = reader.GetString(1);
            tableNames.Add(new SourceTable(schema, tableName));
        }

        return tableNames;
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

    private static DataField ToParquetDataField(DbColumn column) {
        string columnName = column.ColumnName ?? throw new InvalidOperationException("Column name cannot be null.");
        bool isNullable = column.AllowDBNull ?? false;
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
        await using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {QuoteIdentifier(table.Schema)}.{QuoteIdentifier(table.Name)}";

        int rowCount = 0;
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
        IReadOnlyList<DbColumn> dbSchema = await reader.GetColumnSchemaAsync();
        List<ColumnExtract> columnsData = [];

        foreach(DbColumn dbColMeta in dbSchema) {
            DataField field = ToParquetDataField(dbColMeta);
            columnsData.Add(new ColumnExtract(field, [], dbColMeta.DataTypeName ?? string.Empty));
        }

        while(await reader.ReadAsync()) {
            for(int i = 0; i < dbSchema.Count; i++) {
                ColumnExtract colData = columnsData[i];
                object? value = reader.IsDBNull(i)
                    ? null
                    : NormalizeValue(reader.GetValue(i), colData.Field.ClrType);
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
