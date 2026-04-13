using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using Parquet.Serialization;
using Xunit;
using Type = System.Type;

namespace Parquet.Test.Reader;

public class ParquetCsvComparison : TestBase {
    protected async Task CompareFilesAsync(string baseName, string encoding, string dataPageVersion, bool treatByteArrayAsString,
        params Type[] columnTypes) {

        string parquetFilePrefix = $"{baseName}";

        if(!string.IsNullOrEmpty(encoding)) {
            parquetFilePrefix += $".{encoding}";
        }

        if(!string.IsNullOrEmpty(dataPageVersion)) {
            parquetFilePrefix += $".{dataPageVersion}";
        }

        IList<Dictionary<string, object>> parquet = await ReadParquetAsync($"{parquetFilePrefix}.parquet", treatByteArrayAsString);
        IList<Dictionary<string, string>> csv = ReadCsv($"{baseName}.csv");

        Compare(parquet, csv, columnTypes);
    }

    private void Compare(IList<Dictionary<string, object>> parquet, IList<Dictionary<string, string>> csv, Type[] columnTypes) {
        Assert.Equal(csv.Count, parquet.Count);

        List<string> columnNames = csv.Count > 0 ? csv[0].Keys.ToList() : [];

        for(int rowIndex = 0; rowIndex < csv.Count; rowIndex++) {
            Dictionary<string, string> csvRow = csv[rowIndex];
            Dictionary<string, object> parquetRow = parquet[rowIndex];

            for(int colIndex = 0; colIndex < columnNames.Count; colIndex++) {
                string columnName = columnNames[colIndex];

                csvRow.TryGetValue(columnName, out string? csvValue);
                parquetRow.TryGetValue(columnName, out object? parquetValueObj);
                string parquetValue = ConvertToString(parquetValueObj, columnTypes[colIndex]);

                csvValue = Stringify(csvValue);
                parquetValue = Stringify(parquetValue);

                bool equal = string.Equals(csvValue, parquetValue, StringComparison.Ordinal);
                if(!equal) {
                    string message =
                        $"Mismatch at row {rowIndex + 1}, column '{columnName}' (col#{colIndex + 1}).\n" +
                        $"CSV:     [{csvValue ?? "<null>"}]\nParquet: [{parquetValue ?? "<null>"}]";
                    Assert.Fail(message);
                }
            }
        }
    }

    private string ConvertToString(object? value, Type columnType) {
        if(value == null) {
            return string.Empty;
        }

        // Get the non-nullable type for comparison
        Type baseType = columnType.GetNonNullable();

        return baseType switch {
            _ when baseType == typeof(bool) => value.ToString()!.ToLowerInvariant(),
            _ when baseType == typeof(byte[]) => Encoding.UTF8.GetString((byte[])value),
            _ when IsNumericType(baseType) || baseType == typeof(decimal) 
                => Convert.ToString(value, CultureInfo.InvariantCulture)!,
            _ when baseType == typeof(DateTime) 
                => ((DateTime)value).ToString("O", CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };
    }

    private string Stringify(string? value) {
        if(value == null)
            return string.Empty;

        if(value == "0.0")
            return "0";

        if(DateTime.TryParse(value, out DateTime result)) {
            return result.ToString("o");
        }


        return value.Trim();
    }

    private bool IsNumericType(Type type) {
        return type == typeof(byte) ||
               type == typeof(sbyte) ||
               type == typeof(short) ||
               type == typeof(ushort) ||
               type == typeof(int) ||
               type == typeof(uint) ||
               type == typeof(long) ||
               type == typeof(ulong) ||
               type == typeof(float) ||
               type == typeof(double);
    }

    private async Task<IList<Dictionary<string, object>>> ReadParquetAsync(string name,
        bool treatByteArrayAsString) {
        await using Stream s = OpenTestFile(name);
        var options = new ParquetOptions { TreatByteArrayAsString = treatByteArrayAsString };
        ParquetSerializer.UntypedResult r = await ParquetSerializer.DeserializeAsync(s, new ParquetSerializerOptions { ParquetOptions = options });
        return r.Data;
    }

    private IList<Dictionary<string, string>> ReadCsv(string name) {
        var result = new List<Dictionary<string, string>>();

        var columnNames = new List<string>();

        using(StreamReader fs = OpenTestFileReader(name)) {
            var reader = new CsvReader(fs, CultureInfo.InvariantCulture);

            //header
            reader.Read();
            columnNames.AddRange(Enumerable.Range(0, reader.ColumnCount).Select(i => reader.GetField(i)!));

            //values
            while(reader.Read()) {
                var row = new Dictionary<string, string>();
                for(int i = 0; i < reader.ColumnCount; i++) {
                    string columnName = columnNames[i];
                    row[columnName] = reader.GetField(i)!;
                }
                result.Add(row);
            }
        }

        if(columnNames == null)
            throw new InvalidOperationException("no columns?");

        return result;
    }

}