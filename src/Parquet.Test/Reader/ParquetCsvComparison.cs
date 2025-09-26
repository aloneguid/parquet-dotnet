using NetBox.FileFormats.Csv;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Type = System.Type;

namespace Parquet.Test.Reader {
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
            (DataColumn[] Columns, IReadOnlyList<Field> Fields) parquet = await ReadParquetAsync($"{parquetFilePrefix}.parquet", treatByteArrayAsString);
            DataColumn[] csv = ReadCsv($"{baseName}.csv");
            Compare(parquet, csv, columnTypes);
        }

        private void Compare((DataColumn[] Columns, IReadOnlyList<Field> Fields) parquet, DataColumn[] csv, Type[] columnTypes) {
            DataColumn[] parquetCols = parquet.Columns;

            Assert.All(parquet.Fields, (field, i) => {
                var dataField = field as DataField;
                Assert.Equal(columnTypes[i], dataField?.ClrNullableIfHasNullsType);
            });

            var errors = new List<string>();

            //compare number of columns is the same
            Assert.True(parquetCols.Length == csv.Length, $"parquet has {parquetCols.Length} column(s) but CSV has {csv.Length}");

            //compare column names
            foreach(DataColumn column in parquetCols) {
                string colName = column.Field.Name;
                bool contains = csv.Any(f => f.Field.Name == colName);
                Assert.True(contains, $"csv does not contain column '{colName}'");
            }

            //compare column values one by one
            for(int ci = 0; ci < parquetCols.Length; ci++) {
                DataColumn pc = parquetCols[ci];
                DataColumn cc = csv[ci];

                for(int ri = 0; ri < pc.Data.Length; ri++) {
                    Type clrType = pc.Field.ClrType;
                    object? pv = pc.Data.GetValue(ri);
                    object? cv = ChangeType(cc.Data.GetValue(ri), clrType);

                    if(pv == null) {
                        bool isCsvNull =
                           cv == null ||
                           (cv is string s && s == string.Empty);

                        if(!isCsvNull) errors.Add($"expected null value in column {pc.Field.Name}, value #{ri}");
                    }
                    else {
                        if(clrType == typeof(string)) {
                            if(((string)pv).Trim() != ((string?)cv)?.Trim()) {
                                errors.Add($"expected {cv} but was {pv} in column {pc.Field.Name}, value #{ri}");
                                //errors.Add(pv.ToString());
                            }
                        }
                        else if(clrType == typeof(byte[])) {
                            byte[] pva = (byte[])pv;
                            byte[] cva = (byte[])cv!;

                            if(pva.Length != cva.Length)
                                errors.Add($"expected length {cva.Length} but was {pva.Length} in column {pc.Field.Name}, value #{ri}");

                            for(int i = 0; i < pva.Length; i++) {
                                if(pva[i] != cva[i])
                                    errors.Add($"expected {cva[i]} but was {pva[i]} in column {pc.Field.Name}, value #{ri}, array index {i}");
                            }
                        }
                        else {
                            if(!pv.Equals(cv))
                               errors.Add($"expected {cv} but was {pv} in column {pc.Field.Name}, value #{ri}");
                        }
                    }
                }
            }

            if(errors.Count > 0) {
                Assert.Fail($"{errors.Count} error(s):" + Environment.NewLine + string.Join(Environment.NewLine, errors));
            }
        }

        private object? ChangeType(object? v, Type t) {
            if(v == null)
                return null;
            if(v.GetType() == t)
                return v;
            if(v is string s && string.IsNullOrEmpty(s))
                return null;

            if(t == typeof(DateTime)) {
                string so = (string)v;
                return DateTime.Parse(so).ToUniversalTime();
            }

            if(t == typeof(byte[])) {
                string so = (string)v;
                return Encoding.UTF8.GetBytes(so);
            }

            return Convert.ChangeType(v, t);
        }

        private async Task<(DataColumn[] Columns, IReadOnlyList<Field> Fields)> ReadParquetAsync(string name,
            bool treatByteArrayAsString) {
            await using Stream s = OpenTestFile(name);
            var parquetOptions = new ParquetOptions { TreatByteArrayAsString = treatByteArrayAsString };
            using ParquetReader pr = await ParquetReader.CreateAsync(s, parquetOptions);
            using ParquetRowGroupReader rgr = pr.OpenRowGroupReader(0);
            return (await pr.Schema.GetDataFields()
                .Select(df => rgr.ReadColumnAsync(df))
                .SequentialWhenAll(), pr.Schema.Fields);
        }

        private DataColumn[] ReadCsv(string name) {
            var columns = new List<List<string>>();

            string[]? columnNames = null;

            using(Stream fs = OpenTestFile(name)) {
                var reader = new CsvReader(fs, Encoding.UTF8);

                //header
                columnNames = reader.ReadNextRow();
                columns.AddRange(columnNames!.Select(n => new List<string>()));

                //values
                string[]? values;
                while((values = reader.ReadNextRow()) != null) {
                    for(int i = 0; i < values.Length; i++) {
                        List<string> column = columns[i];
                        column.Add(values[i]);
                    }
                }
            }

            if(columnNames == null)
                throw new InvalidOperationException("no columns?");

            var schema = new ParquetSchema(columnNames.Select(n => new DataField<string>(n)).ToList());

            //compose result
            return
               columnNames!.Select((n, i) => new DataColumn(schema.GetDataFields()[i], columns[i].ToArray()))
               .ToArray();
        }

    }
}