﻿using NetBox.FileFormats;
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
        protected async Task CompareFilesAsync(string baseName, string encoding, string dataPageVersion, bool treatByteArrayAsString, params Type[] columnTypes) {
            string parquetFilePrefix = $"{baseName}";
            if(!string.IsNullOrEmpty(encoding)) {
                parquetFilePrefix += $".{encoding}";
            }
            if(!string.IsNullOrEmpty(dataPageVersion)) {
                parquetFilePrefix += $".{dataPageVersion}";
            }
            DataColumn[] parquet = await ReadParquetAsync($"{parquetFilePrefix}.parquet", treatByteArrayAsString);
            DataColumn[] csv = ReadCsv($"{baseName}.csv");
            Compare(parquet, csv, columnTypes);
        }

        private void Compare(DataColumn[] parquet, DataColumn[] csv, Type[] columnTypes) {
            //compar number of columns is the same
            Assert.Equal(parquet.Length, csv.Length);

            //compare column names
            for(int i = 0; i < parquet.Length; i++) {
                Assert.Contains(csv, dc => dc.Field.Name == parquet[i].Field.Name);
            }

            //compare column values one by one
            for(int ci = 0; ci < parquet.Length; ci++) {
                DataColumn pc = parquet[ci];
                DataColumn cc = csv[ci];

                for(int ri = 0; ri < pc.Data.Length; ri++) {
                    Type clrType = pc.Field.ClrType;
                    object pv = pc.Data.GetValue(ri);
                    object cv = ChangeType(cc.Data.GetValue(ri), clrType);

                    if(pv == null) {
                        bool isCsvNull =
                           cv == null ||
                           (cv is string s && s == string.Empty);

                        Assert.True(isCsvNull,
                           $"expected null value in column {pc.Field.Name}, value #{ri}");
                    }
                    else {
                        if(clrType == typeof(string)) {
                            Assert.True(((string)pv).Trim() == ((string)cv).Trim(),
                               $"expected {cv} but was {pv} in column {pc.Field.Name}, value #{ri}");
                        }
                        else if(clrType == typeof(byte[])) {
                            byte[] pva = (byte[])pv;
                            byte[] cva = (byte[])cv;
                            Assert.True(pva.Length == cva.Length, $"expected length {cva.Length} but was {pva.Length} in column {pc.Field.Name}, value #{ri}");
                            for(int i = 0; i < pva.Length; i++) {
                                Assert.True(pva[i] == cva[i], $"expected {cva[i]} but was {pva[i]} in column {pc.Field.Name}, value #{ri}, array index {i}");
                            }
                        }
                        else {
                            Assert.True(pv.Equals(cv),
                               $"expected {cv} but was {pv} in column {pc.Field.Name}, value #{ri}");
                        }
                    }
                }
            }
        }

        private object ChangeType(object v, Type t) {
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

        private async Task<DataColumn[]> ReadParquetAsync(string name, bool treatByteArrayAsString) {
            using(Stream s = OpenTestFile(name)) {
                using(ParquetReader pr = await ParquetReader.CreateAsync(
                    s, new ParquetOptions { TreatByteArrayAsString = treatByteArrayAsString })) {
                    using(ParquetRowGroupReader rgr = pr.OpenRowGroupReader(0)) {
                        return await pr.Schema.GetDataFields()
                           .Select(df => rgr.ReadColumnAsync(df))
                           .SequentialWhenAll();
                    }
                }
            }
        }

        private DataColumn[] ReadCsv(string name) {
            var columns = new List<List<string>>();

            string[] columnNames = null;

            using(Stream fs = OpenTestFile(name)) {
                var reader = new CsvReader(fs, Encoding.UTF8);

                //header
                columnNames = reader.ReadNextRow();
                columns.AddRange(columnNames.Select(n => new List<string>()));

                //values
                string[] values;
                while((values = reader.ReadNextRow()) != null) {
                    for(int i = 0; i < values.Length; i++) {
                        List<string> column = columns[i];
                        column.Add(values[i]);
                    }
                }
            }

            //compose result
            return
               columnNames.Select((n, i) => new DataColumn(new DataField<string>(n), columns[i].ToArray()))
               .ToArray();
        }

    }
}