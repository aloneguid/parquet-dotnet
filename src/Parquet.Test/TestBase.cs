using System;
using System.IO;
using Parquet.Data;
using System.Linq;
using F = System.IO.File;
using System.Collections.Generic;
using System.Threading.Tasks;
using Parquet.Extensions;
using Parquet.Schema;

namespace Parquet.Test {
    public class TestBase {
        protected Stream OpenTestFile(string name) {
            return F.OpenRead("./data/" + name);
        }

        protected async Task<DataColumn?> WriteReadSingleColumn(DataColumn dataColumn) {
            using var ms = new MemoryStream();
            // write with built-in extension method
            await ms.WriteSingleRowGroupParquetFileAsync(new ParquetSchema(dataColumn.Field), dataColumn);
            ms.Position = 0;

            //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

            // read first gow group and first column
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            if(reader.RowGroupCount == 0)
                return null;
            ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

            return await rgReader.ReadColumnAsync(dataColumn.Field);
        }

        protected async Task<Tuple<DataColumn[], ParquetSchema>> WriteReadSingleRowGroup(
            ParquetSchema schema, DataColumn[] columns) {
            ParquetSchema readSchema;
            using var ms = new MemoryStream();
            await ms.WriteSingleRowGroupParquetFileAsync(schema, columns);
            ms.Position = 0;

            //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            readSchema = reader.Schema;

            using ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);
            return Tuple.Create(await columns.Select(c =>
               rgReader.ReadColumnAsync(c.Field))
               .SequentialWhenAll(), readSchema);
        }

        protected async Task<object> WriteReadSingle(DataField field, object? value, CompressionMethod compressionMethod = CompressionMethod.None) {
            //for sanity, use disconnected streams
            byte[] data;

            var options = new ParquetOptions();
#if !NETCOREAPP3_1
            if(value is DateOnly)
                options.UseDateOnlyTypeForDates = true;
#endif

            using(var ms = new MemoryStream()) {
                // write single value

                using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), ms, options)) {
                    writer.CompressionMethod = compressionMethod;

                    using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                    Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
                    dataArray.SetValue(value, 0);
                    var column = new DataColumn(field, dataArray);

                    await rg.WriteColumnAsync(column);
                }

                data = ms.ToArray();
            }

            using(var ms = new MemoryStream(data)) {
                // read back single value

                ms.Position = 0;
                using ParquetReader reader = await ParquetReader.CreateAsync(ms);
                using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
                DataColumn column = await rowGroupReader.ReadColumnAsync(field);

                return column.Data.GetValue(0)!;
            }
        }

        protected async Task<List<DataColumn>> ReadColumns(Stream s) {
            using ParquetReader reader = await ParquetReader.CreateAsync(s);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var r = new List<DataColumn>();
            foreach(DataField df in reader.Schema.DataFields) {
                DataColumn dc = await rgr.ReadColumnAsync(df);
                r.Add(dc);
            }
            return r;
        }
    }
}