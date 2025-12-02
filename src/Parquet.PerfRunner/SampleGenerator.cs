using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow;
using Parquet.Data;
using Parquet.Schema;
using ParquetSharp;
using ParquetSharp.IO;

namespace Parquet.PerfRunner {
    static class SampleGenerator {

        public const int DataSize = 1_000_000;

        public static async Task GenerateFiles() {
            int?[] data = Enumerable.Range(0, DataSize).Select(i => i < 10000 ? (int?)i : null).ToArray();

            await GenerateParquetNetFile(data);
            await GenerateParquetSharpFile(data);
        }

        private static async Task GenerateParquetNetFile(int?[] data) {

            var schema = new ParquetSchema(new DataField<int?>("ints"));

            using var fs = new FileStream("sample-pqnet.parquet", FileMode.Create, FileAccess.Write);
            using ParquetWriter pw = await ParquetWriter.CreateAsync(schema, fs);
            using(ParquetRowGroupWriter rgw = pw.CreateRowGroup()) {
                await rgw.WriteColumnAsync(new DataColumn(schema.DataFields[0], data));
            }
        }

        private static async Task GenerateParquetSharpFile(int?[] data) {
            using var fs = new FileStream("sample-pqsharp.parquet", FileMode.Create, FileAccess.Write);
            using var writer = new ManagedOutputStream(fs);
            var psc = new ParquetSharp.Column(typeof(int?), "test");
            using var fileWriter = new ParquetFileWriter(writer, new[] { psc });
            using RowGroupWriter rowGroup = fileWriter.AppendRowGroup();
            LogicalColumnWriter<int?> w = rowGroup.NextColumn().LogicalWriter<int?>();
            w.WriteBatch(data);
        }
    }
}
