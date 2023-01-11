using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks {
    internal class DataTypes {

        private static Random random = new Random();
        public static string RandomString(int length) {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public async Task SimpleIntWriteRead() {

            // allocate stream large enough to avoid re-allocations during performance test
            const int l = 10000000;
            var ms = new MemoryStream(l * sizeof(int) * 2);
            var schema = new ParquetSchema(new DataField<int>("id"));
            var rnd = new Random();
            int[] ints = new int[l];
            for(int i = 0; i < l; i++) {
                ints[i] = rnd.Next();
            }

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                writer.CompressionMethod = CompressionMethod.None;
                using(ParquetRowGroupWriter g = writer.CreateRowGroup()) {
                    await g.WriteColumnAsync(new DataColumn((DataField)schema[0], ints));
                }
            }

            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                using(ParquetRowGroupReader g = reader.OpenRowGroupReader(0)) {
                    DataColumn data = await g.ReadColumnAsync((DataField)schema[0]);
                }
            }
        }

        public async Task SimpleStringWriteRead() {


            var col = new DataColumn(new DataField<string>("c"), Enumerable.Range(0, 100000).Select(i => RandomString(100)).ToArray());
            var f = (DataField)col.Field;
            var ms = new MemoryStream();
            var schema = new ParquetSchema(col.Field);

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                writer.CompressionMethod = CompressionMethod.None;
                using(ParquetRowGroupWriter g = writer.CreateRowGroup()) {
                    await g.WriteColumnAsync(col);
                }
            }

            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                using(ParquetRowGroupReader g = reader.OpenRowGroupReader(0)) {
                    DataColumn data = await g.ReadColumnAsync(f);
                }
            }
        }

        public async Task WriteRandomStrings() {


            var col = new DataColumn(new DataField<string>("c"), Enumerable.Range(0, 100000).Select(i => RandomString(100)).ToArray());
            var f = (DataField)col.Field;
            var ms = new MemoryStream();
            var schema = new ParquetSchema(col.Field);

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                writer.CompressionMethod = CompressionMethod.None;
                using(ParquetRowGroupWriter g = writer.CreateRowGroup()) {
                    await g.WriteColumnAsync(col);
                }
            }
        }
    }
}
