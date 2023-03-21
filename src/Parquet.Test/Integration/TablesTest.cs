using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Xunit;
using F = System.IO.File;
using Path = System.IO.Path;

namespace Parquet.Test.Integration {
    /// <summary>
    /// This class does some fairly basic integration tests by compring results with parquet-mr using parquet-tools jar package.
    /// You must have java available in PATH.
    /// </summary>
    public class TablesTest : IntegrationBase {

        private async Task CompareWithMr(Table t, Func<string, string>? jsonPreprocessor = null) {
            string testFileName = Path.GetFullPath("temp.parquet");

            if(F.Exists(testFileName))
                F.Delete(testFileName);

            //produce file
            using(Stream s = F.OpenWrite(testFileName)) {
                using(ParquetWriter writer = await ParquetWriter.CreateAsync(t.Schema, s)) {
                    await writer.WriteAsync(t);
                }
            }

            //F.Copy(testFileName, "c:\\tmp\\mr.parquet", true);

            //read back
            Table t2 = await ParquetReader.ReadTableFromFileAsync(testFileName);

            //check we don't have a bug internally before launching MR
            Assert.Equal(t.ToString("j"), t2.ToString("j"), ignoreLineEndingDifferences: true);

            string myJson = t.ToString("j");
            string? mrJson = ExecMrCat(testFileName);

            if(jsonPreprocessor != null) {
                myJson = jsonPreprocessor(myJson);
            }

            Assert.Equal(myJson, mrJson);
        }



        [Fact]
        public async Task Integers_all_types() {
            var table = new Table(new DataField<sbyte>("int8"), new DataField<byte>("uint8"),
               new DataField<short>("int16"), new DataField<ushort>("uint16"),
               new DataField<int>("int32"), new DataField<long>("int64"));

            //generate fake data
            for(int i = 0; i < 1000; i++) {
                table.Add(new Row((sbyte)((i % 127) - 255), (byte)(i % 255), (short)i, (ushort)i, i, (long)i));
            }

            await CompareWithMr(table);
        }

        [Fact]
        public async Task Flat_simple_table() {
            var table = new Table(new DataField<int>("id"), new DataField<string>("city"));

            //generate fake data
            for(int i = 0; i < 1000; i++) {
                table.Add(new Row(i, "record#" + i));
            }

            await CompareWithMr(table);
        }

        [Fact]
        public async Task IntegerIds_and_array_of_strings() {
            var table = new Table(
               new DataField<int>("id"),
               new DataField<string[]>("categories")     //array field
            );

            table.Add(1, new[] { "1", "2", "3" });
            table.Add(3, new[] { "3", "3", "3" });

            await CompareWithMr(table);
        }

        [Fact]
        public async Task Plain_Dictionary_encoding() {
            var table = new Table(
               new DataField<string>("string")
            );

            for(int i = 0; i < 100; i++) {
                table.Add("one");
            }

            for(int i = 0; i < 100; i++) {
                table.Add("two");
            }

            for(int i = 0; i < 100; i++) {
                table.Add((string?)null);
            }

            for(int i = 0; i < 100; i++) {
                table.Add("three");
            }

            await CompareWithMr(table,
                s => s.Replace("\"string\":null", ""));
        }

        [Fact]
        public async Task Byte_arrays()
        {
            var table = new Table(new DataField<byte[]>("ars"));
            for (int i = 0; i < 100; i++)
            {
                table.Add(Encoding.UTF8.GetBytes($"string {i}"));
            }

            await CompareWithMr(table);
        }
    }
}