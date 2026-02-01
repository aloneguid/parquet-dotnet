using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;
using F = System.IO.File;
using Path = System.IO.Path;

namespace Parquet.Test.Integration {
    public class WriteQuestionableTypesTest : IntegrationBase {

        private async Task<string> ReadWithPQT(ParquetSchema schema, DataColumn dc) {
            string testFileName = Path.GetFullPath($"temp.{nameof(WriteQuestionableTypesTest)}.parquet");
            if(F.Exists(testFileName))
                F.Delete(testFileName);

            await using(Stream s = F.OpenWrite(testFileName))
            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, s)) {
                using ParquetRowGroupWriter rgw = writer.CreateRowGroup();

                await rgw.WriteColumnAsync(dc);
            }

            string? json = ExecMrCat(testFileName);
            return json ?? string.Empty;
        }

        [Fact]
        public async Task DateTime_Default() {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                Assert.Skip("Not supported on macOS");
            
            if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
               RuntimeInformation.OSArchitecture == Architecture.X86) {
                Assert.Skip("Not supported on Windows x86");
            }

            var schema = new ParquetSchema(new DataField<DateTime>("qtype"));
            var dc = new DataColumn(schema.DataFields.First(), new[] { new DateTime(2023, 04, 25, 1, 2, 3) });
            string json = await ReadWithPQT(schema, dc);
            Assert.Equal("{\"qtype\":\"AK4X1GIDAACciSUA\"}", json);
        }

        [Fact]
        public async Task Timestamp_Default() {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                Assert.Skip("Not supported on macOS");
            
            if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
               RuntimeInformation.OSArchitecture == Architecture.X86) {
                Assert.Skip("Not supported on Windows x86");
            }

            var schema = new ParquetSchema(new DataField<TimeSpan>("qtype"));
            var dc = new DataColumn(schema.DataFields.First(), new[] { TimeSpan.FromHours(7) });
            string json = await ReadWithPQT(schema, dc);
            Assert.Equal("{\"qtype\":25200000}", json);
        }
    }
}
