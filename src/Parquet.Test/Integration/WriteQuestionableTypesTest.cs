using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Test.Xunit;
using Xunit;
using F = System.IO.File;
using Path = System.IO.Path;

namespace Parquet.Test.Integration {
    public class WriteQuestionableTypesTest : IntegrationBase {

        private async Task<string> ReadWithPQT(ParquetSchema schema, DataColumn dc) {
            string testFileName = Path.GetFullPath($"temp.{nameof(WriteQuestionableTypesTest)}.parquet");
            if(F.Exists(testFileName))
                F.Delete(testFileName);

            using(Stream s = F.OpenWrite(testFileName)) {
                using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, s)) {
                    using ParquetRowGroupWriter rgw = writer.CreateRowGroup();

                    await rgw.WriteColumnAsync(dc);
                }
            }

            string? json = ExecMrCat(testFileName);
            return json ?? string.Empty;
        }

        [SkipOnMac]
        public async Task DateTime_Default() {
            var schema = new ParquetSchema(new DataField<DateTime>("qtype"));
            var dc = new DataColumn(schema.DataFields.First(), new[] { new DateTime(2023, 04, 25, 1, 2, 3) });
            string json = await ReadWithPQT(schema, dc);
            Assert.Equal("{\"qtype\":\"AK4X1GIDAACciSUA\"}", json);
        }

        [SkipOnMac]
        public async Task Timestamp_Default() {
            var schema = new ParquetSchema(new DataField<TimeSpan>("qtype"));
            var dc = new DataColumn(schema.DataFields.First(), new[] { TimeSpan.FromHours(7) });
            string json = await ReadWithPQT(schema, dc);
            Assert.Equal("{\"qtype\":25200000}", json);
        }
    }
}
