using System;
using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test;

public class StatisticsTest : TestBase {

    [Fact]
    public async Task Int32_null_min_max() {
        var schema = new ParquetSchema(new DataField<int>("id"));
        var ms = new MemoryStream();
        await using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms)) {
            using ParquetRowGroupWriter rg = w.CreateRowGroup();
            await rg.WriteAsync<int>(schema.DataFields[0], new int[] { 4, 2, 1, 3, 5, 1, 4 });
        }

        // read back
        ms.Position = 0;
        await using(ParquetReader r = await ParquetReader.CreateAsync(ms)) {
            using ParquetRowGroupReader rgr = r.OpenRowGroupReader(0);
            DataColumn dc = await rgr.ReadColumnAsync(schema.DataFields[0]);
            Assert.Equal(0, dc.Statistics.NullCount);
            Assert.Equal(1, dc.Statistics.MinValue);
            Assert.Equal(5, dc.Statistics.MaxValue);
        }
    }

}