using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Test.Xunit;
using Xunit;
using F = System.IO.File;
using Path = System.IO.Path;

namespace Parquet.Test.Integration;

public class WriteQuestionableTypesTest : IntegrationBase {

    private async Task<string> ReadWithPQT<T>(ParquetSchema schema, DataField df, ReadOnlyMemory<T> values) where T : struct {
        string testFileName = Path.GetFullPath($"temp.{nameof(WriteQuestionableTypesTest)}.parquet");
        if(F.Exists(testFileName))
            F.Delete(testFileName);

        await using(Stream s = F.OpenWrite(testFileName))
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, s)) {
            using ParquetRowGroupWriter rgw = writer.CreateRowGroup();
            await rgw.WriteAsync(df, values);
        }

        string? json = ExecMrCat(testFileName);
        return json ?? string.Empty;
    }

    [NonMacOSFact]
    public async Task DateTime_Default() {
        var schema = new ParquetSchema(new DataField<DateTime>("qtype"));
        DateTime[] values = new[] { new DateTime(2023, 04, 25, 1, 2, 3) };
        string json = await ReadWithPQT<DateTime>(schema, schema.DataFields.First(), values.AsMemory());
        Assert.Equal("{\"qtype\":\"AK4X1GIDAACciSUA\"}", json);
    }

    [NonMacOSFact]
    public async Task Timestamp_Default() {
        var schema = new ParquetSchema(new TimeDataField("qtype", TimeUnitPrecision.Millis));
        int[] values = new[] { (int)TimeSpan.FromHours(7).TotalMilliseconds };
        string json = await ReadWithPQT<int>(schema, schema.DataFields.First(), values.AsMemory());
        Assert.Equal("{\"qtype\":25200000}", json);
    }
}
