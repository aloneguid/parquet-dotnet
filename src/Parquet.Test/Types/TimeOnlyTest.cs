using System;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types;

public class TimeOnlyTest : TestBase {
    [Fact]
    public async Task Time_only_field_created_with_pyarrow_v22() {
        using ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile("time_only_pyarrow_v22.parquet"));
        using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(0);

        Assert.Equal(4626, groupReader.RowCount);
        DataField[] fs = reader.Schema.GetDataFields();
        Assert.Equal(2, fs.Length);

        DataColumn timeData = await groupReader.ReadColumnAsync(fs[1]);
        Assert.Equal(TimeSpan.FromTicks(215720000000), timeData.Data.GetValue(0));
    }
}
