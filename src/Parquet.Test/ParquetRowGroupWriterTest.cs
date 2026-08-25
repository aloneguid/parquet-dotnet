using System.IO;
using System.Threading.Tasks;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test;

public class ParquetRowGroupWriterTest : TestBase {

    [Fact]
    public async Task SortingColumns_round_trip_through_footer() {
        var id = new DataField<int>("id");
        var name = new DataField<string>("name");
        using var stream = new MemoryStream();

        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id, name), stream)) {
            using(ParquetRowGroupWriter rowGroup = writer.CreateRowGroup()) {
                await rowGroup.WriteAsync<int>(id, new[] { 1, 2, 3 });
                await rowGroup.WriteAsync(name, ["one", "two", "three"]);

                rowGroup.ThriftRowGroup.SortingColumns = [
                    new SortingColumn { ColumnIdx = 1, Descending = true, NullsFirst = false },
                    new SortingColumn { ColumnIdx = 0, Descending = false, NullsFirst = true }
                ];
            }
        }

        stream.Position = 0;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(stream)) {
            Assert.NotNull(reader.Metadata);
            Assert.Single(reader.Metadata!.RowGroups);

            RowGroup rowGroup = reader.Metadata.RowGroups[0];
            Assert.NotNull(rowGroup.SortingColumns);
            Assert.Equal(2, rowGroup.SortingColumns!.Count);

            Assert.Equal(1, rowGroup.SortingColumns[0].ColumnIdx);
            Assert.True(rowGroup.SortingColumns[0].Descending);
            Assert.False(rowGroup.SortingColumns[0].NullsFirst);

            Assert.Equal(0, rowGroup.SortingColumns[1].ColumnIdx);
            Assert.False(rowGroup.SortingColumns[1].Descending);
            Assert.True(rowGroup.SortingColumns[1].NullsFirst);
        }
    }
}
