using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.PageIndex;

public class PageIndexScanFallbackTest {
    private static readonly byte[] Key = Enumerable.Range(1, 16).Select(value => (byte)value).ToArray();

    [Fact]
    public async Task ScansPageIndexesWhenFooterReferencesAreMissing() {
        var field = new DataField<int>("value");
        using MemoryStream stream = await WriteAsync(field, [10, 20, 30, 40, 50]);

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        stream.Position = 7;

        OffsetIndex offsetIndex = await rowGroup.GetOrCreateOffsetIndexAsync(field);
        ColumnIndex columnIndex = Assert.IsType<ColumnIndex>(
            await rowGroup.GetOrCreateColumnIndexAsync(field));

        Assert.Equal(7, stream.Position);
        Assert.Equal([0L], offsetIndex.PageLocations.Select(page => page.FirstRowIndex));
        Assert.Single(columnIndex.MinValues);
        Assert.Single(columnIndex.MaxValues);
        Assert.Equal([0L], columnIndex.NullCounts);
    }

    [Fact]
    public async Task ScansEncryptedPageIndexesWhenFooterReferencesAreMissing() {
        var field = new DataField<int>("value");
        using MemoryStream stream = await WriteAsync(
            field,
            [1, 2, 3, 4],
            new ParquetOptions {
                Encryption = new ParquetEncryptionOptions(new ParquetKey(Key))
            });

        var readOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = Key }
        };
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, readOptions);
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);

        OffsetIndex offsetIndex = await rowGroup.GetOrCreateOffsetIndexAsync(field);
        ColumnIndex columnIndex = Assert.IsType<ColumnIndex>(
            await rowGroup.GetOrCreateColumnIndexAsync(field));

        Assert.Equal([0L], offsetIndex.PageLocations.Select(page => page.FirstRowIndex));
        Assert.Single(columnIndex.MinValues);
    }

    private static async Task<MemoryStream> WriteAsync(
        DataField<int> field,
        int[] values,
        ParquetOptions? options = null) {
        var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field),
                        stream,
                        options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, values.AsMemory());
        }
        stream.Position = 0;
        return stream;
    }
}
