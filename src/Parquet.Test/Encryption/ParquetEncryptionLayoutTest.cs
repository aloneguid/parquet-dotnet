using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Parquet.Encryption;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption;

public class ParquetEncryptionLayoutTest {
    private static readonly byte[] Key = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();

    [Fact]
    public async Task EncryptedPageSizesMatchPhysicalLayout() {
        byte[] file = await WriteFileAsync(rowGroupCount: 1);
        using var stream = new MemoryStream(file, writable: false);
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, CreateOptions());
        RowGroup rowGroup = reader.Metadata!.RowGroups[0];
        ColumnChunk chunk = rowGroup.Columns[0];
        ColumnMetaData metadata = chunk.MetaData!;
        ParquetFileCryptoContext crypto = ReadCryptoContext(file);

        using var raw = new MemoryStream(file, writable: false);
        raw.Position = metadata.DataPageOffset;
        long headerOffset = raw.Position;
        byte[] header = crypto.Footer.Decrypt(
            raw,
            ParquetModuleType.DataPageHeader,
            rowGroup.Ordinal!.Value,
            0,
            0);
        long encryptedHeaderLength = raw.Position - headerOffset;
        using var headerStream = new MemoryStream(header, writable: false);
        PageHeader pageHeader = PageHeader.Read(new ThriftCompactProtocolReader(headerStream));
        ParquetCryptoContext.ValidateTrailingPadding(header, headerStream.Position, "page header");

        Assert.Equal(PageType.DATA_PAGE, pageHeader.Type);
        Assert.Equal(
            encryptedHeaderLength + pageHeader.CompressedPageSize,
            metadata.TotalCompressedSize);
        Assert.Equal(
            header.Length + pageHeader.UncompressedPageSize,
            metadata.TotalUncompressedSize);

        raw.Position = headerOffset + encryptedHeaderLength;
        int bodyPayloadLength = raw.ReadInt32();
        Assert.Equal(pageHeader.CompressedPageSize - sizeof(int), bodyPayloadLength);
    }

    [Fact]
    public async Task UsesStoredRowGroupOrdinalForAuthentication() {
        byte[] file = await WriteFileAsync(rowGroupCount: 3);
        using var stream = new MemoryStream(file, writable: false);
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, CreateOptions());

        Assert.Equal(new short?[] { 0, 1, 2 }, reader.Metadata!.RowGroups.Select(group => group.Ordinal));
        RowGroup thirdGroup = reader.Metadata.RowGroups[2];
        long dataPageOffset = thirdGroup.Columns[0].MetaData!.DataPageOffset;
        ParquetFileCryptoContext crypto = ReadCryptoContext(file);
        using var raw = new MemoryStream(file, writable: false);

        raw.Position = dataPageOffset;
        byte[] header = crypto.Footer.Decrypt(
            raw,
            ParquetModuleType.DataPageHeader,
            thirdGroup.Ordinal!.Value,
            0,
            0);
        using var headerStream = new MemoryStream(header, writable: false);
        Assert.Equal(PageType.DATA_PAGE, PageHeader.Read(new ThriftCompactProtocolReader(headerStream)).Type);

        raw.Position = dataPageOffset;
        Assert.Throws<AuthenticationTagMismatchException>(() =>
            crypto.Footer.Decrypt(raw, ParquetModuleType.DataPageHeader, 0, 0, 0));
    }

    private static async Task<byte[]> WriteFileAsync(int rowGroupCount) {
        var field = new DataField<int>("value");
        var options = new ParquetOptions {
            CompressionMethod = CompressionMethod.None,
            Encryption = new ParquetEncryptionOptions(new ParquetKey(Key)) {
                AadPrefix = "layout-tests"u8.ToArray()
            }
        };
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(field), stream, options)) {
            for(int groupIndex = 0; groupIndex < rowGroupCount; groupIndex++) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteAsync<int>(
                    field,
                    Enumerable.Range(groupIndex * 128, 128).ToArray());
            }
        }
        return stream.ToArray();
    }

    private static ParquetOptions CreateOptions() =>
        new() {
            Decryption = new ParquetDecryptionOptions { FooterKey = Key }
        };

    private static ParquetFileCryptoContext ReadCryptoContext(byte[] file) {
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - 8, 4));
        int footerOffset = checked(file.Length - 8 - footerLength);
        using var footer = new MemoryStream(
            file.AsSpan(footerOffset, footerLength).ToArray(),
            writable: false);
        FileCryptoMetaData metadata = FileCryptoMetaData.Read(new ThriftCompactProtocolReader(footer));
        return ParquetFileCryptoContext.CreateForRead(
            metadata.EncryptionAlgorithm,
            metadata.KeyMetadata,
            encryptedFooter: true,
            CreateOptions().Decryption);
    }
}
