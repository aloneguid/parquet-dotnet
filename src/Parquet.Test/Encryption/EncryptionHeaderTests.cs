using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Encryption;
namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class EncryptionHeaderTests {
        [Fact]
        public async Task Gcm_HeaderSizesAndOffsets_Match_OnDisk_Frames() {
            // --- Arrange: write a tiny encrypted file (GCM, AAD prefix stored) ---
            string outDir = Path.Combine(Environment.CurrentDirectory, "interop-artifacts");
            Directory.CreateDirectory(outDir);
            string path = Path.Combine(outDir, "gcm_sizes_offsets.parquet");

            var schema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("name")
            );

            var opts = new ParquetOptions {
                FooterEncryptionKey = "footerKey-16byte",
                AADPrefix = "mr-suite",
                SupplyAadPrefix = false,      // store prefix in file
                UseCtrVariant = false
            };

            using(FileStream fs = System.IO.File.Create(path))
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, fs, formatOptions: opts)) {
                writer.CompressionMethod = CompressionMethod.None; // keep simple
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3, 4 }));
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], new[] { "alice", "bob", "carol", "dave" }));
            }

            // --- Act: open with reader so we have metadata + decrypter ---
            using ParquetReader reader = await ParquetReader.CreateAsync(path, new ParquetOptions {
                FooterEncryptionKey = "footerKey-16byte",
                AADPrefix = "mr-suite"
            });

            FileMetaData meta = reader.Metadata!;

            RowGroup rg0 = meta.RowGroups[0];
            ColumnChunk cc0 = rg0.Columns[0];                // first column: "id"
            short rgOrd = rg0.Ordinal!.Value;
            short colOrd = (short)rg0.Columns.IndexOf(cc0);
            EncryptionBase decr = meta.Decrypter!;

            // Start position should be exactly the column chunk's DataPageOffset
            long expectedFirstHeaderOffset = cc0.MetaData!.DataPageOffset;

            long encTotal = 0;      // on-disk total (encHeader + encBody for all pages)
            long plainTotal = 0;    // plaintext total (header bytes + uncompressed body sizes)
            int valuesSeen = 0;
            short pageOrd = 0;

            using FileStream raw = System.IO.File.OpenRead(path);
            raw.Seek(expectedFirstHeaderOffset, SeekOrigin.Begin);
            Assert.Equal(expectedFirstHeaderOffset, raw.Position);

            while(valuesSeen < cc0.MetaData.NumValues) {
                long posBeforeHeader = raw.Position;

                // Decrypt header (this consumes the encrypted header bytes from the stream)
                byte[] decHeader = decr.DecryptDataPageHeader(
                    new ThriftCompactProtocolReader(raw), rgOrd, colOrd, pageOrd);

                long posAfterHeader = raw.Position;
                int encHeaderLen = checked((int)(posAfterHeader - posBeforeHeader)); // bytes we just consumed on disk

                // Parse plaintext PageHeader to get sizes
                PageHeader ph;
                using(var ms = new MemoryStream(decHeader, writable: false))
                    ph = PageHeader.Read(new ThriftCompactProtocolReader(ms));

                Assert.Equal(PageType.DATA_PAGE, ph.Type);

                // The encrypted BODY follows immediately; parquet-mr will read exactly ph.CompressedPageSize bytes
                int encBodyLen = ph.CompressedPageSize;

                // Sanity: the very first header offset should equal DataPageOffset
                if(pageOrd == 0)
                    Assert.Equal(expectedFirstHeaderOffset, posBeforeHeader);

                // Skip over the encrypted body to the next page header (or end of chunk)
                raw.Seek(encBodyLen, SeekOrigin.Current);

                // Accumulate counts
                encTotal += encHeaderLen + encBodyLen;
                plainTotal += decHeader.Length + ph.UncompressedPageSize;
                valuesSeen += ph.DataPageHeader!.NumValues;
                pageOrd++;
            }

            // --- Assert: metadata totals match what was physically written ---
            Assert.Equal(encTotal, cc0.MetaData.TotalCompressedSize);
            Assert.Equal(plainTotal, cc0.MetaData.TotalUncompressedSize);
        }
    }
}