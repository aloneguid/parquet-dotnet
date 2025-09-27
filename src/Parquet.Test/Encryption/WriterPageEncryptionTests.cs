using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    public class WriterPageEncryptionTests : TestBase {
        private static ParquetOptions EncryptedOptions(string key, string? aadPrefix = null)
            => new ParquetOptions {
                EncryptionKey = key,      // 16/24/32 byte (or base64/hex) – we’ll use raw UTF-8 16B below
                AADPrefix = aadPrefix     // null = prefix stored in file when writer sets supplyAadPrefix=false
            };

        [Fact]
        public async Task A_WriteEncrypted_GcmFooterKey_RoundTrip_ReadsBack() {
            // Arrange: simple schema & data
            var schema = new ParquetSchema(new DataField<string>("name"));
            var col = new DataColumn((DataField)schema.Fields[0], new[] { "a", "b", "c", "d" });

            // 16B key (AES-128) as raw UTF-8; spec requires 16/24/32B after parsing
            ParquetOptions opts = EncryptedOptions("footerKey-16byte", aadPrefix: null);

            using var ms = new MemoryStream();

            // Act: write encrypted
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                // For this step we store the AAD prefix in the file (writer default: supplyAadPrefix=false)
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(col);
            }

            // Read back
            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, opts);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            var df = (DataField)reader.Schema.Fields[0];
            DataColumn readCol = await rgr.ReadColumnAsync(df);

            // Assert: values round-trip
            string[] actual = readCol.Data.Cast<string>().AsEnumerable<string>().ToArray();
            Assert.Equal(new[] { "a", "b", "c", "d" }, actual);
        }

        [Fact]
        public async Task B_WriterSets_CryptoMetadata_OnEncryptedColumnChunks() {
            // Arrange
            var schema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("s")
            );

            var id = new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3 });
            var s = new DataColumn((DataField)schema.Fields[1], new[] { "x", "y", "z" });

            ParquetOptions opts = EncryptedOptions("footerKey-16byte", aadPrefix: null);

            using var ms = new MemoryStream();

            // Act: write encrypted
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(id);
                await rg.WriteColumnAsync(s);
            }

            // Assert: the column chunks are marked as footer-key encrypted
            ms.Position = 0;
            var actor = new ParquetActor(ms);
            await actor.ValidateFileAsync(); // sets IsEncryptedFile when PARE magic present

            FileMetaData meta = await actor.ReadMetadataAsync(
                decryptionKey: opts.EncryptionKey,
                aadPrefix: opts.AADPrefix
            );

            Assert.NotNull(meta.RowGroups);
            Assert.Single(meta.RowGroups);

            RowGroup rowGroup = meta.RowGroups[0];
            Assert.Equal(2, rowGroup.Columns.Count);

            foreach(ColumnChunk chunk in rowGroup.Columns) {
                Assert.NotNull(chunk.CryptoMetadata);
                Assert.NotNull(chunk.CryptoMetadata!.ENCRYPTIONWITHFOOTERKEY);
                Assert.Null(chunk.CryptoMetadata.ENCRYPTIONWITHCOLUMNKEY);
            }
        }

        private static string RandomB64Key(int bytes = 16) =>
            Convert.ToBase64String(RandomNumberGenerator.GetBytes(bytes));

        [Theory]
        [InlineData(false)] // AES-GCM-V1
        [InlineData(true)]  // AES-GCM-CTR-V1
        public async Task Encrypted_WithDictionary_ThenDataPage_RoundTrips(bool useCtr) {
            var field = new DataField<string>("s");
            var schema = new ParquetSchema(field);

            var opts = new ParquetOptions {
                // 128-bit key (Base64)
                EncryptionKey = RandomB64Key(16),
                // store the prefix in the file (no external supply)
                AADPrefix = null,
                SupplyAadPrefix = false,
                UseCtrVariant = useCtr,
                // force dictionary and a very small threshold so dictionary is definitely used
                UseDictionaryEncoding = true,
                DictionaryEncodingThreshold = 1.0
            };

            // Highly repetitive values to ensure a dictionary page is emitted.
            string[] values = Enumerable.Repeat("aaaa", 1000).ToArray();

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(field, values));
            }

            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms, opts);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            DataColumn col = await rgr.ReadColumnAsync(field);

            Assert.Equal(values.Length, col.Data.Length);
            Assert.All<object>(col.Data.Cast<object>(), v => Assert.Equal("aaaa", (string)v!));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Encrypted_NoDictionary_DataPageOrdinalStillStartsAtZero(bool useCtr) {
            var field = new DataField<int>("x");
            var schema = new ParquetSchema(field);

            var opts = new ParquetOptions {
                EncryptionKey = RandomB64Key(16),
                AADPrefix = null,
                SupplyAadPrefix = false,
                UseCtrVariant = useCtr,
                // Disable dictionary to ensure no dict page; first data page should be ordinal 0.
                UseDictionaryEncoding = false
            };

            int[] data = Enumerable.Range(0, 256).ToArray();

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn(field, data));
            }

            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms, opts);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            DataColumn col = await rgr.ReadColumnAsync(field);

            Assert.Equal(data, col.Data);
        }
    }
}
