using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class PlaintextFooterReaderTests {
        private static ParquetSchema OneIntSchema()
            => new ParquetSchema(new DataField<int>("id"));

        private static DataColumn OneIntColumn(params int[] values)
            => new DataColumn((DataField)OneIntSchema().Fields[0], values);

        private static async Task<byte[]> WriteAsync(ParquetOptions opts, params int[] values) {
            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(OneIntSchema(), ms, opts)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();
                await rg.WriteColumnAsync(OneIntColumn(values));
            }
            return ms.ToArray();
        }

        private static async Task<int[]> ReadAllAsync(byte[] bytes, ParquetOptions opts) {
            using var ms = new MemoryStream(bytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, opts);
            using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);
            DataColumn col = await rg.ReadColumnAsync((DataField)reader.Schema.Fields[0]);
            return col.AsSpan<int>().ToArray();
        }

#if NET6_0_OR_GREATER || NETSTANDARD2_1
        [Fact]
        public async Task SignedPlaintextFooter_WithStoredAadPrefix_ReadsOK_WithoutSupplyingPrefix() {
            var opts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = "00112233445566778899AABBCCDDEEFF", // hex 16B
                AADPrefix = "table=users/part=0",
                SupplyAadPrefix = false, // stored in file
            };

            byte[] file = await WriteAsync(opts, 1, 2, 3);

            // Read: do not provide aad prefix — should succeed because it was stored
            var readOpts = new ParquetOptions { FooterSigningKey = opts.FooterSigningKey };
            int[] vals = await ReadAllAsync(file, readOpts);

            Assert.Equal(new[] { 1, 2, 3 }, vals);
        }

        [Fact]
        public async Task SignedPlaintextFooter_WithSupplyAadPrefix_RequiresPrefixOnRead() {
            var opts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = "00112233445566778899AABBCCDDEEFF",
                FooterEncryptionKey = "0102030405060708090A0B0C0D0E0F10",
                AADPrefix = "dataset=events/date=2025-09-28",
                SupplyAadPrefix = true, // not stored, must be supplied by readers
            };
            byte[] file = await WriteAsync(opts, 10, 20);

            // Without prefix: should fail with a clear error
            var readOptsMissing = new ParquetOptions {
                FooterSigningKey = opts.FooterSigningKey,
                FooterEncryptionKey = opts.FooterEncryptionKey
            };
            await Assert.ThrowsAsync<InvalidDataException>(async () => {
                await ReadAllAsync(file, readOptsMissing);
            });

            // With correct prefix: succeeds
            var readOpts = new ParquetOptions {
                FooterEncryptionKey = opts.FooterEncryptionKey,
                FooterSigningKey = opts.FooterSigningKey,
                AADPrefix = opts.AADPrefix
            };
            int[] vals = await ReadAllAsync(file, readOpts);
            Assert.Equal(new[] { 10, 20 }, vals);
        }

        [Fact]
        public async Task SignedPlaintextFooter_TamperTag_FailsVerification() {
            var opts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = "00112233445566778899AABBCCDDEEFF",
                AADPrefix = "anything",
                SupplyAadPrefix = false
            };
            byte[] ok = await WriteAsync(opts, 7, 8, 9);

            // File layout (tail): [footerPlain][nonce(12)][tag(16)][len(4)][ 'PAR1' ]
            // Flip the last byte of the tag to break the signature.
            byte[] bad = (byte[])ok.Clone();
            // position of last 4 bytes magic:
            int magicOff = bad.Length - 4;
            // read the combined length (little-endian) sitting right before magic
            int lenOff = bad.Length - 8;
            int combinedLen = BitConverter.ToInt32(bad.AsSpan(lenOff, 4)); // footer+28
            int tagStart = magicOff - 4 /*len*/ - combinedLen + (combinedLen - 28) + 12; // footerStart + footerLen + 12
            int lastTagByte = tagStart + 15;
            bad[lastTagByte] ^= 0xFF; // flip

            var readOpts = new ParquetOptions { FooterSigningKey = opts.FooterSigningKey };
            await Assert.ThrowsAsync<InvalidDataException>(async () => {
                await ReadAllAsync(bad, readOpts);
            });
        }
#endif

        [Fact]
        public async Task LegacyPlaintextFooter_NoSigning_LoadsAndReads() {
            // No SecretKey, no encryption/signing at all.
            var opts = new ParquetOptions();
            byte[] file = await WriteAsync(opts, 4, 5, 6);

            var readOpts = new ParquetOptions();
            int[] vals = await ReadAllAsync(file, readOpts);

            Assert.Equal(new[] { 4, 5, 6 }, vals);
        }

#if NET6_0_OR_GREATER || NETSTANDARD2_1
        [Fact]
        public async Task EncryptedFooter_ModeStillWorks() {
            var opts = new ParquetOptions {
                UsePlaintextFooter = false,      // encrypted footer mode
                FooterSigningKey = Convert.ToBase64String(Enumerable.Range(0, 16).Select(i => (byte)i).ToArray()),
                AADPrefix = "tbl=orders/part=42",
                SupplyAadPrefix = false
            };
            byte[] file = await WriteAsync(opts, 100, 200, 300);

            var readOpts = new ParquetOptions { FooterSigningKey = opts.FooterSigningKey };
            int[] vals = await ReadAllAsync(file, readOpts);
            Assert.Equal(new[] { 100, 200, 300 }, vals);
        }
#endif

#if NET6_0_OR_GREATER || NETSTANDARD2_1
        [Fact]
        public async Task EncryptedFooter_MissingKey_Fails() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            byte[] file;
            using(var ms = new MemoryStream()) {
                var opts = new ParquetOptions { FooterEncryptionKey = "00112233445566778899AABBCCDDEEFF" };
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2 }));
                }
                file = ms.ToArray();
            }

            using var ms2 = new MemoryStream(file);
            await Assert.ThrowsAsync<InvalidDataException>(async () => {
                // no key provided
                using ParquetReader _ = await ParquetReader.CreateAsync(ms2, new ParquetOptions());
            });
        }
#endif

#if NET6_0_OR_GREATER || NETSTANDARD2_1
        [Fact]
        public async Task SignedPlaintextFooter_WrongAadPrefix_Fails() {
            var schema = new ParquetSchema(new DataField<int>("n"));
            byte[] file;
            var opts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = "00112233445566778899AABBCCDDEEFF",
                AADPrefix = "dataset=A",
                SupplyAadPrefix = true
            };
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 5 }));
                } // ← w.Dispose() runs here, writing [footer][nonce|tag][len][PAR1]
                file = ms.ToArray(); // ← now it's safe to capture the final bytes
            }

            var readOpts = new ParquetOptions { FooterSigningKey = opts.FooterSigningKey, AADPrefix = "dataset=B" };
            using var ms2 = new MemoryStream(file);
            await Assert.ThrowsAsync<InvalidDataException>(async () => {
                using ParquetReader r = await ParquetReader.CreateAsync(ms2, readOpts);
                _ = r.Schema; // force read
            });
        }
#endif
    }
}