// src/Parquet.Test/Encryption/Aad_CrossFileSwapTests.cs
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class Aad_CrossFileSwapTests : TestBase {

        private static async Task<byte[]> WriteSimpleEfAsync(string aadPrefix, string footerKeyHexOrB64) {
            var schema = new ParquetSchema(new DataField<int>("v"));

            var opts = new ParquetOptions {
                UsePlaintextFooter = false,
                FooterEncryptionKey = footerKeyHexOrB64,
                AADPrefix = aadPrefix,
                SupplyAadPrefix = false,
                UseCtrVariant = false,
                UseDictionaryEncoding = false,
            };

            using var ms = new MemoryStream();
            using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                w.CompressionMethod = CompressionMethod.None;
                using ParquetRowGroupWriter rg = w.CreateRowGroup();
                await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], Enumerable.Range(0, 256).ToArray()));
            }
            return ms.ToArray();
        }

        [Fact(DisplayName = "EF: swapping a GCM-encrypted data page header between files breaks auth (aad_file_unique)")]
        public async Task CrossFile_HeaderSwap_Fails() {
            string footerKey = Convert.ToBase64String(Enumerable.Range(1, 16).Select(i => (byte)i).ToArray());

            // Two files, same schema, same key, different aad_file_unique (random inside file)
            byte[] f1 = await WriteSimpleEfAsync("x-suite", footerKey);
            byte[] f2 = await WriteSimpleEfAsync("x-suite", footerKey);

            // Locate first data page header in both: at ColumnChunk.DataPageOffset
            long GetFirstHeaderOffset(byte[] file) {
                using var ms = new MemoryStream(file, writable: false);
                var actor = new ParquetActor(ms);
                actor.ValidateFileAsync().GetAwaiter().GetResult();
                FileMetaData meta = actor.ReadMetadataAsync(footerSigningKey: footerKey, footerEncryptionKey: footerKey, aadPrefix: "x-suite").GetAwaiter().GetResult();
                ColumnChunk cc0 = meta.RowGroups[0].Columns[0];
                return cc0.MetaData!.DataPageOffset;
            }

            long h1 = GetFirstHeaderOffset(f1);
            long h2 = GetFirstHeaderOffset(f2);

            static int FramedLengthAt(byte[] bytes, long offset) {
                return BitConverter.ToInt32(bytes, checked((int)offset)); // 4B length prefix
            }

            int len1 = FramedLengthAt(f1, h1);
            int len2 = FramedLengthAt(f2, h2);

            // Swap only the header frames (not bodies)
            void CopyBlock(byte[] src, long srcOff, int len, byte[] dst, long dstOff) {
                Buffer.BlockCopy(src, (int)srcOff, dst, (int)dstOff, len);
            }

            byte[] tampered = (byte[])f1.Clone();
            CopyBlock(f2, h2, 4 + len2, tampered, h1); // [len][nonce][ct][tag] header frame into file1

            // Attempt to read → should fail due to AAD mismatch (aad_file_unique differs)
            using var msT = new MemoryStream(tampered, writable: false);
            await Assert.ThrowsAnyAsync<Exception>(async () => {
                using ParquetReader r = await ParquetReader.CreateAsync(msT, new ParquetOptions {
                    FooterEncryptionKey = footerKey,
                    AADPrefix = "x-suite"
                });
                using ParquetRowGroupReader rg = r.OpenRowGroupReader(0);
                _ = await rg.ReadColumnAsync((DataField)r.Schema.Fields[0]);
            });
        }
    }
}
