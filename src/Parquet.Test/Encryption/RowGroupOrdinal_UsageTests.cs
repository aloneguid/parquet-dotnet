// src/Parquet.Test/Encryption/RowGroupOrdinal_UsageTests.cs
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Meta.Proto;
using Xunit;
using Parquet.Meta;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class RowGroupOrdinal_UsageTests : TestBase {

        [Fact(DisplayName = "Decrypter can use RowGroup.ordinal from footer (not a local counter)")]
        public async Task Uses_Stored_RowGroup_Ordinal() {
            // Write EF file with 3 row groups
            var schema = new ParquetSchema(new DataField<int>("v"));
            var opts = new ParquetOptions {
                FooterEncryptionKey = Convert.ToBase64String(Enumerable.Range(1, 16).Select(i => (byte)i).ToArray()),
                AADPrefix = "rg-ordinal",
                SupplyAadPrefix = false,
                UseDictionaryEncoding = false,
            };

            byte[] bytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                    w.CompressionMethod = CompressionMethod.None;
                    for(int g = 0; g < 3; g++) {
                        using ParquetRowGroupWriter rg = w.CreateRowGroup();
                        await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], Enumerable.Range(g * 10, 10).ToArray()));
                    }
                }
                bytes = ms.ToArray();
            }

            using var raw = new MemoryStream(bytes, writable: false);
            var actor = new ParquetActor(raw);
            await actor.ValidateFileAsync();
            FileMetaData meta = await actor.ReadMetadataAsync(footerSigningKey: opts.FooterEncryptionKey, footerEncryptionKey: opts.FooterEncryptionKey, aadPrefix: null);

            // Target row group 2 (ordinal should be 2)
            RowGroup rg2 = meta.RowGroups[2];
            short storedOrdinal = rg2.Ordinal!.Value;
            Assert.Equal((short)2, storedOrdinal);

            ColumnChunk cc0 = rg2.Columns[0];
            long headerOffset = cc0.MetaData!.DataPageOffset;

            // Move to header and decrypt using STORED ordinal (not a local counter)
            raw.Position = headerOffset;
            byte[] decHeader = meta.Decrypter!.DecryptDataPageHeader(new ThriftCompactProtocolReader(raw), storedOrdinal, columnOrdinal: 0, pageOrdinal: 0);

            // Sanity: parse header
            PageHeader ph;
            using var msHdr = new MemoryStream(decHeader, writable: false);
            ph = PageHeader.Read(new ThriftCompactProtocolReader(msHdr));
            Assert.Equal(PageType.DATA_PAGE, ph.Type);

            // Negative: using wrong ordinal must fail
            raw.Position = headerOffset;
            Assert.ThrowsAny<Exception>(() => {
                _ = meta.Decrypter!.DecryptDataPageHeader(new ThriftCompactProtocolReader(raw), rowGroupOrdinal: 0, columnOrdinal: 0, pageOrdinal: 0);
            });
        }
    }
}
