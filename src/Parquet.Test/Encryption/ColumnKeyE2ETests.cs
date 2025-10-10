using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Xunit;
using Parquet.Encryption;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class ColumnKeyE2ETests : TestBase {

        private static ParquetSchema MakeSchema() => new(
          new DataField<string>("name"),
          new DataField<int>("age"),
          new DataField<double>("salary"),
          new DataField<string>("ssn")
        );

        private static MemoryStream AsLengthPrefixedStream(byte[] payload) {
            if(payload.Length >= 4) {
                // Interpret first 4 bytes as LE length
                int len = BitConverter.ToInt32(payload, 0);
                // If that length equals the remaining bytes, it's already [len|frame]
                if(len == payload.Length - 4 && len >= 12 + 16) // at least nonce+tag
                {
                    return new MemoryStream(payload, writable: false);
                }
            }

            // Otherwise, wrap as [len (LE)] + payload
            var ms = new MemoryStream(capacity: 4 + payload.Length);
            byte[] lenLE = BitConverter.GetBytes(payload.Length);
            if(!BitConverter.IsLittleEndian)
                Array.Reverse(lenLE);
            ms.Write(lenLE, 0, 4);
            ms.Write(payload, 0, payload.Length);
            ms.Position = 0;
            return ms;
        }



        private static (DataColumn name, DataColumn age, DataColumn salary, DataColumn ssn)
        MakeColumns(ParquetSchema s) {
            var fName = (DataField)s.Fields[0];
            var fAge = (DataField)s.Fields[1];
            var fSalary = (DataField)s.Fields[2];
            var fSsn = (DataField)s.Fields[3];

            return (
              new DataColumn(fName, new[] { "alice", "bob", "carol", "dave" }),
              new DataColumn(fAge, new[] { 31, 29, 41, 38 }),
              new DataColumn(fSalary, new[] { 100_000.0, 75_500.0, 120_250.0, 88_000.0 }),
              new DataColumn(fSsn, new[] { "111-22-3333", "222-33-4444", "333-44-5555", "444-55-6666" })
            );
        }

        // Handy constants (16B keys, accepted by ParseKeyString as raw UTF-8)
        private const string FooterKey = "01234567891234FO";
        private const string SalaryKey = "01234567891234SA";
        private const string SsnKey = "01234567891234SS";

        // --- 1) Encrypted footer mode: success with keys, failure without ---

        [Fact]
        public async Task EF_ColumnKeys_Enable_Full_Read_And_MissingKeys_Fails_EncryptedCols() {
            ParquetSchema schema = MakeSchema();
            (DataColumn? cName, DataColumn? cAge, DataColumn? cSalary, DataColumn? cSsn) = MakeColumns(schema);

            byte[] bytes;
            // Write: EF mode, two encrypted columns with column-specific keys
            var writeOpts = new ParquetOptions {
                FooterEncryptionKey = FooterKey,
            };
            writeOpts.ColumnKeys["salary"] = new ParquetOptions.ColumnKeySpec(SalaryKey, Encoding.ASCII.GetBytes("kmeta:salary/v1"));
            writeOpts.ColumnKeys["ssn"] = new ParquetOptions.ColumnKeySpec(SsnKey, Encoding.ASCII.GetBytes("kmeta:ssn/v1"));

            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, writeOpts)) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(cName);
                    await rg.WriteColumnAsync(cAge);
                    await rg.WriteColumnAsync(cSalary);
                    await rg.WriteColumnAsync(cSsn);
                }
                bytes = ms.ToArray();
            }

            // Positive: provide resolver → can read all columns
            var readAllOpts = new ParquetOptions {
                FooterEncryptionKey = FooterKey,
                ColumnKeyResolver = (path, kmeta) => string.Join(".", path) switch {
                    "salary" => SalaryKey,
                    "ssn" => SsnKey,
                    _ => null
                }
            };

            using(var ms = new MemoryStream(bytes, writable: false))
            using(ParquetReader r = await ParquetReader.CreateAsync(ms, readAllOpts))
            using(ParquetRowGroupReader rg = r.OpenRowGroupReader(0)) {
                var dfName = (DataField)r.Schema.Fields[0];
                var dfAge = (DataField)r.Schema.Fields[1];
                var dfSalary = (DataField)r.Schema.Fields[2];
                var dfSsn = (DataField)r.Schema.Fields[3];

                DataColumn name = await rg.ReadColumnAsync(dfName);
                DataColumn age = await rg.ReadColumnAsync(dfAge);
                DataColumn sal = await rg.ReadColumnAsync(dfSalary);
                DataColumn ssn = await rg.ReadColumnAsync(dfSsn);

                Assert.Equal(new[] { "alice", "bob", "carol", "dave" }, name.Data.Cast<string>().ToArray());
                Assert.Equal(new[] { 31, 29, 41, 38 }, age.Data.Cast<int>().ToArray());
                Assert.Equal(new[] { 100_000.0, 75_500.0, 120_250.0, 88_000.0 }, sal.Data.Cast<double>().ToArray());
                Assert.Equal(new[] { "111-22-3333", "222-33-4444", "333-44-5555", "444-55-6666" }, ssn.Data.Cast<string>().ToArray());
            }

            // Negative: no resolver/keys → plain columns ok, encrypted columns fail on read
            var readPlainOnly = new ParquetOptions { FooterEncryptionKey = FooterKey };
            using(var ms = new MemoryStream(bytes, writable: false))
            using(ParquetReader r = await ParquetReader.CreateAsync(ms, readPlainOnly))
            using(ParquetRowGroupReader rg = r.OpenRowGroupReader(0)) {
                var dfName = (DataField)r.Schema.Fields[0];
                var dfAge = (DataField)r.Schema.Fields[1];
                var dfSalary = (DataField)r.Schema.Fields[2];

                // Plain columns succeed
                _ = await rg.ReadColumnAsync(dfName);
                _ = await rg.ReadColumnAsync(dfAge);

                // First encrypted column must throw (GCM tag mismatch / invalid data)
                await Assert.ThrowsAnyAsync<Exception>(async () => {
                    _ = await rg.ReadColumnAsync(dfSalary);
                });
            }
        }

        // --- 2) PF mode: stats stripped in plaintext meta; recovered from encrypted_column_metadata with key ---

        [Fact]
        public async Task PF_Strips_Plaintext_Stats_For_Encrypted_Columns_And_Recovers_From_EncryptedColumnMetadata() {
            ParquetSchema schema = MakeSchema();
            (DataColumn? cName, DataColumn? cAge, DataColumn? cSalary, DataColumn? cSsn) = MakeColumns(schema);

            byte[] bytes;
            var writeOpts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = FooterKey,    // used to sign PF
            };
            writeOpts.ColumnKeys["salary"] = new ParquetOptions.ColumnKeySpec(SalaryKey, Encoding.ASCII.GetBytes("kmeta:salary/v1"));
            writeOpts.ColumnKeys["ssn"] = new ParquetOptions.ColumnKeySpec(SsnKey);

            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, writeOpts)) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(cName);
                    await rg.WriteColumnAsync(cAge);
                    await rg.WriteColumnAsync(cSalary);
                    await rg.WriteColumnAsync(cSsn);
                }
                bytes = ms.ToArray();
            }

            // Open with *no* column keys; examine plaintext footer stats
            using(var ms = new MemoryStream(bytes, writable: false))
            using(ParquetReader r = await ParquetReader.CreateAsync(ms, new ParquetOptions { FooterSigningKey = FooterKey })) {
                FileMetaData meta = r.Metadata!;
                RowGroup rg0 = meta.RowGroups[0];

                ColumnChunk ccName = rg0.Columns[0];
                ColumnChunk ccAge = rg0.Columns[1];
                ColumnChunk ccSalary = rg0.Columns[2];
                ColumnChunk ccSsn = rg0.Columns[3];

                // Plain columns keep full stats in plaintext
                Assert.NotNull(ccName.MetaData!.Statistics);
                Assert.NotNull(ccAge.MetaData!.Statistics);

                // Encrypted columns have plaintext *stats stripped* (per writer)
                Assert.NotNull(ccSalary.MetaData!.Statistics);
                Assert.Null(ccSalary.MetaData!.Statistics.MinValue);
                Assert.Null(ccSalary.MetaData!.Statistics.MaxValue);

                Assert.NotNull(ccSsn.MetaData!.Statistics);
                Assert.Null(ccSsn.MetaData!.Statistics.MinValue);
                Assert.Null(ccSsn.MetaData!.Statistics.MaxValue);

                // But encrypted_column_metadata is present and can be decrypted with the column key
                Assert.True(ccSalary.EncryptedColumnMetadata?.Length > 0);
                Assert.True(ccSsn.EncryptedColumnMetadata?.Length > 0);

                // Decrypt ColumnMetaData for salary using the reader’s decrypter + our resolver
                EncryptionBase decr = meta.Decrypter!;
                // salary
                decr.FooterEncryptionKey = EncryptionBase.ParseKeyString(SalaryKey);
                using(MemoryStream msWrap = AsLengthPrefixedStream(ccSalary.EncryptedColumnMetadata!)) {
                    var rdr = new ThriftCompactProtocolReader(msWrap);
                    byte[] plain = decr.DecryptColumnMetaData(rdr, rg0.Ordinal!.Value, (short)2);
                    using var msCmd = new MemoryStream(plain, writable: false);
                    var cmd = ColumnMetaData.Read(new ThriftCompactProtocolReader(msCmd));
                    Assert.NotNull(cmd.Statistics?.MinValue);
                    Assert.NotNull(cmd.Statistics?.MaxValue);
                }

                // ssn
                decr.FooterEncryptionKey = EncryptionBase.ParseKeyString(SsnKey);
                using(MemoryStream msWrap = AsLengthPrefixedStream(ccSsn.EncryptedColumnMetadata!)) {
                    var rdr = new ThriftCompactProtocolReader(msWrap);
                    byte[] plain = decr.DecryptColumnMetaData(rdr, rg0.Ordinal!.Value, (short)3);
                    using var msCmd = new MemoryStream(plain, writable: false);
                    var cmd = ColumnMetaData.Read(new ThriftCompactProtocolReader(msCmd));
                    Assert.NotNull(cmd.Statistics?.MinValue);
                    Assert.NotNull(cmd.Statistics?.MaxValue);
                }

            }
        }

        // --- 3) Multi-key file: must have the right combination ---

        [Fact]
        public async Task MultiKey_File_Requires_All_Column_Keys() {
            ParquetSchema schema = MakeSchema();
            (DataColumn? cName, DataColumn? cAge, DataColumn? cSalary, DataColumn? cSsn) = MakeColumns(schema);

            byte[] bytes;
            var writeOpts = new ParquetOptions {
                FooterEncryptionKey = FooterKey,
            };
            writeOpts.ColumnKeys["salary"] = new ParquetOptions.ColumnKeySpec(SalaryKey);
            writeOpts.ColumnKeys["ssn"] = new ParquetOptions.ColumnKeySpec(SsnKey);

            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, writeOpts)) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(cName);
                    await rg.WriteColumnAsync(cAge);
                    await rg.WriteColumnAsync(cSalary);
                    await rg.WriteColumnAsync(cSsn);
                }
                bytes = ms.ToArray();
            }

            // Provide only 1 of the 2 column keys → one encrypted column reads, the other fails
            var readHalf = new ParquetOptions {
                FooterEncryptionKey = FooterKey,
                ColumnKeyResolver = (path, km) => string.Join(".", path) switch {
                    "salary" => SalaryKey,     // only salary provided
                    _ => null
                }
            };

            using(var ms = new MemoryStream(bytes, writable: false))
            using(ParquetReader r = await ParquetReader.CreateAsync(ms, readHalf))
            using(ParquetRowGroupReader rg = r.OpenRowGroupReader(0)) {
                var dfSalary = (DataField)r.Schema.Fields[2];
                var dfSsn = (DataField)r.Schema.Fields[3];

                // salary should succeed
                _ = await rg.ReadColumnAsync(dfSalary);

                // ssn must fail due to missing key
                await Assert.ThrowsAnyAsync<Exception>(async () => {
                    _ = await rg.ReadColumnAsync(dfSsn);
                });
            }

            // Provide both keys → success
            var readBoth = new ParquetOptions {
                FooterEncryptionKey = FooterKey,
                ColumnKeyResolver = (path, km) => string.Join(".", path) switch {
                    "salary" => SalaryKey,
                    "ssn" => SsnKey,
                    _ => null
                }
            };
            using(var ms = new MemoryStream(bytes, writable: false))
            using(ParquetReader r = await ParquetReader.CreateAsync(ms, readBoth))
            using(ParquetRowGroupReader rg = r.OpenRowGroupReader(0)) {
                var dfSalary = (DataField)r.Schema.Fields[2];
                var dfSsn = (DataField)r.Schema.Fields[3];
                _ = await rg.ReadColumnAsync(dfSalary);
                _ = await rg.ReadColumnAsync(dfSsn);
            }
        }
    }
}
