using System;
using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [CollectionDefinition(nameof(ParquetEncryptionTestCollection), DisableParallelization = true)]
    public class ParquetEncryptionTestCollection { }

    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class ParquetEncryptionTests : TestBase {
        private const string EncryptedFile = "encrypted_utf8_aes_gcm_v1_192bit.parquet";
        private const string CorrectKey = "QFwuIKG8yb845rEufVJAgcOo"; // from your test
        private const string WrongKey = "AAAAAAAAAAAAAAAAAAAAAAAA"; // same length, wrong bytes

        /// <summary>
        /// Happy path: decrypt the sample AES-GCM v1 (192-bit key) file and confirm we can read
        /// row group 0, all data fields, with non-empty values. This uses AADPrefix = null
        /// because the file does not require a supplied prefix.
        /// </summary>
        [Fact]
        public async Task Z_Decrypts_AESGCMv1_192bit_File_And_Reads_All_Columns() {
            using Stream stream = OpenTestFile(EncryptedFile);

            var parquetOptions = new ParquetOptions {
                EncryptionKey = CorrectKey,
                AADPrefix = null
            };

            using ParquetReader reader = await ParquetReader.CreateAsync(stream, parquetOptions);
            Assert.True(reader.RowGroupCount > 0);
            Assert.NotNull(reader.Schema);
            Assert.True(reader.Schema.DataFields.Length > 0);

            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);

            foreach(DataField df in reader.Schema.DataFields) {
                DataColumn dc = await rgr.ReadColumnAsync(df);

                // basic sanity checks that strongly imply successful decryption:
                Assert.Equal(df, dc.Field);
                Assert.True(dc.Data.Length > 0);

                // additional helpful checks for common text columns:
                if(df.ClrType == typeof(string)) {
                    string[] strings = (string[])dc.Data;
                    Assert.True(strings.Length > 0);
                    // ensure at least one non-null/non-empty string
                    Assert.Contains(strings, s => !string.IsNullOrEmpty(s));
                }
            }
        }

        /// <summary>
        /// Negative path: the same file with an incorrect key should throw when we attempt
        /// to read (either during footer decryption or the first column/page).
        /// We assert that some crypto/IO exception is raised.
        /// </summary>
        [Fact]
        public async Task Zz_Fails_With_Wrong_Key_On_AESGCMv1_File() {
            using Stream stream = OpenTestFile(EncryptedFile);

            var parquetOptions = new ParquetOptions {
                EncryptionKey = WrongKey,
                AADPrefix = null
            };

            // Creating the reader may already fail; if it doesn’t, reading the first column should.
            await Assert.ThrowsAnyAsync<Exception>(async () => {
                using ParquetReader reader = await ParquetReader.CreateAsync(stream, parquetOptions);
                using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
                DataField df = reader.Schema.DataFields[0];
                _ = await rgr.ReadColumnAsync(df);
            });
        }
    }
}
