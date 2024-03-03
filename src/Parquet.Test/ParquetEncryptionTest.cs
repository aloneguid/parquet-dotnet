using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    [CollectionDefinition(nameof(ParquetEncryptionTestCollection), DisableParallelization = true)]
    public class ParquetEncryptionTestCollection {

    }

    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class ParquetEncryptionTest : TestBase {
        /// <remarks>
        /// If this test doesn't run on its own and last it breaks things. 
        /// Running this test last in XUnit is only temporary a workaround.
        /// </remarks>
        [Fact]
        public async Task Z_DecryptFile_UTF8_AesGcmV1_192bit() {
            using Stream stream = OpenTestFile("encrypted_utf8_aes_gcm_v1_192bit.parquet");

            var parquetOptions = new ParquetOptions {
                EncryptionKey = "QFwuIKG8yb845rEufVJAgcOo",
                AADPrefix = null //this file doesn't use an aad prefix
            };

            using ParquetReader reader = await ParquetReader.CreateAsync(stream, parquetOptions);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
            foreach(DataField df in reader.Schema.DataFields) {
                DataColumn dc = await rgr.ReadColumnAsync(df);
                //TODO: Moar testing
            }
        }
    }
}