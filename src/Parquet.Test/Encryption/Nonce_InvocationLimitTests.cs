// src/Parquet.Test/Encryption/Nonce_InvocationLimitTests.cs
using System;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Encryption;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class Nonce_InvocationLimitTests {

        /*
         * This test is a spec guard. If/when you expose a per-key invocation counter,
         * replace the reflection/TODO below with a real toggle and set a tiny limit.
         */

        [Fact(Skip = "Enable once per-key invocation guard is available in encryption layer")]
        public void Exceeding_PerKey_Invocation_Limit_Throws() {
            // Arrange
            byte[] key = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
            var enc = new AES_GCM_V1_Encryption {
                FooterEncryptionKey = key,
                AadPrefix = Array.Empty<byte>(),
                AadFileUnique = new byte[] { 0xCA, 0xFE }
            };

            // TODO: set a small limit (e.g., 8) via a test-only hook or env var.
            // EncryptionBase.SetPerKeyInvocationLimitForTests(8);

            // Act
            for(int i = 0; i < 8; i++) {
                _ = enc.EncryptDataPage(new byte[1], 0, 0, (short)i);
            }

            // Assert: the next invocation should throw
            Assert.ThrowsAny<Exception>(() => enc.EncryptDataPage(new byte[1], 0, 0, 9));
        }
    }
}
