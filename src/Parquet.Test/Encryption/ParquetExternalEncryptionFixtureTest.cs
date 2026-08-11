using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption;

public class ParquetExternalEncryptionFixtureTest : TestBase {
    private static readonly byte[] CorrectKey = Encoding.UTF8.GetBytes("QFwuIKG8yb845rEufVJAgcOo");
    private static readonly byte[] WrongKey = Encoding.UTF8.GetBytes("AAAAAAAAAAAAAAAAAAAAAAAA");

    [Fact]
    public async Task ReadsExternalAes192Fixture() {
        using Stream stream = OpenTestFile("encrypted_utf8_aes_gcm_v1_192bit.parquet");
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = CorrectKey }
        };

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        Assert.NotEmpty(reader.Schema.DataFields);
        Assert.True(reader.RowGroupCount > 0);

        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        foreach(DataField field in reader.Schema.DataFields)
            await ReadAnyFieldAsync(rowGroup, field);
    }

    [Fact]
    public async Task RejectsWrongKeyForExternalAes192Fixture() {
        using Stream stream = OpenTestFile("encrypted_utf8_aes_gcm_v1_192bit.parquet");
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = WrongKey }
        };

        await Assert.ThrowsAsync<AuthenticationTagMismatchException>(async () =>
            await ParquetReader.CreateAsync(stream, options));
    }
}
