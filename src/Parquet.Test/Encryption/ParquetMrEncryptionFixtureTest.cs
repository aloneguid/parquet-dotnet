using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Parquet.Schema;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test.Encryption;

public class ParquetMrEncryptionFixtureTest : TestBase {
    private const string BaseKey = "testBaseKey";
    private static readonly byte[] AadPrefix = "mr-suite"u8.ToArray();
    private static readonly string[] AllColumns = ["name", "age", "salary", "ssn"];
    private static readonly string[] PlaintextColumns = ["name", "age"];

    public static IEnumerable<object[]> ReadableCases => BuildCases();

    public static IEnumerable<object[]> MissingAadCases =>
        BuildCases().Where(testCase => (AadMode)testCase[3] == AadMode.Supply);

    [Theory]
    [MemberData(nameof(ReadableCases), DisableDiscoveryEnumeration = false)]
    public async Task ReadsParquetMrFixture(
        string file,
        FooterMode footerMode,
        bool uniformEncryption,
        AadMode aadMode,
        int keyLength) {
        using Stream stream = OpenTestFile($"encryption/{file}");
        ParquetOptions options = CreateOptions(keyLength, aadMode);

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, options);
        Assert.Equal(1, reader.RowGroupCount);
        Assert.Equal(footerMode == FooterMode.Encrypted ? "PARE" : "PAR1", ReadMagic(file));

        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        IEnumerable<string> readableColumns = uniformEncryption ? AllColumns : PlaintextColumns;
        foreach(string name in readableColumns) {
            DataField field = reader.Schema.DataFields.Single(candidate => candidate.Name == name);
            await ReadAnyFieldAsync(rowGroup, field);
        }
    }

    [Theory]
    [MemberData(nameof(MissingAadCases), DisableDiscoveryEnumeration = false)]
    public async Task RejectsParquetMrFixtureWithoutRequiredAad(
        string file,
        FooterMode footerMode,
        bool uniformEncryption,
        AadMode aadMode,
        int keyLength) {
        Assert.Equal(AadMode.Supply, aadMode);
        using Stream stream = OpenTestFile($"encryption/{file}");
        var options = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions {
                FooterKey = DeriveKey("footer", keyLength)
            }
        };

        Exception? exception = await Record.ExceptionAsync(async () =>
            await ParquetReader.CreateAsync(stream, options));

        Assert.NotNull(exception);
        Assert.True(
            exception is InvalidDataException or CryptographicException,
            $"Unexpected exception for {footerMode}/{uniformEncryption}: {exception}");
    }

    private static IEnumerable<object[]> BuildCases() {
        foreach(EncryptionAlgorithm algorithm in Enum.GetValues<EncryptionAlgorithm>()) {
            foreach(FooterMode footerMode in Enum.GetValues<FooterMode>()) {
                foreach(AadMode aadMode in Enum.GetValues<AadMode>()) {
                    foreach(int keyLength in new int[] { 16, 32 }) {
                        yield return [
                            FileName(algorithm, footerMode, aadMode, uniform: true, keyMetadata: false, keyLength),
                            footerMode,
                            true,
                            aadMode,
                            keyLength
                        ];
                        yield return [
                            FileName(algorithm, footerMode, aadMode, uniform: false, keyMetadata: false, keyLength),
                            footerMode,
                            false,
                            aadMode,
                            keyLength
                        ];
                        yield return [
                            FileName(algorithm, footerMode, aadMode, uniform: false, keyMetadata: true, keyLength),
                            footerMode,
                            false,
                            aadMode,
                            keyLength
                        ];
                    }
                }
            }
        }
    }

    private static string FileName(
        EncryptionAlgorithm algorithm,
        FooterMode footerMode,
        AadMode aadMode,
        bool uniform,
        bool keyMetadata,
        int keyLength) {
        string algorithmName = algorithm == EncryptionAlgorithm.Gcm ? "gcm" : "gcm_ctr";
        string footerName = footerMode == FooterMode.Encrypted ? "EF" : "PF";
        string aadName = aadMode switch {
            AadMode.None => "none",
            AadMode.Stored => "stored",
            _ => "supply"
        };

        return uniform
            ? $"algo={algorithmName},mode={footerName},aad={aadName},uniform=Y_file-{keyLength}.parquet"
            : $"algo={algorithmName},mode={footerName},aad={aadName},partial=Y,keymeta={(keyMetadata ? "Y" : "N")}_file-{keyLength}.parquet";
    }

    private static ParquetOptions CreateOptions(int keyLength, AadMode aadMode) =>
        new() {
            Decryption = new ParquetDecryptionOptions {
                FooterKey = DeriveKey("footer", keyLength),
                AadPrefix = aadMode == AadMode.Supply ? AadPrefix : null
            }
        };

    private static byte[] DeriveKey(string label, int keyLength) {
        byte[] material = Encoding.UTF8.GetBytes($"{BaseKey}\0{label}");
        byte[] hash = SHA256.HashData(material);
        return hash.AsSpan(0, keyLength).ToArray();
    }

    private static string ReadMagic(string file) {
        using Stream stream = F.OpenRead(Path.Combine("data", "encryption", file));
        stream.Seek(-4, SeekOrigin.End);
        Span<byte> magic = stackalloc byte[4];
        stream.ReadExactly(magic);
        return Encoding.ASCII.GetString(magic);
    }

    public enum EncryptionAlgorithm {
        Gcm,
        GcmCtr
    }

    public enum FooterMode {
        Encrypted,
        Plaintext
    }

    public enum AadMode {
        None,
        Stored,
        Supply
    }
}
