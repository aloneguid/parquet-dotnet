// src/Parquet.Test/Encryption/InteropWithParquetMr_AllCasesTests.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Interop.Inspector;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Test.Integration;
using Xunit;
using Xunit.Sdk;

namespace Parquet.Test.Encryption {
    /// <summary>
    /// Interop tests against parquet-mr generated files (full matrix).
    ///
    /// IMPORTANT: Update the constants below to match how you generated your fixtures.
    /// </summary>
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class InteropWithParquetMr_AllCasesTests : TestBase {
        private readonly EncParquetInspectorClient _client;
        private const string BaseKeyUtf8 = "testBaseKey";
        private const string AadPrefixUtf8 = "mr-suite";

        public InteropWithParquetMr_AllCasesTests() {
            _client = new EncParquetInspectorClient();
        }

        public static readonly object[][] AllCasesArray = BuildAllCases().ToArray();
        public static IEnumerable<object[]> AllCases => AllCasesArray;

        public static readonly object[][] EF_Supply_MissingPrefix_Cases_Array =
            Filter(aad => aad == AAD.Supply, mode => mode == Mode.EF)
            .Select(row => new object[] { (string)row[0], (Mode)row[2], (int)row[6] })
            .ToArray();
        public static IEnumerable<object[]> EF_Supply_MissingPrefix_Cases => EF_Supply_MissingPrefix_Cases_Array;

        public static readonly object[][] PF_Supply_MissingPrefix_Cases_Array =
            Filter(aad => aad == AAD.Supply, mode => mode == Mode.PF)
            .Select(row => new object[] { (string)row[0], (Mode)row[2], (int)row[6] })
            .ToArray();
        public static IEnumerable<object[]> PF_Supply_MissingPrefix_Cases => PF_Supply_MissingPrefix_Cases_Array;

        public static readonly object[][] UniformSuccessCasesSlim_Array =
            Filter(aad => true, mode => true, uniformOnly: true)
            .Select(row => new object[] { (string)row[0], (Mode)row[2], (AAD)row[3], (int)row[6] })
            .ToArray();
        public static IEnumerable<object[]> UniformSuccessCasesSlim => UniformSuccessCasesSlim_Array;

        public static readonly object[][] PartialPlainOnlyCases_Array =
            Filter(aad => true, mode => true, uniformOnly: false)
            .Select(row => new object[] { (string)row[0], (Mode)row[2], (AAD)row[3], (int)row[6] })
            .ToArray();
        public static IEnumerable<object[]> PartialPlainOnlyCases => PartialPlainOnlyCases_Array;



        // ---------- Enums mirroring your generator ----------
        public enum Algo { Gcm, GcmCtr }
        public enum Mode { EF, PF }            // EF = encrypted footer, PF = plaintext footer (signed)
        public enum AAD { None, Stored, Supply }

        private static IEnumerable<object[]> BuildAllCases() {
            Algo[] algos = [Algo.Gcm, Algo.GcmCtr];
            Mode[] modes = [Mode.EF, Mode.PF];
            AAD[] aads = [AAD.None, AAD.Stored, AAD.Supply];
            int[] keySizes = [16, 32];

            foreach(Algo algo in algos)
                foreach(Mode mode in modes)
                    foreach(AAD aad in aads)
                        foreach(int keySize in keySizes) {
                            // Uniform
                            yield return new object[] {
                    FileName(algo, mode, aad, uniform:true,   keyMeta:false, keySize),
                    algo, mode, aad, true,  false, keySize
                };
                            // Partial with keymeta=N
                            yield return new object[] {
                    FileName(algo, mode, aad, uniform:false,  keyMeta:false, keySize),
                    algo, mode, aad, false, false, keySize
                };
                            // Partial with keymeta=Y
                            yield return new object[] {
                    FileName(algo, mode, aad, uniform:false,  keyMeta:true, keySize),
                    algo, mode, aad, false, true, keySize
                };
                        }
        }

        private static IEnumerable<object[]> Filter(
            Func<AAD, bool> aadPred,
            Func<Mode, bool> modePred,
            bool? uniformOnly = null) {
            foreach(object[] row in AllCases) {
                var aad = (AAD)row[3];
                var mode = (Mode)row[2];
                bool isUniform = (bool)row[4];

                if(!aadPred(aad) || !modePred(mode))
                    continue;
                if(uniformOnly.HasValue && uniformOnly.Value != isUniform)
                    continue;

                yield return row;
            }
        }

        private static string FileName(Algo algo, Mode mode, AAD aad, bool uniform, bool keyMeta, int keySize) {
            string a = algo == Algo.Gcm ? "gcm" : "gcm_ctr";
            string m = mode.ToString(); // EF/PF
            string ad = aad switch { AAD.None => "none", AAD.Stored => "stored", _ => "supply" };
            if(uniform) {
                return $"algo={a},mode={m},aad={ad},uniform=Y_file-{keySize}.parquet";
            } else {
                string km = keyMeta ? "Y" : "N";
                return $"algo={a},mode={m},aad={ad},partial=Y,keymeta={km}_file-{keySize}.parquet";
            }
        }

        // ------------ Key derivation identical to parquet-mr helper ------------
        private static byte[] DeriveKey(string baseKeyUtf8, string label, int sizeBytes) {
            using var sha = SHA256.Create();
            sha.TransformBlock(Encoding.UTF8.GetBytes(baseKeyUtf8), 0, baseKeyUtf8.Length, null, 0);
            sha.TransformBlock([0x00], 0, 1, null, 0);
            byte[] labelBytes = Encoding.UTF8.GetBytes(label);
            sha.TransformFinalBlock(labelBytes, 0, labelBytes.Length);
            byte[] full = sha.Hash!;
            if(sizeBytes == 16)
                return full.Take(16).ToArray();
            if(sizeBytes == 24)
                return full.Take(24).ToArray();
            if(sizeBytes == 32)
                return full.Take(32).ToArray();
            throw new ArgumentOutOfRangeException(nameof(sizeBytes));
        }

        private static string ToHex(byte[] key) => BitConverter.ToString(key).Replace("-", "");

        // Columns in those fixtures:
        //   name (string, required)
        //   age (int32, required)
        //   salary (double, optional)  -- encrypted in partial cases (column-key)
        //   ssn (string, optional)     -- encrypted in partial cases (column-key)
        private static readonly string[] PlainColumns = ["name", "age"];
        private static readonly string[] AllColumns = ["name", "age", "salary", "ssn"];

        // -------------------- Assertions --------------------

        [Fact]
        public void CheckTestFiles() {
            Assert.True(EF_Supply_MissingPrefix_Cases.Count() > 0, "no EF/Supply/MissingPrefix cases");
            Assert.True(PF_Supply_MissingPrefix_Cases.Count() > 0, "no PF/Supply/MissingPrefix cases");
            Assert.True(UniformSuccessCasesSlim.Count() > 0, "no UniformSuccessCasesSlim cases");
            Assert.True(PartialPlainOnlyCases.Count() > 0, "no PartialPlainOnlyCases cases");
        }

        [Theory(DisplayName = "EF Missing AAD → throws")]
        [MemberData(nameof(EF_Supply_MissingPrefix_Cases), DisableDiscoveryEnumeration = false)]
        public async Task EF_With_SupplyAAD_MissingPrefix_Throws(string file, Mode mode, int keySize) {
            AssumeEf(mode);
            using Stream s = OpenTestFile($"encryption/{file}");
            var opts = new ParquetOptions {
                FooterEncryptionKey = ToHex(DeriveKey(BaseKeyUtf8, "footer", keySize)),
                AADPrefix = null
            };
            await AssertMissingAadThrowsAsync(async () => {
                using ParquetReader r = await ParquetReader.CreateAsync(s, opts);
                _ = r.RowGroupCount;
            });
        }

        // Accept either early InvalidDataException (prefix required) or
        // Crypto failures (GCM tag mismatch) depending on file metadata.
        private static async Task AssertMissingAadThrowsAsync(Func<Task> action) {
            try {
                await action();
                Assert.Fail("Expected an exception due to missing AAD prefix.");
            } catch(Exception ex) {
                bool ok =
                    ex is InvalidDataException ||
                    ex is AuthenticationTagMismatchException ||
                    ex is CryptographicException;
                Assert.True(ok, ex.Message);
            }
        }

        [Theory(DisplayName = "PF Missing AAD → throws")]
        [MemberData(nameof(PF_Supply_MissingPrefix_Cases), DisableDiscoveryEnumeration = false)]
        public async Task PF_With_SupplyAAD_MissingPrefix_Throws(string file, Mode mode, int keySize) {
            AssumePf(mode);
            using Stream s = OpenTestFile($"encryption/{file}");
            var opts = new ParquetOptions {
                UsePlaintextFooter = true,
                FooterSigningKey = ToHex(DeriveKey(BaseKeyUtf8, "footer", keySize)),
                // no AADPrefix on purpose
            };
            await Assert.ThrowsAsync<InvalidDataException>(async () => {
                using ParquetReader r = await ParquetReader.CreateAsync(s, opts);
                _ = r.RowGroupCount;
            });
        }

        [Theory(DisplayName = "Can read all columns")]
        [MemberData(nameof(UniformSuccessCasesSlim), DisableDiscoveryEnumeration = false)]
        public async Task Uniform_CanRead_AllColumns(string file, Mode mode, AAD aad, int keySize) {
            using Stream s = OpenTestFile($"encryption/{file}");

            string footerHex = ToHex(DeriveKey(BaseKeyUtf8, "footer", keySize));
            var opts = new ParquetOptions();

            if(mode == Mode.EF) {
                opts.FooterEncryptionKey = footerHex;
            } else {
                opts.UsePlaintextFooter = true;
                opts.FooterSigningKey = footerHex;   // verify PF signature
                opts.FooterEncryptionKey = footerHex;   // decrypt columns in uniform mode
            }

            opts.AADPrefix = aad switch {
                AAD.None => null,
                AAD.Stored => null,               // stored in file
                AAD.Supply => AadPrefixUtf8,      // must supply
                _ => null
            };

            using ParquetReader r = await ParquetReader.CreateAsync(s, opts);
            Assert.NotNull(r.Schema);

            using ParquetRowGroupReader rg = r.OpenRowGroupReader(0);
            foreach(string name in AllColumns) {
                var df = (DataField)r.Schema.DataFields.First(f => f.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                DataColumn col = await rg.ReadColumnAsync(df);
                Assert.NotNull(col.Data);
                Assert.True(col.Data.Length > 0);
            }
        }

        [Theory(DisplayName = "Can read plain columns only")]
        [MemberData(nameof(PartialPlainOnlyCases), DisableDiscoveryEnumeration = false)]
        public async Task Partial_CanRead_PlainColumns_Without_ColumnKeys(string file, Mode mode, AAD aad, int keySize) {
            using Stream s = OpenTestFile($"encryption/{file}");

            string footerHex = ToHex(DeriveKey(BaseKeyUtf8, "footer", keySize));
            var opts = new ParquetOptions();

            if(mode == Mode.EF) {
                opts.FooterEncryptionKey = footerHex;
            } else {
                // opts.FooterEncryptionKey = footerHex;
                opts.UsePlaintextFooter = true;
                opts.FooterSigningKey = footerHex;
                // no column keys on purpose; we will only read plaintext columns
            }

            opts.AADPrefix = aad switch {
                AAD.None => null,
                AAD.Stored => null,
                AAD.Supply => AadPrefixUtf8,
                _ => null
            };

            using ParquetReader r = await ParquetReader.CreateAsync(s, opts);
            Assert.NotNull(r.Schema);

            using ParquetRowGroupReader rg = r.OpenRowGroupReader(0);
            foreach(string name in PlainColumns) {
                var df = (DataField)r.Schema.DataFields.First(f => f.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                DataColumn col = await rg.ReadColumnAsync(df);
                Assert.NotNull(col.Data);
                Assert.True(col.Data.Length > 0);
            }
        }

        // -------------------- Helpers --------------------
        private static void AssumeEf(Mode m) { if(m != Mode.EF) throw new XunitException("Test data assumption failed: expected EF"); }
        private static void AssumePf(Mode m) { if(m != Mode.PF) throw new XunitException("Test data assumption failed: expected PF"); }
    }
}
