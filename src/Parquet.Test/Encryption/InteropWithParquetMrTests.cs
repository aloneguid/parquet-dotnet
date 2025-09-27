// src/Parquet.Test/Encryption/InteropWithParquetMrTests.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class InteropWithParquetMrTests : TestBase {

        private async static Task<(long RowCount, string[] Ids, int?[] Ages, bool[] Flags)>
          ReadBasicContent(Stream s, string encryptionKey, string? aadPrefix) {

            using ParquetReader reader = await ParquetReader.CreateAsync(s, new ParquetOptions {
                SecretKey = encryptionKey,
                AADPrefix = aadPrefix
            });

            Assert.NotNull(reader.Schema);

            DataField find(string n) {
                DataField? f = reader.Schema.DataFields
                    .FirstOrDefault(df => string.Equals(df.Name, n, StringComparison.OrdinalIgnoreCase));
                if(f is null) {
                    string got = string.Join(", ", reader.Schema.DataFields.Select(df => df.Name));
                    throw new InvalidDataException($"Expected field '{n}' not found. Schema has: {got}");
                }
                return (DataField)f;
            }

            DataField idF = find("id");
            DataField ageF = find("age");
            DataField flagF = find("flag");

            var ids = new List<string>();
            var ages = new List<int?>();
            var flags = new List<bool>();

            for(int rg = 0; rg < reader.RowGroupCount; rg++) {
                using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(rg);

                DataColumn idCol = await rgr.ReadColumnAsync(idF);
                DataColumn ageCol = await rgr.ReadColumnAsync(ageF);
                DataColumn flagCol = await rgr.ReadColumnAsync(flagF);

                ids.AddRange(idCol.Data.Cast<string>());

                // age is optional -> may contain nulls
                ages.AddRange(ToNullableIntArray(ageCol.Data));

                flags.AddRange(flagCol.Data.Cast<bool>());
            }

            return (ids.LongCount(), ids.ToArray(), ages.ToArray(), flags.ToArray());

            static int?[] ToNullableIntArray(Array src) {
                int n = src.Length;
                int?[] a = new int?[n];
                for(int i = 0; i < n; i++) {
                    object? v = src.GetValue(i);
                    a[i] = v is null ? (int?)null : Convert.ToInt32(v);
                }
                return a;
            }
        }

        [Fact]
        public async Task A_FooterOnly_NoPrefix_Succeeds() {
            using Stream s = OpenTestFile("encryption/enc_footer_only.parquet");
            var opts = new ParquetOptions {
                SecretKey = "footerKey-16byte",
                AADPrefix = null
            };
            using ParquetReader r = await ParquetReader.CreateAsync(s, opts);
            using ParquetRowGroupReader rg = r.OpenRowGroupReader(0);

            DataField df = r.Schema.DataFields.First();
            DataColumn col = await rg.ReadColumnAsync(df);
            Assert.NotEmpty(col.Data); // sanity
        }

        [Fact]
        public async Task B1_Missing_AadPrefix_throws_InvalidDataException() {
            using Stream s = OpenTestFile("encryption/enc_footer_with_aadprefix.parquet");
            var opts = new ParquetOptions {
                SecretKey = "footerKey-16byte",
                AADPrefix = null // missing
            };

            await Assert.ThrowsAsync<InvalidDataException>(async () => {
                using ParquetReader r = await ParquetReader.CreateAsync(s, opts);
            });
        }

        [Fact]
        public async Task B2_Wrong_AadPrefix_throws_CryptographicException() {
            using Stream s = OpenTestFile("encryption/enc_footer_with_aadprefix.parquet");
            var opts = new ParquetOptions {
                SecretKey = "footerKey-16byte",
                AADPrefix = "wr-fixtures-suiteX" // wrong bytes
            };

            await Assert.ThrowsAnyAsync<CryptographicException>(async () => {
                using ParquetReader r = await ParquetReader.CreateAsync(s, opts);
            });
        }


        [Fact]
        public async Task B_WithAadPrefix_CorrectPrefix_Succeeds() {
            using Stream s = OpenTestFile("encryption/enc_footer_with_aadprefix.parquet");
            var good = new ParquetOptions {
                SecretKey = "footerKey-16byte",
                AADPrefix = "wr-fixtures-suite"
            };
            using ParquetReader r = await ParquetReader.CreateAsync(s, good);
            using ParquetRowGroupReader rg = r.OpenRowGroupReader(0);
            DataField df = r.Schema.DataFields.First();
            DataColumn col = await rg.ReadColumnAsync(df);
            Assert.NotEmpty(col.Data);
        }

        [Fact]
        public async Task C_ColumnKey_NotSupported_Throws() {
            using Stream s = OpenTestFile("encryption/enc_footer_and_idcol.parquet");
            var opts = new ParquetOptions {
                SecretKey = "footerKey-16byte",
                AADPrefix = "wr-fixtures-suite"
            };
            await Assert.ThrowsAsync<NotSupportedException>(async () => {
                using ParquetReader r = await ParquetReader.CreateAsync(s, opts);
                using ParquetRowGroupReader rg = r.OpenRowGroupReader(0);
                DataField df = r.Schema.DataFields.First();
                _ = await rg.ReadColumnAsync(df);
            });
        }

        [Fact]
        public async Task FooterOnly_NoAAD_Content_Verified() {
            using Stream s = OpenTestFile("encryption/enc_footer_only.parquet");
            (long count, string[]? ids, int?[]? ages, bool[]? flags) = await ReadBasicContent(s, "footerKey-16byte", null);

            Assert.True(count >= 50_000, $"expected at least 50k rows, got {count}");
            Assert.Equal(ids.Length, ages.Length);
            Assert.Equal(ids.Length, flags.Length);

            // Validate exact DataRows pattern
            for(int i = 0; i < ids.Length; i++) {
                Assert.Equal($"user-{i}", ids[i]);
                if(i % 3 != 0) {
                    Assert.True(ages[i].HasValue, $"age should be present at i={i}");
                    Assert.Equal(i % 100, ages[i]!.Value);
                } else {
                    Assert.Null(ages[i]);
                }
                Assert.Equal(i % 2 == 0, flags[i]);
            }
        }

        [Fact]
        public async Task FooterOnly_WithAAD_Content_Verified() {
            using Stream s = OpenTestFile("encryption/enc_footer_with_aadprefix.parquet");
            (long count, string[]? ids, int?[]? ages, bool[]? flags) = await ReadBasicContent(s, "footerKey-16byte", "wr-fixtures-suite");

            Assert.True(count >= 50_000);
            Assert.Equal(ids.Length, ages.Length);
            Assert.Equal(ids.Length, flags.Length);

            for(int i = 0; i < ids.Length; i++) {
                Assert.Equal($"user-{i}", ids[i]);
                if(i % 3 != 0) {
                    Assert.True(ages[i].HasValue);
                    Assert.Equal(i % 100, ages[i]!.Value);
                } else {
                    Assert.Null(ages[i]);
                }
                Assert.Equal(i % 2 == 0, flags[i]);
            }
        }
    }
}
