using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Serialization;
using Parquet.Test.Xunit;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test.Integration {
    public class ParquetSerializerTest : IntegrationBase {
        class IdWithTags {
            public int Id { get; set; }

            public Dictionary<string, string>? Tags { get; set; }
        }

        private async Task<string> WriteToTempFile<T>(IEnumerable<T> data) {
            string testFileName = Path.GetFullPath($"{nameof(ParquetSerializerTest)}.parquet");

            if(F.Exists(testFileName))
                F.Delete(testFileName);

            await ParquetSerializer.SerializeAsync(data, testFileName);
            return testFileName;
        }


        [SkipOnMac]
        public async Task SimpleMapReadsWithParquetMr() {
            var data = Enumerable.Range(0, 10).Select(i => new IdWithTags {
                Id = i,
                Tags = new Dictionary<string, string> {
                    ["id"] = i.ToString(),
                    ["gen"] = DateTime.UtcNow.ToString()
                }
            }).ToList();

            string fileName = await WriteToTempFile(data);

            // read with Java
            string? javaCat = ExecMrCat(fileName);
            Assert.NotNull(javaCat);
            Assert.Contains("id", javaCat);
            Assert.Contains("gen", javaCat);
        }

        [SkipOnMac]
        public async Task SimpleMapReadsWithPyArrow() {
            var data = Enumerable.Range(0, 10).Select(i => new IdWithTags {
                Id = i,
                Tags = new Dictionary<string, string> {
                    ["id"] = i.ToString(),
                    ["gen"] = DateTime.UtcNow.ToString()
                }
            }).ToList();

            string fileName = await WriteToTempFile(data);

            //F.Copy(fileName, "c:\\tmp\\pyarrow.parquet", true);

            // read with Java
            string? arrowCat = ExecPyArrowToJson(fileName);
            Assert.NotNull(arrowCat);
            Assert.Contains("id", arrowCat);
            Assert.Contains("gen", arrowCat);
        }
    }
}
