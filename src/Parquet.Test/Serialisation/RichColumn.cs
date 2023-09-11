using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class RichColumnTest : TestBase {
        [Fact]
        public async Task Smoke() {
            using Stream s = OpenTestFile("types/alltypes.gzip.parquet");
            using ParquetReader reader = await ParquetReader.CreateAsync(s);
            using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0);
        }
    }
}
