using Xunit;
using Parquet.Meta.Proto;
using System.IO;
using Parquet.Meta;

namespace Parquet.Test {
    public class ThriftTest : TestBase {
        [Fact]
        public void TestFileRead_Table() {
            using Stream fs = OpenTestFile("thrift/wide.bin");
            FileMetaData fileMeta = FileMetaData.Read(new ThriftCompactProtocolReader(fs));
        }
    }
}