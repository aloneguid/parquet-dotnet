using System.IO;
using System.Threading.Tasks;
using Parquet.Utils;
using Xunit;

namespace Parquet.Test {
    public class UtilsTest {
        [Fact]
        public async Task MergeFiles() {
            var merger = new FileMerger(new DirectoryInfo(@"C:\dev\mod\dq_data\pf_columns"));

            using System.IO.FileStream dest = System.IO.File.Create(@"C:\dev\mod\dq_data\pf_columns\merged_f.parquet");
            await merger.MergeFilesAsync(dest);
        }

        [Fact]
        public async Task MergeRowGroup() {
            var merger = new FileMerger(new DirectoryInfo(@"C:\dev\mod\dq_data\pf_columns"));

            using System.IO.FileStream dest = System.IO.File.Create(@"C:\dev\mod\dq_data\pf_columns\merged_rg.parquet");
            await merger.MergeRowGroups(dest);
        }
    }
}
