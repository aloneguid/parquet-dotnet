using System;
using System.IO;
using System.Reflection;
using Xunit;

namespace Parquet.Test
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            using (Stream s = File.OpenRead(GetDataFilePath("alltypes_plain.parquet")))
            {
                using (var r = new ParquetReader(s))
                {

                }
            }

        }

        private string GetDataFilePath(string name)
        {
            string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
            return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
        }
    }
}
