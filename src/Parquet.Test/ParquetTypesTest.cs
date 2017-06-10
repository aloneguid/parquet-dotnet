using System;
using System.IO;
using System.Reflection;
using Xunit;

namespace Parquet.Test
{
    public class ParquetTypesTest
    {
        [Fact]
        public void TestInt32()
        {
           byte[] parquetBlock =  BitConverter.GetBytes(1);

           var encoding = new ParquetEncoding();
           var output = encoding.ReadPlain<int>(parquetBlock, ParquetTypes.Type.Int32, 1);

            Assert.Equal(1, output);
        }

       [Fact]
       public void TestInt64()
       {
          byte[] parquetBlock = BitConverter.GetBytes((Int64) 1);

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<Int64>(parquetBlock, ParquetTypes.Type.Int64, 1);

          Assert.Equal(1, output);
       }

      private string GetDataFilePath(string name)
        {
            string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
            return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
        }
    }
}
