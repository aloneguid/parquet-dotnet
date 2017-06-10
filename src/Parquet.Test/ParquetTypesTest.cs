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

       [Fact]
       public void TestInt96()
       {
         // have to fit 12 bytes into this 
         /*
          * assert b'\x00\x00\x00\x00\x00\x00\x00\x00\xe7\x03\x00\x00' == fastparquet.encoding.read_plain(

             struct.pack(b"<qi", 0, 999),

             parquet_thrift.Type.INT96, 1)
             */
          byte[] parquetBlock = new byte[] {1 };
         
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
