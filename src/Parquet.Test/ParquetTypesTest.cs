using System;
using System.Collections;
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
          byte[] parquetBlock = System.Text.Encoding.UTF8.GetBytes("999");

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<int>(parquetBlock, ParquetTypes.Type.Int32, 3);

          Assert.Equal(999, output);
       }

       [Fact]
       public void TestInt64()
       {
          byte[] parquetBlock = System.Text.Encoding.UTF8.GetBytes("9999");

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<Int64>(parquetBlock, ParquetTypes.Type.Int64, 4);

          Assert.Equal(9999, output);
       }

       [Fact]
       public void TestFloat()
       {
          byte[] parquetBlock = System.Text.Encoding.UTF8.GetBytes("9999.999");

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<float>(parquetBlock, ParquetTypes.Type.Float, 8);

          Assert.Equal(9999.999f, output);
       }

       [Fact]
       public void TestDouble()
       {
          byte[] parquetBlock = System.Text.Encoding.UTF8.GetBytes("9999.9999999");

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<double>(parquetBlock, ParquetTypes.Type.Double, 12);

          Assert.Equal(9999.9999999D, output);
       }

       [Fact]
       public void TestBoolean()
       {
          var boolArray = new bool[] {true, true, false, true};
          var bitArray = new BitArray(boolArray);

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<bool[]>(new byte[] {bitArray.GetByte()}, ParquetTypes.Type.Boolean, 4);

          Assert.Equal(boolArray, output);
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
