using System;
using System.Collections;
using System.IO;
using System.Numerics;
using System.Reflection;
using System.Runtime.Serialization;
using Xunit;

namespace Parquet.Test
{
    public class ParquetTypesTest
    {
       [Fact]
       public void TestInt32()
       {
          var utils = new NumericUtils();
          byte[] parquetBlock = utils.IntToLittleEndian(999);

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<int>(parquetBlock, Thrift.Type.INT32);

          Assert.Equal(999, output);
       }

       [Fact]
       public void TestInt64()
       {
          var utils = new NumericUtils();
          byte[] parquetBlock = utils.LongToLittleEndian(9999);

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<long>(parquetBlock, Thrift.Type.INT64);

          Assert.Equal(9999, output);
       }

       [Fact]
       public void TestFloat()
       {
          var utils = new NumericUtils();
          byte[] parquetBlock = utils.FloatToLittleEndian(9999.999f);

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<float>(parquetBlock, Thrift.Type.FLOAT);

          Assert.Equal(9999.999f, output);
       }

       [Fact]
       public void TestDouble()
       {
          var utils = new NumericUtils();
          byte[] parquetBlock = utils.DoubleToLittleEndian(9999.9999999D);

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<double>(parquetBlock, Thrift.Type.DOUBLE);

          Assert.Equal(9999.9999999D, output);
       }

       [Fact]
       public void TestBoolean()
       {
          var boolArray = new bool[] {true, true, false, true};
          var bitArray = new BitArray(boolArray);

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<bool[]>(new byte[] {bitArray.GetByte()}, Thrift.Type.BOOLEAN, null, 4);

          Assert.Equal(boolArray, output);
      }

       [Fact]
       public void TestDate()
       {
          var utils = new NumericUtils();
          var dateTime = DateTime.Parse("2017-01-01");
          byte[] input = utils.IntToLittleEndian(Convert.ToInt32(dateTime.GetUnixUnixTimeDays()));

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<DateTime>(input, Thrift.Type.INT32, Thrift.ConvertedType.DATE);

          Assert.Equal(dateTime, output);
       }

       [Fact]
       public void TestTimestamp()
       {
          var utils = new NumericUtils();
          var dateTime = new DateTime(2017, 1, 1, 0, 0, 0, 0);
          byte[] input = utils.LongToLittleEndian(dateTime.ToUnixTime());

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<DateTime>(input, Thrift.Type.INT64, Thrift.ConvertedType.TIMESTAMP_MILLIS);

          Assert.Equal(dateTime, output);
       }

       [Fact]
       public void TestInt96()
       {
          byte[] parquetBlock = new byte[] { 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
         
          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<BigInteger>(parquetBlock, Thrift.Type.INT96);

          var bi = new BigInteger(65535D);

          Assert.Equal(bi, output);
       }

       [Fact]
       public void TestByteArray()
       {
          byte[] parquetBlock = new byte[] { 1, 0, 0, 0, 67, 68, 69, 70 };

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<byte[]>(parquetBlock, Thrift.Type.BYTE_ARRAY);

          Assert.Equal(new byte[] { 67 }, output);
       }

       [Fact]
       public void TestFixedLenByteArray()
       {
          byte[] parquetBlock = new byte[] { 67, 68, 69, 70 };

          var encoding = new ParquetEncoding();
          var output = encoding.ReadPlain<byte[]>(parquetBlock, Thrift.Type.FIXED_LEN_BYTE_ARRAY);

          Assert.Equal(new byte[] { 67, 68, 69, 70 }, output);
       }

      private string GetDataFilePath(string name)
        {
            string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
            return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
        }
    }
}
