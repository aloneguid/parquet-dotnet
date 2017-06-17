using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using Parquet.Thrift;

namespace Parquet
{
    class ParquetEncoding
    {
       public T ReadPlain<T>(byte[] parquetBlock, Thrift.Type type, Thrift.ConvertedType? convertedType = null, int count = 0, int width = 0)
       {
         // This is a poor implementation we have the width so we should walk the array - boxing/unboxing and casting ugly, slow and bloaty!!
          switch (type)
          {
             case Thrift.Type.INT32:
                if (!convertedType.HasValue || convertedType.Value != ConvertedType.DATE)
                   return (T) (object) BitConverter.ToInt32(parquetBlock, 0);
                int epochInt = BitConverter.ToInt32(parquetBlock, 0);
                return (T) (object) epochInt.FromUnixTime();
             case Thrift.Type.INT64:
                if (!convertedType.HasValue || convertedType.Value != ConvertedType.TIMESTAMP_MILLIS)
                   return (T) (object) BitConverter.ToInt64(parquetBlock, 0);
                long epoch = BitConverter.ToInt64(parquetBlock, 0);
                return (T) (object) epoch.FromUnixTime();
             case Thrift.Type.INT96:
                return (T) (object) new BigInteger(parquetBlock);
             case Thrift.Type.FLOAT:
                return (T) (object) BitConverter.ToSingle(parquetBlock, 0);
             case Thrift.Type.DOUBLE:
                return (T) (object) BitConverter.ToDouble(parquetBlock, 0);
             case Thrift.Type.BOOLEAN:
                return (T) (object) new BitArray(parquetBlock).ConvertToBoolArray(count);
             case Thrift.Type.BYTE_ARRAY:
                int elementCount = ParquetUtils.GetByteArrayLELength(parquetBlock.Take(4).ToArray());
                return (T) (object) parquetBlock.Skip(4).Take(elementCount).ToArray();

             default:
                return default(T);
          }
       }

      
   }
}
