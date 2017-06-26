/* MIT License
 *
 * Copyright (c) 2017 Elastacloud Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
             case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
                return (T)(object) parquetBlock.ToArray();

            default:
                return default(T);
          }
       }

      
   }
}
