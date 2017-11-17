using System;
using System.Collections;
using System.IO;
using Parquet.Data;

namespace Parquet.Data
{
   class Int64DataType : BasicPrimitiveDataType<long>
   {
      public Int64DataType() : base(DataType.Int64, Thrift.Type.INT64)
      {
      }

      protected override long ReadOne(BinaryReader reader)
      {
         return reader.ReadInt64();
      }
   }
}
