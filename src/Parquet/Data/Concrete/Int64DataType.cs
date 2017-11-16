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

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, long> readOneFunc)
      {
         typeWidth = 8;
         readOneFunc = r => r.ReadInt64();
      }
   }
}
