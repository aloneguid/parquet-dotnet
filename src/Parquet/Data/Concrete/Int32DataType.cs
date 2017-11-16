using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.Data
{
   class Int32DataType : BasicPrimitiveDataType<int>
   {
      public Int32DataType() : base(DataType.Int32, Thrift.Type.INT32, null, 32)
      {
      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, int> readOneFunc)
      {
         typeWidth = 4;
         readOneFunc = r => r.ReadInt32();
      }
   }
}
