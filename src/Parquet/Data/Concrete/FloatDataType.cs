using System;
using System.Collections;
using System.IO;
using Parquet.Data;

namespace Parquet.Data
{
   class FloatDataType : BasicPrimitiveDataType<float>
   {
      public FloatDataType() : base(DataType.Float, Thrift.Type.FLOAT)
      {
      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, float> readOneFunc)
      {
         typeWidth = 4;
         readOneFunc = r => r.ReadSingle();
      }
   }
}
