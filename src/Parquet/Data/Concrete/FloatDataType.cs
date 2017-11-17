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

      protected override float ReadOne(BinaryReader reader)
      {
         return reader.ReadSingle();
      }
   }
}
