using System;
using System.Collections;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class FloatDataType : BasicPrimitiveDataType<float>
   {
      public FloatDataType() : base(Thrift.Type.FLOAT)
      {
      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, float> readOneFunc)
      {
         typeWidth = 4;
         readOneFunc = r => r.ReadSingle();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Float, parent);
      }
   }
}
