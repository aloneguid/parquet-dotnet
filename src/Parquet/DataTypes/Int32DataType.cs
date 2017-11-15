using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int32DataType : BasicPrimitiveDataType<int>
   {
      public Int32DataType() : base(Thrift.Type.INT32, null, 32)
      {
      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, int> readOneFunc)
      {
         typeWidth = 4;
         readOneFunc = r => r.ReadInt32();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Int32, parent);
      }
   }
}
