using System;
using System.Collections;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int64DataType : BasicPrimitiveDataType<long>
   {
      public Int64DataType() : base(Thrift.Type.INT64)
      {
      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, long> readOneFunc)
      {
         typeWidth = 8;
         readOneFunc = r => r.ReadInt64();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Int64, parent);
      }
   }
}
