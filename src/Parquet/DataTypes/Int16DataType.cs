using System;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int16DataType : BasicPrimitiveDataType<short>
   {
      public Int16DataType() : base(Thrift.Type.INT32, Thrift.ConvertedType.INT_16, 16)
      {

      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Short, parent);
      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, short> readOneFunc)
      {
         typeWidth = 2;
         readOneFunc = r => r.ReadInt16();
      }
   }
}
