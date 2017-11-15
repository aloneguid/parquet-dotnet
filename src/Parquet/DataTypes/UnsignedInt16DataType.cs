using System;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class UnsignedInt16DataType : BasicPrimitiveDataType<ushort>
   {
      public UnsignedInt16DataType() : base(Thrift.Type.INT32, Thrift.ConvertedType.UINT_16, 16)
      {

      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.UnsignedShort, parent);
      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, ushort> readOneFunc)
      {
         typeWidth = 2;
         readOneFunc = r => r.ReadUInt16();
      }
   }
}
