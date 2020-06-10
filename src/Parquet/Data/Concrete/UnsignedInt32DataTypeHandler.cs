﻿using System.IO;

namespace Parquet.Data.Concrete
{
   class UnsignedInt32DataTypeHandler : BasicPrimitiveDataTypeHandler<uint>
   {
      public UnsignedInt32DataTypeHandler() : base(DataType.UnsignedInt32, Thrift.Type.INT32, Thrift.ConvertedType.UINT_32)
      {

      }

      protected override uint ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return reader.ReadUInt32();
      }

      protected override void WriteOne(BinaryWriter writer, uint value)
      {
         writer.Write(value);
      }
   }
}
