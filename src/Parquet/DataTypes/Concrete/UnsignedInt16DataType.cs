using System;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class UnsignedInt16DataType : BasicPrimitiveDataType<ushort>
   {
      public UnsignedInt16DataType() : base(DataType.UnsignedInt16, Thrift.Type.INT32, Thrift.ConvertedType.UINT_16, 16)
      {

      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, ushort> readOneFunc)
      {
         typeWidth = 2;
         readOneFunc = r => r.ReadUInt16();
      }
   }
}
