using System;
using System.IO;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class UnsignedInt16DataType : BasicPrimitiveDataType<ushort>
   {
      public UnsignedInt16DataType() : base(DataType.UnsignedInt16, Thrift.Type.INT32, Thrift.ConvertedType.UINT_16, 16)
      {

      }

      protected override ushort ReadOne(BinaryReader reader)
      {
         return reader.ReadUInt16();
      }

      protected override void WriteOne(BinaryWriter writer, ushort value)
      {
         writer.Write(value);
      }
   }
}
