using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class ByteDataType : BasicPrimitiveDataType<byte>
   {
      public ByteDataType(): base(DataType.Byte, Thrift.Type.INT32, Thrift.ConvertedType.UINT_8)
      {

      }

      protected override byte ReadOne(BinaryReader reader)
      {
         return reader.ReadByte();
      }

      protected override void WriteOne(BinaryWriter writer, byte value)
      {
         writer.Write(value);
      }
   }
}
