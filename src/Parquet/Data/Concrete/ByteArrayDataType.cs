using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class ByteArrayDataType : BasicDataType<byte[]>
   {
      public ByteArrayDataType() : base(DataType.ByteArray, Thrift.Type.BYTE_ARRAY)
      {
      }

      public override IList CreateEmptyList(bool isNullable, bool isArray, int capacity)
      {
         return isArray
            ? (IList)(new List<List<byte[]>>())
            : (IList)(new List<byte[]>());
      }

      protected override byte[] ReadOne(BinaryReader reader)
      {
         int length = reader.ReadInt32();
         byte[] data = reader.ReadBytes(length);
         return data;
      }

      protected override void WriteOne(BinaryWriter writer, byte[] value)
      {
         writer.Write(value.Length);
         writer.Write(value);
      }
   }
}
