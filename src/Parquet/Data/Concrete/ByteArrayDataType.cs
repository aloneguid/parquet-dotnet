using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.Data
{
   class ByteArrayDataType : BasicDataType<byte[]>
   {
      public ByteArrayDataType() : base(DataType.ByteArray, Thrift.Type.BYTE_ARRAY)
      {
      }

      public override IList CreateEmptyList(bool isNullable, int capacity)
      {
         return new List<byte[]>();
      }

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         List<byte[]> result = (List<byte[]>)CreateEmptyList(tse.IsNullable(), 0);

         while(reader.BaseStream.Position < reader.BaseStream.Length)
         {
            int length = reader.ReadInt32();
            byte[] data = reader.ReadBytes(length);
            result.Add(data);
         }

         return result;
      }

      public override void Write(BinaryWriter writer, IList values)
      {
         throw new System.NotImplementedException();
      }
   }
}
