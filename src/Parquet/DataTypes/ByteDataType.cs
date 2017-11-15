using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class ByteDataType : BasicPrimitiveDataType<byte>
   {
      public ByteDataType(): base(Thrift.Type.INT32, Thrift.ConvertedType.UINT_8, 8)
      {

      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, byte> readOneFunc)
      {
         typeWidth = 1;
         readOneFunc = r => r.ReadByte();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Byte, parent);
      }
   }
}
