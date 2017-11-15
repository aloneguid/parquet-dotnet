using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class SignedByteDataType : BasicPrimitiveDataType<sbyte>
   {
      public SignedByteDataType(): base(DataType.SignedByte, Thrift.Type.INT32, Thrift.ConvertedType.INT_8, 8)
      {

      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, sbyte> readOneFunc)
      {
         typeWidth = 1;
         readOneFunc = r => r.ReadSByte();
      }
   }
}
