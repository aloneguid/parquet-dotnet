using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data
{
   class SignedByteDataType : BasicPrimitiveDataType<sbyte>
   {
      public SignedByteDataType(): base(DataType.SignedByte, Thrift.Type.INT32, Thrift.ConvertedType.INT_8, 8)
      {

      }

      protected override sbyte ReadOne(BinaryReader reader)
      {
         return reader.ReadSByte();
      }
   }
}
