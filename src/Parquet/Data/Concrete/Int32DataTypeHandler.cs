using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class Int32DataTypeHandler : BasicPrimitiveDataTypeHandler<int>
   {
      public Int32DataTypeHandler() : base(DataType.Int32, Thrift.Type.INT32)
      {
      }

      protected override int ReadOne(BinaryReader reader)
      {
         return reader.ReadInt32();
      }

      protected override void WriteOne(BinaryWriter writer, int value)
      {
         writer.Write(value);
      }
   }
}
