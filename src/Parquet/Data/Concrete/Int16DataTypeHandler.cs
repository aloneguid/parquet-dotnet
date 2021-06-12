using System;
using System.IO;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class Int16DataTypeHandler : BasicPrimitiveDataTypeHandler<Int16>
   {
      public Int16DataTypeHandler() : base(DataType.Int16, Thrift.Type.INT32, Thrift.ConvertedType.INT_16)
      {

      }

      protected override short ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return (short) reader.ReadInt32();
      }

      protected override void WriteOne(BinaryWriter writer, short value)
      {
         writer.Write((int) value);
      }
   }
}
