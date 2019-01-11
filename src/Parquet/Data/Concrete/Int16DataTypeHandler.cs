using System;
using System.IO;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class Int16DataTypeHandler : BasicPrimitiveDataTypeHandler<short>
   {
      public Int16DataTypeHandler() : base(DataType.Short, Thrift.Type.INT32, Thrift.ConvertedType.INT_16)
      {

      }

      protected override short ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return reader.ReadInt16();
      }

      protected override void WriteOne(BinaryWriter writer, short value)
      {
         writer.Write(value);
      }
   }
}
