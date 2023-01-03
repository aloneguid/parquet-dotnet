using System.IO;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class UnsignedInt64DataTypeHandler : BasicPrimitiveDataTypeHandler<ulong>
   {
      public UnsignedInt64DataTypeHandler() : base(DataType.UnsignedInt64, Thrift.Type.INT64, Thrift.ConvertedType.UINT_64)
      {

      }

      protected override ulong ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return reader.ReadUInt64();
      }

      protected override void WriteOne(BinaryWriter writer, ulong value)
      {
         writer.Write(value);
      }
   }
}
