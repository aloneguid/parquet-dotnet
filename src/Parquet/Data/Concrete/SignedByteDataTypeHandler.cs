using System.IO;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class SignedByteDataTypeHandler : BasicPrimitiveDataTypeHandler<sbyte>
   {
      public SignedByteDataTypeHandler(): base(DataType.SignedByte, Thrift.Type.INT32, Thrift.ConvertedType.INT_8)
      {

      }

      protected override sbyte ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return (sbyte) reader.ReadInt32();
      }

      protected override void WriteOne(BinaryWriter writer, sbyte value)
      {
         writer.Write((int) value);
      }
   }
}
