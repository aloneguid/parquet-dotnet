using System.IO;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class UnsignedShortDataTypeHandler : BasicPrimitiveDataTypeHandler<ushort>
   {
      public UnsignedShortDataTypeHandler() : base(DataType.UnsignedShort, Thrift.Type.INT32, Thrift.ConvertedType.UINT_16)
      {

      }

      protected override ushort ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return (ushort)reader.ReadUInt32();
      }

      protected override void WriteOne(BinaryWriter writer, ushort value)
      {
         writer.Write((int)value);
      }
   }
}
