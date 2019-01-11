using System.IO;

namespace Parquet.Data.Concrete
{
   class UnsignedInt16DataTypeHandler : BasicPrimitiveDataTypeHandler<ushort>
   {
      public UnsignedInt16DataTypeHandler() : base(DataType.UnsignedInt16, Thrift.Type.INT32, Thrift.ConvertedType.UINT_16)
      {

      }

      protected override ushort ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return reader.ReadUInt16();
      }

      protected override void WriteOne(BinaryWriter writer, ushort value)
      {
         writer.Write(value);
      }
   }
}
