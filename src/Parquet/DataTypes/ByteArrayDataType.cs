using Parquet.Data;

namespace Parquet.DataTypes
{
   class ByteArrayDataType : BasicDataType<byte[]>
   {
      public ByteArrayDataType() : base(Thrift.Type.BYTE_ARRAY)
      {
      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.ByteArray, parent);
      }
   }
}
