using Parquet.Data;

namespace Parquet.DataTypes
{
   class ByteArrayDataType : BasicDataType<byte[]>
   {
      public ByteArrayDataType() : base(Thrift.Type.BYTE_ARRAY)
      {
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.ByteArray, parent);
      }
   }
}
