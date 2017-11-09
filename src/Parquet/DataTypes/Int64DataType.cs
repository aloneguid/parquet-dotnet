using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int64DataType : BasicDataType<long>
   {
      public Int64DataType() : base(Thrift.Type.INT64)
      {
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Int64, parent);
      }
   }
}
