using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int64DataType : BasicDataType<long>
   {
      public Int64DataType() : base(Thrift.Type.INT64)
      {
      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.Int64, parent);
      }
   }
}
