using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int32DataType : BasicDataType<int>
   {
      public Int32DataType() : base(Thrift.Type.INT32, null, 32)
      {
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Int32, parent);
      }
   }
}
