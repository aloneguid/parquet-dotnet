using Parquet.Data;

namespace Parquet.DataTypes
{
   class FloatDataType : BasicDataType<float>
   {
      public FloatDataType() : base(Thrift.Type.FLOAT)
      {
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Float, parent);
      }
   }
}
