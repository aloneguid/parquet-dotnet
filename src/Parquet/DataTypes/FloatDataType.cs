using Parquet.Data;

namespace Parquet.DataTypes
{
   class FloatDataType : BasicDataType<float>
   {
      public FloatDataType() : base(Thrift.Type.FLOAT)
      {
      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.Float, parent);
      }
   }
}
