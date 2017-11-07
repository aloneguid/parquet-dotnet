namespace Parquet.DataTypes
{
   class BooleanDataType : BasicValueDataType<bool>
   {
      public BooleanDataType() : base(Thrift.Type.BOOLEAN, null, 1)
      {
      }
   }
}
