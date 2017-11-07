namespace Parquet.DataTypes
{
   class Int32DataType : BasicValueDataType<int>
   {
      public Int32DataType() : base(Thrift.Type.INT32, null, 32)
      {
      }
   }
}
