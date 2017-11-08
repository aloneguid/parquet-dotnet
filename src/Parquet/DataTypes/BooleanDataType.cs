using System.Collections.Generic;
using Parquet.Data;
using Parquet.Thrift;

namespace Parquet.DataTypes
{
   class BooleanDataType : BasicDataType<bool>
   {
      public BooleanDataType() : base(Type.BOOLEAN, null, 1)
      {
      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.Boolean, parent);
      }
   }
}
