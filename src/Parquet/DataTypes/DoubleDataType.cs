using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Thrift;

namespace Parquet.DataTypes
{
   class DoubleDataType : BasicDataType<double>
   {
      public DoubleDataType() : base(Thrift.Type.DOUBLE)
      {

      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.Double, parent);
      }
   }
}
