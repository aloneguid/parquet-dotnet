using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class DoubleDataType : BasicDataType<double>
   {
      public DoubleDataType() : base(Thrift.Type.DOUBLE)
      {

      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Double, parent);
      }
   }
}
