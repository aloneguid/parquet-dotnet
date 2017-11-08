using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;
using Parquet.Thrift;

namespace Parquet.DataTypes
{
   class IntervalDataType : BasicDataType<Interval>
   {
      public IntervalDataType() : base(Thrift.Type.FIXED_LEN_BYTE_ARRAY, ConvertedType.INTERVAL)
      {

      }

      protected override SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement2(tse.Name, DataType.Interval, parent);
      }
   }
}
