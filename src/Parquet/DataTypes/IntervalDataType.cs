using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;

namespace Parquet.DataTypes
{
   class IntervalDataType : BasicDataType<Interval>
   {
      public IntervalDataType() : base(Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.INTERVAL)
      {

      }

      public override IList Read(BinaryReader reader)
      {
         throw new NotImplementedException();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Interval, parent);
      }
   }
}
