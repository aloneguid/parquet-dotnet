using System.Collections;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int64DataType : BasicDataType<long>
   {
      public Int64DataType() : base(Thrift.Type.INT64)
      {
      }

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         throw new System.NotImplementedException();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Int64, parent);
      }
   }
}
