using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int32DataType : BasicDataType<int>
   {
      public Int32DataType() : base(Thrift.Type.INT32, null, 32)
      {
      }

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         var result = new List<int>();

         while (reader.BaseStream.Position + 4 <= reader.BaseStream.Length)
         {
            result.Add(reader.ReadInt32());
         }

         return result;
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Int32, parent);
      }
   }
}
