using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class Int96DataType : BasicPrimitiveDataType<BigInteger>
   {
      public Int96DataType() : base(Thrift.Type.INT96, null, 12)
      {
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.Type == Thrift.Type.INT96 && !formatOptions.TreatBigIntegersAsDates;
      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, BigInteger> readOneFunc)
      {
         typeWidth = 12;
         readOneFunc = (r) =>
         {
            byte[] data = r.ReadBytes(12);
            var big = new BigInteger(data);
            return big;
         };
      }

      protected override Data.SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Int96, parent);
      }
   }
}
