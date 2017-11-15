using System;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class DoubleDataType : BasicPrimitiveDataType<double>
   {
      public DoubleDataType() : base(Thrift.Type.DOUBLE)
      {

      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, double> readOneFunc)
      {
         typeWidth = 8;
         readOneFunc = r => r.ReadDouble();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Double, parent);
      }
   }
}
