using System;
using System.IO;
using Parquet.Data;

namespace Parquet.Data
{
   class DoubleDataType : BasicPrimitiveDataType<double>
   {
      public DoubleDataType() : base(DataType.Double, Thrift.Type.DOUBLE)
      {

      }

      protected override void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, double> readOneFunc)
      {
         typeWidth = 8;
         readOneFunc = r => r.ReadDouble();
      }
   }
}
