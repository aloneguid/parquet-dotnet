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

      protected override double ReadOne(BinaryReader reader)
      {
         return reader.ReadDouble();
      }

      protected override void WriteOne(BinaryWriter writer, double value)
      {
         writer.Write(value);
      }
   }
}
