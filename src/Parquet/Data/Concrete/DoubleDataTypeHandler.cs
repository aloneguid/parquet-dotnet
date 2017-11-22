using System;
using System.IO;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class DoubleDataTypeHandler : BasicPrimitiveDataTypeHandler<double>
   {
      public DoubleDataTypeHandler() : base(DataType.Double, Thrift.Type.DOUBLE)
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
