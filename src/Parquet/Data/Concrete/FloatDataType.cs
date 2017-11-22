using System.IO;

namespace Parquet.Data.Concrete
{
   class FloatDataType : BasicPrimitiveDataType<float>
   {
      public FloatDataType() : base(DataType.Float, Thrift.Type.FLOAT)
      {
      }

      protected override float ReadOne(BinaryReader reader)
      {
         return reader.ReadSingle();
      }

      protected override void WriteOne(BinaryWriter writer, float value)
      {
         writer.Write(value);
      }
   }
}
