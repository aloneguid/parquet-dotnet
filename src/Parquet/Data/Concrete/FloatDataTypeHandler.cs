using System.IO;

namespace Parquet.Data.Concrete
{
   class FloatDataTypeHandler : BasicPrimitiveDataTypeHandler<float>
   {
      public FloatDataTypeHandler() : base(DataType.Float, Thrift.Type.FLOAT)
      {
      }

      protected override float ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return reader.ReadSingle();
      }

      protected override void WriteOne(BinaryWriter writer, float value)
      {
         writer.Write(value);
      }
   }
}
