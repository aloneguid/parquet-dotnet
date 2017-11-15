using System.Collections;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class FloatDataType : BasicDataType<float>
   {
      public FloatDataType() : base(Thrift.Type.FLOAT)
      {
      }

      public override IList Read(BinaryReader reader)
      {
         throw new System.NotImplementedException();
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Float, parent);
      }
   }
}
