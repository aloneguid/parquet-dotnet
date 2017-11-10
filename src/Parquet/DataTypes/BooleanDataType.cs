using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class BooleanDataType : BasicDataType<bool>
   {
      public BooleanDataType() : base(Thrift.Type.BOOLEAN, null, 1)
      {
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Boolean, parent);
      }

      public override IList Read(BinaryReader reader)
      {
         var dest = new List<bool>();

         int ibit = 0;

         while(reader.BaseStream.Position < reader.BaseStream.Length)
         {
            byte b = reader.ReadByte();

            while(ibit <= 8)
            {
               bool set = ((b >> ibit++) & 1) == 1;
               dest.Add(set);
            }

            ibit = 0;
         }

         return dest;
      }
   }
}
