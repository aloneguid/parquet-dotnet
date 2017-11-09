using System.Collections;
using System.Collections.Generic;
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

      public override IList Read(byte[] data)
      {
         var dest = new List<bool>();

         int ibit = 0;
         int ibyte = 0;
         byte b = data[0];

         while(ibyte < data.Length)
         {
            if (ibit == 8)
            {
               if (ibyte + 1 >= data.Length)
               {
                  break;
               }
               b = data[++ibyte];
               ibit = 0;
            }

            bool set = ((b >> ibit++) & 1) == 1;
            dest.Add(set);
         }

         return dest;
      }
   }
}
