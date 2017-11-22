using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class BooleanDataTypeHandler : BasicPrimitiveDataTypeHandler<bool>
   {
      public BooleanDataTypeHandler() : base(DataType.Boolean, Thrift.Type.BOOLEAN)
      {
      }

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         IList dest = CreateEmptyList(tse.IsNullable(), false, 0);

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

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         var lst = (List<bool>)values;
         int n = 0;
         byte b = 0;
         byte[] buffer = new byte[lst.Count / 8 + 1];
         int ib = 0;

         foreach (bool flag in lst)
         {
            if (flag)
            {
               b |= (byte)(1 << n);
            }

            if (n == 8)
            {
               buffer[ib++] = b;
               n = 0;
               b = 0;
            }

            n += 1;
         }

         if (n != 0) buffer[ib] = b;

         writer.Write(buffer);
      }
   }
}
