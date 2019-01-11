using System;
using System.Collections;
using System.IO;

namespace Parquet.Data.Concrete
{
   class BooleanDataTypeHandler : BasicPrimitiveDataTypeHandler<bool>
   {
      public BooleanDataTypeHandler() : base(DataType.Boolean, Thrift.Type.BOOLEAN)
      {
      }

      public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset)
      {
         int start = offset;

         int ibit = 0;
         bool[] bdest = (bool[])dest;

         while (reader.BaseStream.Position < reader.BaseStream.Length && offset < dest.Length)
         {
            byte b = reader.ReadByte();

            while (ibit < 8 && offset < dest.Length)
            {
               bool set = ((b >> ibit++) & 1) == 1;
               bdest[offset++] = set;
            }

            ibit = 0;
         }


         return offset - start;
      }

      /// <summary>
      /// Normally bools are packed, which is implemented in <see cref="Read(BinaryReader, Thrift.SchemaElement, Array, int)"/>
      /// </summary>
      protected override bool ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length)
      {
         return reader.ReadBoolean();
      }

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values, Thrift.Statistics statistics)
      {
         int n = 0;
         byte b = 0;
         byte[] buffer = new byte[values.Count / 8 + 1];
         int ib = 0;

         foreach (bool flag in values)
         {
            if (flag)
            {
               b |= (byte)(1 << n);
            }

            n++;
            if (n == 8)
            {
               buffer[ib++] = b;
               n = 0;
               b = 0;
            }
         }

         if (n != 0) buffer[ib] = b;

         writer.Write(buffer);
      }
   }
}
