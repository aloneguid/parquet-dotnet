using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;

namespace Parquet.DataTypes
{
   class DecimalDataType : BasicPrimitiveDataType<decimal>
   {
      public DecimalDataType() : base(Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.DECIMAL)
      {
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return

            tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DECIMAL &&

            (
               tse.Type == Thrift.Type.FIXED_LEN_BYTE_ARRAY ||
               tse.Type == Thrift.Type.INT32 ||
               tse.Type == Thrift.Type.INT64
            );
      }

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         IList result = CreateEmptyList(tse, formatOptions, 0);

         switch(tse.Type)
         {
            case Thrift.Type.INT32:
               ReadAsInt32(tse, reader, result);
               break;
            case Thrift.Type.INT64:
               ReadAsInt64(tse, reader, result);
               break;
            case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
               ReadAsFixedLengthByteArray(tse, reader, result);
               break;
            default:
               throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
         }

         return result;
      }

      private void ReadAsInt32(Thrift.SchemaElement tse, BinaryReader reader, IList result)
      {
         decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
         while(reader.BaseStream.Position + 4 <= reader.BaseStream.Length)
         {
            int iv = reader.ReadInt32();
            decimal dv = iv * scaleFactor;
            result.Add(dv);
         }
      }

      private void ReadAsInt64(Thrift.SchemaElement tse, BinaryReader reader, IList result)
      {
         decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
         while (reader.BaseStream.Position + 8 <= reader.BaseStream.Length)
         {
            long lv = reader.ReadInt64();
            decimal dv = lv * scaleFactor;
            result.Add(dv);
         }
      }

      private void ReadAsFixedLengthByteArray(Thrift.SchemaElement tse, BinaryReader reader, IList result)
      {
         int typeLength = tse.Type_length;

         //can't read if there is no type length set
         if (typeLength == 0) return;

         while (reader.BaseStream.Position + typeLength <= reader.BaseStream.Length)
         {
            byte[] itemData = reader.ReadBytes(typeLength);
            decimal dc = new BigDecimal(itemData, tse);
            result.Add(dc);
         }
      }

      protected override SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType.Decimal, parent);
      }
   }
}
