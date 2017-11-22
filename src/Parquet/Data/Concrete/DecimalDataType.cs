using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.File.Values.Primitives;

namespace Parquet.Data.Concrete
{
   class DecimalDataType : BasicPrimitiveDataType<decimal>
   {
      public DecimalDataType() : base(DataType.Decimal, Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.DECIMAL)
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

      public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         base.CreateThrift(se, parent, container);

         //modify this element slightly
         Thrift.SchemaElement tse = container.Last();

         if (se is DecimalDataField dse)
         {
            if(dse.ForceByteArrayEncoding)
            {
               tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
            }
            else
            {
               if (dse.Precision <= 9)
                  tse.Type = Thrift.Type.INT32;
               else if (dse.Precision <= 18)
                  tse.Type = Thrift.Type.INT64;
               else
                  tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
            }

            tse.Precision = dse.Precision;
            tse.Scale = dse.Scale;
            tse.Type_length = BigDecimal.GetBufferSize(dse.Precision);
         }
         else
         {
            //set defaults
            tse.Precision = 38;
            tse.Scale = 18;
            tse.Type_length = 16;
         }
      }

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         IList result = CreateEmptyList(tse.IsNullable(), false, 0);

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

      public override void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         switch(tse.Type)
         {
            case Thrift.Type.INT32:
               WriteAsInt32(tse, writer, values);
               break;
            case Thrift.Type.INT64:
               WriteAsInt64(tse, writer, values);
               break;
            case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
               WriteAsFixedLengthByteArray(tse, writer, values);
               break;
            default:
               throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
         }
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

      private void WriteAsInt32(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         double scaleFactor = Math.Pow(10, tse.Scale);
         foreach (decimal d in values)
         {
            try
            {
               int i = (int)(d * (decimal)scaleFactor);
               writer.Write(i);
            }
            catch (OverflowException)
            {
               throw new ParquetException(
                  $"value '{d}' is too large to fit into scale {tse.Scale} and precision {tse.Precision}");
            }
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

      private void WriteAsInt64(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         double scaleFactor = Math.Pow(10, tse.Scale);

         foreach (decimal d in values)
         {
            try
            {
               long l = (long)(d * (decimal)scaleFactor);
               writer.Write(l);
            }
            catch (OverflowException)
            {
               throw new ParquetException(
                  $"value '{d}' is too large to fit into scale {tse.Scale} and precision {tse.Precision}");
            }
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

      private void WriteAsFixedLengthByteArray(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         foreach (decimal d in values)
         {
            var bd = new BigDecimal(d, tse.Precision, tse.Scale);
            byte[] itemData = bd.ToByteArray();
            tse.Type_length = itemData.Length; //always re-set type length as it can differ from default type length

            writer.Write(itemData);
         }
      }

   }
}
