using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data
{
   class StringDataType : BasicDataType<string>
   {
      public StringDataType() : base(DataType.String, Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8)
      {
      }

      public override IList CreateEmptyList(bool isNullable, bool isArray, int capacity)
      {
         return isArray
            ? (IList)(new List<List<string>>())
            : (IList)(new List<string>(capacity));
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.type &&
            tse.Type == Thrift.Type.BYTE_ARRAY &&
            (
               (tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.UTF8) ||
               formatOptions.TreatByteArrayAsString
            );
      }

      protected override string ReadOne(BinaryReader reader)
      {
         int length = reader.ReadInt32();
         byte[] data = reader.ReadBytes(length);
         string s = Encoding.UTF8.GetString(data);
         return s;
      }

      protected override void WriteOne(BinaryWriter writer, string value)
      {
         if(value.Length == 0)
         {
            writer.Write((int)0);
         }
         else
         {
            //transofrm to byte array first, as we need the length of the byte buffer, not string length
            byte[] data = Encoding.UTF8.GetBytes(value);
            writer.Write((int)data.Length);
            writer.Write(data);
         }
      }
   }
}
