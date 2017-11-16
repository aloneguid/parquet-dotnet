using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data
{
   class StringDataType : BasicDataType<string>
   {
      public StringDataType() : base(DataType.String, Thrift.Type.BYTE_ARRAY)
      {
      }

      public override IList CreateEmptyList(bool isNullable, int capacity)
      {
         return new List<string>(capacity);
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

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         var result = new List<string>();

         while(reader.BaseStream.Position < reader.BaseStream.Length)
         {
            int length = reader.ReadInt32();
            byte[] data = reader.ReadBytes(length);
            string s = Encoding.UTF8.GetString(data);
            result.Add(s);
         }

         return result;
      }

      public override void Write(BinaryWriter writer, IList values)
      {
         throw new System.NotImplementedException();
      }
   }
}
