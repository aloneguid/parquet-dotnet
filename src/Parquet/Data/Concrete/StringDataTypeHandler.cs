using System;
using System.Buffers;
using System.IO;
using System.Text;

namespace Parquet.Data.Concrete
{
   class StringDataTypeHandler : BasicDataTypeHandler<string>
   {
      private static readonly Encoding E = Encoding.UTF8;
      private static readonly ArrayPool<byte> Pool = ArrayPool<byte>.Shared;
      private static readonly ArrayPool<string> SPool = ArrayPool<string>.Shared;

      public StringDataTypeHandler() : base(DataType.String, Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8)
      {
      }

      public override Array GetArray(int minCount, bool rent, bool isNullable)
      {
         if (rent)
         {
            return SPool.Rent(minCount);
         }

         return new string[minCount];
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

      public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset, ParquetOptions formatOptions)
      {
         string[] tdest = (string[])dest;

         int totalLength = (int)reader.BaseStream.Length;
         int idx = offset;
         Stream s = reader.BaseStream;

         while (s.Position < totalLength)
         {
            string element = ReadOne(reader);
            tdest[idx++] = element;
         }

         return idx - offset;

      }

      public override void ReturnArray(Array array, bool isNullable)
      {
         SPool.Return((string[])array);
      }

      public override Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel)
      {
         return UnpackGenericDefinitions((string[])src, definitionLevels, maxDefinitionLevel);
      }

      protected override string ReadOne(BinaryReader reader)
      {
         int length = reader.ReadInt32();

         byte[] buffer = Pool.Rent(length);
         reader.Read(buffer, 0, length);
         string s = E.GetString(buffer, 0, length);   //can't avoid this :(
         Pool.Return(buffer);

         //non-optimised version
         //byte[] data = reader.ReadBytes(length);
         //string s = Encoding.UTF8.GetString(data);

         return s;
      }

      protected override void WriteOne(BinaryWriter writer, string value)
      {
         if (value.Length == 0)
         {
            writer.Write((int)0);
         }
         else
         {
            //transofrm to byte array first, as we need the length of the byte buffer, not string length
            byte[] data = E.GetBytes(value);
            writer.Write(data.Length);
            writer.Write(data);
         }
      }
   }
}