using System;
using System.Buffers;
using System.IO;
using System.Text;
using Parquet.Extensions;

namespace Parquet.Data.Concrete
{
   class StringDataTypeHandler : BasicDataTypeHandler<string>
   {
      private static readonly Encoding E = Encoding.UTF8;
      private static readonly ArrayPool<byte> _bytePool = ArrayPool<byte>.Shared;
      private static readonly ArrayPool<string> _stringPool = ArrayPool<string>.Shared;

      public StringDataTypeHandler() : base(DataType.String, Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8)
      {
      }

      public override Array GetArray(int minCount, bool rent, bool isNullable)
      {
         if (rent)
         {
            return _stringPool.Rent(minCount);
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
         int remLength = (int)(reader.BaseStream.Length - reader.BaseStream.Position);

         if (remLength == 0)
            return 0;

         string[] tdest = (string[])dest;

         //reading string one by one is extremely slow, read all data

         byte[] allBytes = _bytePool.Rent(remLength);
         reader.BaseStream.Read(allBytes, 0, remLength);
         int destIdx = offset;
         try
         {
            Span<byte> span = allBytes.AsSpan(0, remLength);   //will be passed as input in future versions

            int spanIdx = 0;

            while(spanIdx < span.Length && destIdx < tdest.Length)
            {
               int length = span.Slice(spanIdx, 4).ReadInt32();
               byte[] buffer = span.Slice(spanIdx + 4, length).ToArray();
               string s = E.GetString(buffer, 0, length);
               tdest[destIdx++] = s;
               spanIdx = spanIdx + 4 + length;
            }
         }
         finally
         {
            _bytePool.Return(allBytes);
         }

         return destIdx - offset;
      }

      public override Array PackDefinitions(Array data, int maxDefinitionLevel, out int[] definitions, out int definitionsLength)
      {
         return PackDefinitions<string>((string[])data, maxDefinitionLevel, out definitions, out definitionsLength);
      }

      public override Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel, out bool[] hasValueFlags)
      {
         return UnpackGenericDefinitions((string[])src, definitionLevels, maxDefinitionLevel, out hasValueFlags);
      }

      protected override string ReadOne(BinaryReader reader)
      {
         int length = reader.ReadInt32();

         byte[] buffer = _bytePool.Rent(length);
         reader.Read(buffer, 0, length);
         string s = E.GetString(buffer, 0, length);   //can't avoid this :(
         _bytePool.Return(buffer);

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