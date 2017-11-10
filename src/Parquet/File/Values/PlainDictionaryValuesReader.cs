using Parquet.Data;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Parquet.File.Values
{
   class PlainDictionaryValuesReader : IValuesReader
   {
      //todo: kill this, only used in legacy reader
      public void Read(BinaryReader reader, SchemaElement schema, IList destination, long maxValues)
      {
         int bitWidth = reader.ReadByte();

         //when bit width is zero reader must stop and just repeat zero maxValue number of times
         if(bitWidth == 0)
         {
            for(int i = 0; i < maxValues; i++)
            {
               destination.Add(0);
            }
            return;
         }

         int length = GetRemainingLength(reader);

         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, length, (List<int>)destination);
      }

      public static List<int> Read(BinaryReader reader, long maxValues)
      {
         var result = new List<int>();
         int bitWidth = reader.ReadByte();

         //when bit width is zero reader must stop and just repeat zero maxValue number of times
         if (bitWidth == 0)
         {
            for (int i = 0; i < maxValues; i++)
            {
               result.Add(0);
            }
            return result;
         }

         int length = GetRemainingLength(reader);
         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, length, result);
         return result;
      }


      private static int GetRemainingLength(BinaryReader reader)
      {
         return (int)(reader.BaseStream.Length - reader.BaseStream.Position);
      }
   }
}
