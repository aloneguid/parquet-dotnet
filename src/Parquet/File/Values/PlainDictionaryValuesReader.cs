using Parquet.Data;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Parquet.File.Values
{
   class PlainDictionaryValuesReader : IValuesReader
   {
      public void Read(BinaryReader reader, SchemaElement schema, IList destination, long maxValues)
      {
         int bitWidth = reader.ReadByte();

         //when bit width is zero reader must stop and just repeat zero maxValue number of times
         if(bitWidth == 0)
         {
            destination.AddRange(Enumerable.Repeat(0, (int)maxValues));
            return;
         }

         int length = GetRemainingLength(reader);

         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, length, (List<int>)destination);
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static int GetRemainingLength(BinaryReader reader)
      {
         return (int)(reader.BaseStream.Length - reader.BaseStream.Position);
      }
   }
}
