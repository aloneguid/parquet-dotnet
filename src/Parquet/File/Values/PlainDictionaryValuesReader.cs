using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Thrift;
using System.Runtime.CompilerServices;

namespace Parquet.File.Values
{
   class PlainDictionaryValuesReader : IValuesReader
   {
      public void Read(BinaryReader reader, SchemaElement schema, IList destination, long maxValues)
      {
         int bitWidth = reader.ReadByte();
         int length = GetRemainingLength(reader);

         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, length, (List<int>)destination, maxValues);
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static int GetRemainingLength(BinaryReader reader)
      {
         return (int)(reader.BaseStream.Length - reader.BaseStream.Position);
      }
   }
}
