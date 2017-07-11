using Parquet.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Parquet.File.Values
{
   class RunLengthBitPackingHybridValuesReader : IValuesReader
   {
      public void Read(BinaryReader reader, SchemaElement schema, IList destination, long maxValues)
      {
         int bitWidth = schema.Thrift.Type_length;
         List<int> destinationTyped = (List<int>)destination;
         int length = GetRemainingLength(reader);
         ReadRleBitpackedHybrid(reader, bitWidth, length, destinationTyped, maxValues);
      }

      /* from specs:
       * rle-bit-packed-hybrid: <length> <encoded-data>
       * length := length of the <encoded-data> in bytes stored as 4 bytes little endian
       * encoded-data := <run>*
       * run := <bit-packed-run> | <rle-run>  
       * bit-packed-run := <bit-packed-header> <bit-packed-values>  
       * bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)  
       * // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8  
       * bit-pack-count := (number of values in this run) / 8  
       * bit-packed-values := *see 1 below*  
       * rle-run := <rle-header> <repeated-value>  
       * rle-header := varint-encode( (number of times repeated) << 1)  
       * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
       */

      public static void ReadRleBitpackedHybrid(BinaryReader reader, int bitWidth, int length, List<int> destination, long maxValues)
      {
         if (length == 0) length = reader.ReadInt32();

         long start = reader.BaseStream.Position;
         while (reader.BaseStream.Position - start < length)
         {
            int header = ReadUnsignedVarInt(reader);
            bool isRle = (header & 1) == 0;

            if (isRle)
            {
               ReadRle(header, reader, bitWidth, destination);
            }
            else
            {
               ReadBitpacked(header, reader, bitWidth, destination);
            }
         }
      }

      /// <summary>
      /// Read run-length encoded run from the given header and bit length.
      /// </summary>
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadRle(int header, BinaryReader reader, int bitWidth, List<int> destination)
      {
         // The count is determined from the header and the width is used to grab the
         // value that's repeated. Yields the value repeated count times.

         int count = header >> 1;
         int width = (bitWidth + 7) / 8; //round up to next byte
         byte[] data = reader.ReadBytes(width);
         int value = ReadIntOnBytes(data);
         destination.AddRange(Enumerable.Repeat(value, count));
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void ReadBitpacked(int header, BinaryReader reader, int bitWidth, List<int> destination)
      {
         int groupCount = header >> 1;
         int count = groupCount * 8;
         int byteCount = (bitWidth * count) / 8;

         byte[] rawBytes = reader.ReadBytes(byteCount);
         int mask = MaskForBits(bitWidth);

         int i = 0;
         int b = rawBytes[i];
         int total = byteCount * 8;
         int bwl = 8;
         int bwr = 0;
         while (total >= bitWidth)
         {
            if (bwr >= 8)
            {
               bwr -= 8;
               bwl -= 8;
               b >>= 8;
            }
            else if (bwl - bwr >= bitWidth)
            {
               int r = ((b >> bwr) & mask);
               total -= bitWidth;
               bwr += bitWidth;

               destination.Add(r);
            }
            else if (i + 1 < byteCount)
            {
               i += 1;
               b |= (rawBytes[i] << bwl);
               bwl += 8;
            }
         }
      }

      /// <summary>
      /// Read a value using the unsigned, variable int encoding.
      /// </summary>
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static int ReadUnsignedVarInt(BinaryReader reader)
      {
         int result = 0;
         int shift = 0;

         while (true)
         {
            byte b = reader.ReadByte();
            result |= ((b & 0x7F) << shift);
            if ((b & 0x80) == 0) break;
            shift += 7;
         }

         return result;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static int ReadIntOnBytes(byte[] data)
      {
         switch (data.Length)
         {
            case 0:
               return 0;
            case 1:
               return (data[0]);
            case 2:
               return (data[1] << 8) + (data[0]);
            case 3:
               return (data[2] << 16) + (data[1] << 8) + (data[0]);
            case 4:
               return BitConverter.ToInt32(data, 0);
            default:
               throw new IOException($"encountered byte width ({data.Length}) that requires more than 4 bytes.");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static int MaskForBits(int width)
      {
         return (1 << width) - 1;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static int GetRemainingLength(BinaryReader reader)
      {
         return (int)(reader.BaseStream.Length - reader.BaseStream.Position);
      }
   }
}
