using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using TType = global::Type;

namespace Parquet.File
{
   //todo: performance - inspect that appropriate type is used in bit shifting (i.e. don't use int for 8 bit values)


   /// <summary>
   /// Handles parquet encoding logic.
   /// All methods are made static for increased performance.
   /// </summary>
   static class PEncoding
   {
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

      public static void ReadRleBitpackedHybrid(BinaryReader reader)
      {
         int length = reader.ReadInt32();
         int header = ReadUnsignedVarInt(reader);
         bool isRle = (header & 1) == 0;

         if (isRle)
            ReadRle(header, reader);
         else
            ReadBitpacked(header, reader);
      }

      public static void ReadPlain(BinaryReader reader, TType thriftType)
      {
         long byteCount = reader.BaseStream.Length - reader.BaseStream.Position;
         byte[] data = reader.ReadBytes((int)byteCount);

         switch (thriftType)
         {
            case TType.BOOLEAN:
               //todo: avoid using BitArray as this requires creating a new class every time we read data
               bool[] rb = ReadPlainBoolean(data, 8);
               break;
            case TType.INT32:
               var r32 = new List<int>();
               for(int i = 0; i < data.Length; i += 4)
               {
                  int iv = BitConverter.ToInt32(data, i);
                  r32.Add(iv);
               }
               break;
            case TType.INT96:
               //todo: this is a sample how to read int96, not tested this yet
               var r96 = new List<BigInteger>();
               byte[] v96 = new byte[12];
               for(int i = 0; i < data.Length; i+= 12)
               {
                  Array.Copy(data, i, v96, 0, 12);
                  var bi = new BigInteger(v96);
               }
               break;
            default:
               throw new NotImplementedException($"type {thriftType} not implemented");
         }

      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static bool[] ReadPlainBoolean(byte[] data, int count)
      {
         var res = new bool[count];
         int ibit = 0;
         int ibyte = 0;
         byte b = data[0];

         for(int ires = 0; ires < res.Length; ires++)
         {
            if (ibit == 8)
            {
               b = data[++ibyte];
               ibit = 0;
            }

            bool set = ((b >> ibit++) & 1) == 1;
            res[ires] = set;
         }

         return res;
      }

      /// <summary>
      /// Read a value using the unsigned, variable int encoding.
      /// </summary>
      [MethodImpl(MethodImplOptions.AggressiveInlining)] //todo: mark every method with agressive inlining
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

      /// <summary>
      /// Read run-length encoded run from the given header and bit length.
      /// </summary>
      public static int[] ReadRle(int header, BinaryReader reader, int bitWidth = 1)
      {
         // The count is determined from the header and the width is used to grab the
         // value that's repeated. Yields the value repeated count times.

         int count = header >> 1;
         int width = bitWidth + 7 / 8; //round up to next byte
         byte[] data = reader.ReadBytes(width);
         //int value = BitConverter(data, 0);
         int value = (int)data[0];
         int[] result = Enumerable.Repeat(value, count).ToArray();   //todo: not sure how fast it is
         return result;
      }

      public static void ReadBitpacked(int header, BinaryReader reader, int width = 1)
      {
         int groupCount = header >> 1;
         int count = groupCount * 8;
         int byteCount = (width * count) / 8;

         byte[] rawBytes = reader.ReadBytes(byteCount);
         int mask = MaskForBits(width);

         int i = 0;
         byte b = rawBytes[i];
         int total = byteCount * 8;
         int bwl = 8;
         int bwr = 0;
         var tmp = new List<byte>();
         while (total >= width)
         {
            if (bwr >= 8)
            {
               bwr -= 8;
               bwl -= 8;
               b >>= 8;
            }
            else if (bwl - bwr >= width)
            {
               byte r = (byte)((b >> bwr) & mask);
               total -= width;
               bwr += width;
            }
            else if (i < byteCount)
            {
               i += 1;
               b |= (byte)(rawBytes[i] << bwl);
               bwl += 8;
            }
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public static int MaskForBits(int width)
      {
         return (1 << width) - 1;
      }

   }
}
