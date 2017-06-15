using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using TType = Parquet.Thrift.Type;

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

      public static List<int> ReadRleBitpackedHybrid(BinaryReader reader, int bitWidth, int length)
      {
         if (length == 0) length = reader.ReadInt32();

         //todo: use length to read continuously up to "length" bytes
         //there might be one page after another in RLE/Bitpacking following each other
         //all the code below is repeatable

         int header = ReadUnsignedVarInt(reader);
         bool isRle = (header & 1) == 0;

         if (isRle)
            return ReadRle(header, reader, bitWidth);
         else
            return ReadBitpacked(header, reader, bitWidth);
      }

      public static ICollection ReadPlain(BinaryReader reader, TType thriftType)
      {
         long byteCount = reader.BaseStream.Length - reader.BaseStream.Position;
         byte[] data = reader.ReadBytes((int)byteCount);

         switch (thriftType)
         {
            case TType.BOOLEAN:
               //todo: avoid using BitArray as this requires creating a new class every time we read data
               return ReadPlainBoolean(data, 8);
            case TType.INT32:
               var r32 = new List<int>(data.Length / 4);
               for(int i = 0; i < data.Length; i += 4)
               {
                  int iv = BitConverter.ToInt32(data, i);
                  r32.Add(iv);
               }
               return r32;
            case TType.INT96:
               //todo: this is a sample how to read int96, not tested this yet
               var r96 = new List<BigInteger>(data.Length / 12);
               byte[] v96 = new byte[12];
               for(int i = 0; i < data.Length; i+= 12)
               {
                  Array.Copy(data, i, v96, 0, 12);
                  var bi = new BigInteger(v96);
               }
               return r96;
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
      public static List<int> ReadRle(int header, BinaryReader reader, int bitWidth)
      {
         // The count is determined from the header and the width is used to grab the
         // value that's repeated. Yields the value repeated count times.

         int count = header >> 1;
         int width = bitWidth + 7 / 8; //round up to next byte
         byte[] data = reader.ReadBytes(width);
         if (data.Length > 1) throw new NotImplementedException();
         int value = (int)data[0];
         return Enumerable.Repeat(value, count).ToList();   //todo: not sure how fast it is
      }

      public static List<int> ReadBitpacked(int header, BinaryReader reader, int bitWidth)
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
         var result = new List<int>(total);
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
               result.Add(r);
            }
            else if (i + 1 < byteCount)
            {
               i += 1;
               b |= (rawBytes[i] << bwl);
               bwl += 8;
            }
         }
         return result;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public static int MaskForBits(int width)
      {
         return (1 << width) - 1;
      }

      public static int GetWidthFromMaxInt(int value)
      {
         for(int i = 0; i < 64; i++)
         {
            if (value == 0) return i;
            value >>= 1;
         }

         return 1;
      }

   }
}
