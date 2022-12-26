using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.File.Values.Primitives;

namespace Parquet.File.Values
{
   //todo: this abstrtion is not useful and must die - create RLE encoding class instead for both reading and writing
   class RunLengthBitPackingHybridValuesReader
   {
      public static int Read(BinaryReader reader, int bitWidth, int[] dest, int destOffset, int maxReadCount)
      {
         int length = GetRemainingLength(reader);
         return ReadRleBitpackedHybrid(reader, bitWidth, length, dest, destOffset, maxReadCount);
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

      public static int ReadRleBitpackedHybrid(BinaryReader reader, int bitWidth, int length, int[] dest, int offset, int pageSize)
      {
         if (length == 0) length = reader.ReadInt32();

         long start = reader.BaseStream.Position;
         int startOffset = offset;
         while ((reader.BaseStream.Position - start < length))
         {
            int header = reader.ReadUnsignedVarInt();
            bool isRle = (header & 1) == 0;

            if (isRle)
            {
               offset += ReadRle(header, reader, bitWidth, dest, offset, pageSize - (offset - startOffset));
            }
            else
            {
               offset += ReadBitpacked(header, reader, bitWidth, dest, offset, pageSize - (offset - startOffset));
            }
         }

         return offset - startOffset;
      }


      /// <summary>
      /// Read run-length encoded run from the given header and bit length.
      /// </summary>
      private static void ReadRle(int header, BinaryReader reader, int bitWidth, List<int> destination)
      {
         // The count is determined from the header and the width is used to grab the
         // value that's repeated. Yields the value repeated count times.

         int count = header >> 1;
         if (count == 0) return; //important not to continue reading as will result in data corruption in data page further
         int width = (bitWidth + 7) / 8; //round up to next byte
         byte[] data = reader.ReadBytes(width);
         int value = ValuesUtils.ReadIntOnBytes(data);
         destination.AddRange(Enumerable.Repeat(value, count));
      }

      private static int ReadRle(int header, BinaryReader reader, int bitWidth, int[] dest, int offset, int maxItems)
      {
         // The count is determined from the header and the width is used to grab the
         // value that's repeated. Yields the value repeated count times.

         int start = offset;
         int headerCount = header >> 1;
         if (headerCount == 0) return 0; // important not to continue reading as will result in data corruption in data page further
         int count = Math.Min(headerCount, maxItems); // make sure we remain within bounds
         int width = (bitWidth + 7) / 8; // round up to next byte
         byte[] data = reader.ReadBytes(width);
         int value = ValuesUtils.ReadIntOnBytes(data);

         for (int i = 0; i < count; i++)
         {
            dest[offset++] = value;
         }

         return offset - start;
      }

      private static int ReadBitpacked(int header, BinaryReader reader, int bitWidth, int[] dest, int offset, int maxItems)
      {
         int start = offset;
         int groupCount = header >> 1;
         int count = groupCount * 8;
         int byteCount = (bitWidth * count) / 8;
         //int byteCount2 = (int)Math.Ceiling(bitWidth * count / 8.0);

         byte[] rawBytes = reader.ReadBytes(byteCount);
         byteCount = rawBytes.Length;  //sometimes there will be less data available, typically on the last page

         int mask = MaskForBits(bitWidth);

         int i = 0;
         uint b = rawBytes[i];
         int total = byteCount * 8;
         int bwl = 8;
         int bwr = 0;
         while (total >= bitWidth && (offset - start) < maxItems)
         {
            if (bwr >= 8)
            {
               bwr -= 8;
               bwl -= 8;
               b >>= 8;
            }
            else if (bwl - bwr >= bitWidth)
            {
               int r = (int)((b >> bwr) & mask);
               total -= bitWidth;
               bwr += bitWidth;

               dest[offset++] = r;
            }
            else if (i + 1 < byteCount)
            {
               i += 1;
               b |= (uint)(rawBytes[i] << bwl);
               bwl += 8;
            }
         }

         return offset - start;
      }

      private static int MaskForBits(int width)
      {
         return (1 << width) - 1;
      }

      private static int GetRemainingLength(BinaryReader reader)
      {
         return (int)(reader.BaseStream.Length - reader.BaseStream.Position);
      }
   }
}
