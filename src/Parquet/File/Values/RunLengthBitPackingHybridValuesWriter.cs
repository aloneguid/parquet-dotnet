using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Parquet.File.Values
{
   class RunLengthBitPackingHybridValuesWriter
   {
      /// <summary>
      /// Writes to target stream without jumping around, therefore can be used in forward-only stream
      /// </summary>
      public static void WriteForwardOnly(BinaryWriter writer, int bitWidth, int[] data, int count)
      {
         //write data to a memory buffer, as we need data length to be written before the data
         using (var ms = new MemoryStream())
         {
            using (var bw = new BinaryWriter(ms, Encoding.UTF8, true))
            {
               //write actual data
               WriteData(bw, data, count, bitWidth);
            }

            //int32 - length of data
            writer.Write((int)ms.Length);

            //actual data
            ms.Position = 0;
            ms.CopyTo(writer.BaseStream); //warning! CopyTo performs .Flush internally
         }
      }

      private static void WriteData(BinaryWriter writer, int[] data, int count, int bitWidth)
      {
         //for simplicity, we're only going to write RLE, however bitpacking needs to be implemented as well

         const int maxCount = int.MaxValue >> 1;  //max count for an integer with one lost bit

         //chunk identical values and write
         int lastValue = 0;
         int chunkCount = 0;
         for (int i = 0; i < count; i++)
         {
            int item = data[i];

            if(chunkCount == 0)
            {
               chunkCount = 1;
               lastValue = item;
            }
            else
            {
               if(item != lastValue || chunkCount == maxCount)
               {
                  WriteRle(writer, chunkCount, lastValue, bitWidth);

                  chunkCount = 1;
                  lastValue = item;
               }
               else
               {
                  chunkCount += 1;
               }
            }
         }

         if(chunkCount > 0)
         {
            WriteRle(writer, chunkCount, lastValue, bitWidth);
         }
      }

      private static void WriteRle(BinaryWriter writer, int chunkCount, int value, int bitWidth)
      {
         int header = 0x0; // the last bit for RLE is 0
         header = chunkCount << 1;
         int byteWidth = (bitWidth + 7) / 8; //number of whole bytes for this bit width

         WriteUnsignedVarInt(writer, header);
         WriteIntBytes(writer, value, byteWidth);
      }

      private static void WriteIntBytes(BinaryWriter writer, int value, int byteWidth)
      {
         byte[] dataBytes = BitConverter.GetBytes(value);

         switch(byteWidth)
         {
            case 0:
               break;
            case 1:
               writer.Write(dataBytes[0]);
               break;
            case 2:
               writer.Write(dataBytes[1]);
               writer.Write(dataBytes[0]);
               break;
            case 3:
               writer.Write(dataBytes[2]);
               writer.Write(dataBytes[1]);
               writer.Write(dataBytes[0]);
               break;
            case 4:
               writer.Write(dataBytes);
               break;
            default:
               throw new IOException($"encountered bit width ({byteWidth}) that requires more than 4 bytes.");
         }
      }

      private static void WriteUnsignedVarInt(BinaryWriter writer, int value)
      {
         while(value > 127)
         {
            byte b = (byte)((value & 0x7F) | 0x80);

            writer.Write(b);

            value >>= 7;
         }

         writer.Write((byte)value);
      }
   }
}
