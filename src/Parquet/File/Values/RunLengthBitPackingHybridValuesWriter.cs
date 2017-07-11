using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using System.Runtime.CompilerServices;

namespace Parquet.File.Values
{
   class RunLengthBitPackingHybridValuesWriter : IValuesWriter
   {
      public void Write(BinaryWriter writer, SchemaElement schema, IList data)
      {
         //int32 - length of data (we'll come back here so let's just write a zero)
         long dataLengthOffset = writer.BaseStream.Position;
         writer.Write((int)0);

         //write actual data
         WriteData(writer, (List<int>)data);

         //come back to write data length
         long dataLength = writer.BaseStream.Position - dataLengthOffset - sizeof(int);
         writer.BaseStream.Seek(dataLengthOffset, SeekOrigin.Begin);
         writer.Write((int)dataLength);

         //and jump back to the end again
         writer.BaseStream.Seek(0, SeekOrigin.End);
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void WriteData(BinaryWriter writer, List<int> data)
      {
         //for simplicity, we're only going to write RLE, however bitpacking needs to be implemented as well

         const int maxCount = 0b0111_1111_1111_1111;  //max count for an integer with one lost bit

         //chunk identical values and write
         int lastValue = 0;
         int chunkCount = 0;
         foreach (int item in data)
         {
            if(chunkCount == 0)
            {
               chunkCount = 1;
               lastValue = item;
            }
            else
            {
               if(item != lastValue || chunkCount == maxCount)
               {
                  WriteRle(writer, chunkCount, lastValue);

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
            WriteRle(writer, chunkCount, lastValue);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void WriteRle(BinaryWriter writer, int chunkCount, int value)
      {
         int header = 0x0; // the last bit for RLE is 0
         header = chunkCount << 1;
         const int bitWidth = 1; //todo: set to 1 as we only write booleans, however this won't work if you try doing anything else
         int byteWidth = (bitWidth + 7) / 8; //number of whole bytes for this bit width

         WriteUnsignedVarInt(writer, header);
         WriteIntBytes(writer, value, byteWidth);
      }

      private void WriteBitpacked()
      {
         int header = 0x1;

         //todo: implement this
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private void WriteIntBytes(BinaryWriter writer, int value, int byteWidth)
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

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
