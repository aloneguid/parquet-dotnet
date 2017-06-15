using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet
{
    public static class ParquetUtils
    {
       public static bool[] ConvertToBoolArray(this BitArray bits, int bitCount)
       {
          var boolList = new List<bool>();
          for (int index = 0; index < bitCount; index++)
          {
             boolList.Add(bits.Get(index));
          }

          return boolList.ToArray();
       }

       public static Byte GetByte(this BitArray array)
       {
          Byte byt = 0;
          for (int i = array.Length - 1; i >= 0; i--)
             byt = (byte)((byt << 1) | (array[i] ? 1 : 0));
          return byt;
       }

       // ReSharper disable once InconsistentNaming
       public static int GetByteArrayLELength(byte[] len)
       {
          int output = BitConverter.ToInt32(len, 0);
          return output;
       }

       
   }

   public class NumericUtils
   {
      public byte[] IntToLittleEndian(int data)
      {
         byte[] b = new byte[4];
         b[0] = (byte) data;
         b[1] = (byte) (((uint) data >> 8) & 0xFF);
         b[2] = (byte) (((uint) data >> 16) & 0xFF);
         b[3] = (byte) (((uint) data >> 24) & 0xFF);

         return b;
      }


      public byte[] LongToLittleEndian(long data)
      {
         byte[] b = new byte[8];

         for (int i = 0; i < 8; i++)
         {
            b[i] = (byte) (data & 0xFF);
            data >>= 8;
         }

         return b;
      }
      

      public byte[] DoubleToLittleEndian(double data)
      {
         return BitConverter.GetBytes(data);
      }

      public byte[] FloatToLittleEndian(float data)
      {
         return BitConverter.GetBytes(data);
      }
   }
}
