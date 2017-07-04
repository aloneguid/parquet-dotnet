/* MIT License
 *
 * Copyright (c) 2017 Elastacloud Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet
{
    static class ParquetUtils
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

       public static DateTime FromUnixTime(this int unixTime)
       {
          var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
          return epoch.AddDays(unixTime);
       }

       public static DateTime FromUnixTime(this long unixTime)
       {
          var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
          return epoch.AddSeconds(unixTime);
       }

      public static long ToUnixTime(this DateTime date)
       {
          var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
          return Convert.ToInt64((date - epoch).TotalSeconds);
       }

       public static long GetUnixUnixTimeDays(this DateTime date)
       {
          var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
          return Convert.ToInt64((date - epoch).TotalDays);
       }

      public static DateTime JulianToDateTime(this int julianDate)
       {
         double unixTime = julianDate - 2440587;
         DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
         dtDateTime = dtDateTime.AddDays(unixTime - 1).ToLocalTime();

          return dtDateTime;
       }

   }

   class NumericUtils
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
