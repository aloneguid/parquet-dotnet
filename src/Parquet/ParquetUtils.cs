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
   }
}
