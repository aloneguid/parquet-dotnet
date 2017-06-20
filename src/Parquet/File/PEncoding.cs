#define SPARK_TYPES

using Parquet.Thrift;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using TType = Parquet.Thrift.Type;
using TConvertedType = Parquet.Thrift.ConvertedType;

namespace Parquet.File
{
   //todo: performance - inspect that appropriate type is used in bit shifting (i.e. don't use int for 8 bit values)


   /// <summary>
   /// Handles parquet encoding logic.
   /// All methods are made static for increased performance.
   /// </summary>
   static class PEncoding
   {
      private static readonly System.Text.Encoding UTF8 = System.Text.Encoding.UTF8;

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
