using System;
using Parquet.Data;

namespace Parquet
{
   static class OtherExtensions
   {
      private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

      public static int GetBitWidth(this int value)
      {
         for (int i = 0; i < 64; i++)
         {
            if (value == 0) return i;
            value >>= 1;
         }

         return 1;
      }

      public static DateTime FromUnixTime(this int unixTime)
      {
         return UnixEpoch.AddDays(unixTime - 1);
      }

      public static DateTime FromUnixTime(this long unixTime)
      {
         return UnixEpoch.AddSeconds(unixTime);
      }

      public static long ToUnixTime(this DateTimeOffset date)
      {
         return Convert.ToInt64((date - UnixEpoch).TotalSeconds);
      }

      public static double ToUnixDays(this DateTimeOffset dto)
      {
         TimeSpan diff = dto - UnixEpoch;
         return diff.TotalDays;
      }

      public static long GetUnixUnixTimeDays(this DateTime date)
      {
         return Convert.ToInt64((date - UnixEpoch).TotalDays);
      }

      public static string AddPath(this string s, params string[] parts)
      {
         foreach(string part in parts)
         {
            s += Schema.PathSeparator;
            s += part;
         }

         return s;
      }
   }
}
