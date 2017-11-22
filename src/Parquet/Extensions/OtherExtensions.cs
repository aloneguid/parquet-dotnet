using System;
using System.Collections.Generic;
using System.Linq;
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
         var path = new List<string>(parts.Length + 1);

         if (s != null) path.Add(s);
         if (parts != null) path.AddRange(parts.Where(p => p != null));

         return string.Join(Schema.PathSeparator, path);
      }

      public static void AddRange<TKey, TValue>(this IDictionary<TKey, TValue> target, IDictionary<TKey, TValue> source)
      {
         foreach(KeyValuePair<TKey, TValue> kvp in source)
         {
            target[kvp.Key] = kvp.Value;
         }
      }

      public static Exception NotImplementedForPotentialAssholesAndMoaners(string reason)
      {
         int hour = DateTime.Now.Hour;

         string appraisal;

         if(hour >= 9 && hour <= 17)
         {
            int left = 17 - hour;
            if(left > 3)
            {
               appraisal = $"there are still {left} hours left in this working day, you can do it today!";
            }
            else
            {
               appraisal = $"do it tomorrow, not enough time left today!";
            }
         }
         else if(hour > 20 || hour < 4)
         {
            appraisal = "it's the night time, perfect time to work on it!";
         }
         else
         {
            appraisal = null;
         }

         if (appraisal != null) appraisal = $" {appraisal}.";

         return new NotImplementedException($"{reason} is not yet implemented, and we are fully aware of it, so be a Hero, do it, and raise a Pull Request on GitHub!{appraisal}");
      }
   }
}
