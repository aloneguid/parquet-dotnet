using System;
using System.IO;

namespace Parquet.File.Values.Primitives
{
   class NanoTime
   {
      private readonly int _julianDay;
      private readonly long _timeOfDayNanos;

      public NanoTime(byte[] data, int offset)
      {
         _timeOfDayNanos = BitConverter.ToInt64(data, offset);
         _julianDay = BitConverter.ToInt32(data, offset + 8);
      }

      public NanoTime(DateTimeOffset dt)
      {
         int m = dt.Month;
         int d = dt.Day;
         int y = dt.Year;

         if (m < 3)
         {
            m = m + 12;
            y = y - 1;
         }

         _julianDay = d + (153 * m - 457) / 5 + 365 * y + (y / 4) - (y / 100) + (y / 400) + 1721119;
         _timeOfDayNanos = (long)(dt.TimeOfDay.TotalMilliseconds * 1000000D);
      }

      public void Write(BinaryWriter writer)
      {
         writer.Write(_timeOfDayNanos);
         writer.Write(_julianDay);
      }

      public static implicit operator DateTimeOffset(NanoTime nanoTime)
      {
         long L = nanoTime._julianDay + 68569;
         long N = (long)((4 * L) / 146097);
         L = L - ((long)((146097 * N + 3) / 4));
         long I = (long)((4000 * (L + 1) / 1461001));
         L = L - (long)((1461 * I) / 4) + 31;
         long J = (long)((80 * L) / 2447);
         int Day = (int)(L - (long)((2447 * J) / 80));
         L = (long)(J / 11);
         int Month = (int)(J + 2 - 12 * L);
         int Year = (int)(100 * (N - 49) + I + L);

         double ms = nanoTime._timeOfDayNanos / 1000000D;

         var result = new DateTimeOffset(Year, Month, Day,
            0, 0, 0,
            TimeSpan.Zero);
         result = result.AddMilliseconds(ms);

         return result;
      }
   }
}
