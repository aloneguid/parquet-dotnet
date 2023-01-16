using System;
using System.IO;

namespace Parquet.File.Values.Primitives {
    class NanoTime {
        private readonly int _julianDay;
        private readonly long _timeOfDayNanos;
        public const int BinarySize = 12;

        public NanoTime(byte[] data, int offset) {
            _timeOfDayNanos = BitConverter.ToInt64(data, offset);
            _julianDay = BitConverter.ToInt32(data, offset + 8);
        }

        public NanoTime(Span<byte> span) {
#if NETSTANDARD2_0
            byte[] data = span.ToArray();
            _timeOfDayNanos = BitConverter.ToInt64(data, 0);
            _julianDay = BitConverter.ToInt32(data, sizeof(long));
#else
            _timeOfDayNanos = BitConverter.ToInt64(span.Slice(0, sizeof(long)));
            _julianDay = BitConverter.ToInt32(span.Slice(sizeof(long)));
#endif
        }

        public NanoTime(DateTime dt) {
            dt = dt.ToUniversalTime();
            int m = dt.Month;
            int d = dt.Day;
            int y = dt.Year;

            if(m < 3) {
                m = m + 12;
                y = y - 1;
            }

            _julianDay = d + (((153 * m) - 457) / 5) + (365 * y) + (y / 4) - (y / 100) + (y / 400) + 1721119;
            _timeOfDayNanos = dt.TimeOfDay.Ticks * 100;
        }

        public void Write(BinaryWriter writer) {
            writer.Write(_timeOfDayNanos);
            writer.Write(_julianDay);
        }

        public void Write(Stream s) {
            byte[] b1 = BitConverter.GetBytes(_timeOfDayNanos);
            byte[] b2 = BitConverter.GetBytes(_julianDay);
            s.Write(b1, 0, b1.Length);
            s.Write(b2, 0, b2.Length);
        }

        public byte[] GetBytes() {
            byte[] b1 = BitConverter.GetBytes(_timeOfDayNanos);
            byte[] b2 = BitConverter.GetBytes(_julianDay);
            byte[] r = new byte[b1.Length+ b2.Length];
            b1.CopyTo(r, 0);
            b2.CopyTo(r, b1.Length);
            return r;
        }

        public static implicit operator DateTime(NanoTime nanoTime) {
            long L = nanoTime._julianDay + 68569;
            long N = (long)(4 * L / 146097);
            L = L - ((long)(((146097 * N) + 3) / 4));
            long I = (long)((4000 * (L + 1) / 1461001));
            L = L - (long)(1461 * I / 4) + 31;
            long J = (long)(80 * L / 2447);
            int Day = (int)(L - (long)(2447 * J / 80));
            L = (long)(J / 11);
            int Month = (int)(J + 2 - (12 * L));
            int Year = (int)((100 * (N - 49)) + I + L);
            // DateTimeOffset.MinValue.Year is 1
            Year = Math.Max(Year, 1);

            long timeOfDayTicks = nanoTime._timeOfDayNanos / 100;

            var result = new DateTime(Year, Month, Day,
               0, 0, 0, DateTimeKind.Utc);
            result = result.AddTicks(timeOfDayTicks);

            return result;
        }
    }
}