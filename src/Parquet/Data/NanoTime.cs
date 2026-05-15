using System;
using System.Buffers.Binary;
using System.IO;

namespace Parquet.Data;

readonly struct NanoTime {
    private readonly int _julianDay;
    private readonly long _timeOfDayNanos;
    public const int BinarySize = 12;

    public NanoTime(Span<byte> span) {
        if(span.Length != BinarySize)
            throw new ArgumentException($"NanoTime bytes must be {BinarySize} bytes long");

        _timeOfDayNanos = BitConverter.ToInt64(span.Slice(0, sizeof(long)));
        _julianDay = BitConverter.ToInt32(span.Slice(sizeof(long)));
    }

    public NanoTime(DateTime dt) {
        dt = dt.ToUniversalTime();
        int m = dt.Month;
        int d = dt.Day;
        int y = dt.Year;

        if(m < 3) {
            m += 12;
            y -= 1;
        }

        _julianDay = d + (((153 * m) - 457) / 5) + (365 * y) + (y / 4) - (y / 100) + (y / 400) + 1721119;
        _timeOfDayNanos = dt.TimeOfDay.Ticks * 100;
    }

    public void Write(Stream s) {
        Span<byte> buffer = stackalloc byte[sizeof(long) + sizeof(int)];
        BinaryPrimitives.WriteInt64LittleEndian(buffer, _timeOfDayNanos);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[sizeof(long)..], _julianDay);
        s.Write(buffer);
    }

    public byte[] GetBytes() {
        byte[] r = new byte[sizeof(long) + sizeof(int)];
        BinaryPrimitives.WriteInt64LittleEndian(r, _timeOfDayNanos);
        BinaryPrimitives.WriteInt32LittleEndian(r.AsSpan(sizeof(long)), _julianDay);
        return r;
    }
    
    public DateTime ToDateTime() {
        long L = _julianDay + 68569;
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

        long timeOfDayTicks = _timeOfDayNanos / 100;

        var result = new DateTime(Year, Month, Day,
            0, 0, 0, DateTimeKind.Unspecified);
        result = result.AddTicks(timeOfDayTicks);

        return result;
    }
}