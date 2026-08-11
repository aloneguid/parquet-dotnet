using System;
using System.Numerics;
using Parquet.Data;
using Parquet.File.Values.Primitives;

namespace Parquet.Encodings;

static class StatsCompute {
    public static void Compute<T>(ReadOnlySpan<T> data, DataColumnStatistics? stats) where T : struct, INumber<T> {
        if(stats == null)
            return;

        MinMax<T>(data, out T min, out T max);
        stats.MinValue = min;
        stats.MaxValue = max;

    }

    public static void Compute(ReadOnlySpan<string> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        // Pairwise min/max: ~1.5 comparisons per element instead of ~2.
        // Compare pairs against each other first, then compare the smaller
        // against min and the larger against max.
        string min = data[0];
        string max = min;
        int i = 1;
        int len = data.Length;

        for(; i + 1 < len; i += 2) {
            string a = data[i];
            string b = data[i + 1];

            // 1 comparison to order the pair
            if(a.AsSpan().SequenceCompareTo(b.AsSpan()) <= 0) {
                // a <= b: a is candidate for min, b for max
                if(a.AsSpan().SequenceCompareTo(min.AsSpan()) < 0)
                    min = a;
                if(b.AsSpan().SequenceCompareTo(max.AsSpan()) > 0)
                    max = b;
            } else {
                // b < a: b is candidate for min, a for max
                if(b.AsSpan().SequenceCompareTo(min.AsSpan()) < 0)
                    min = b;
                if(a.AsSpan().SequenceCompareTo(max.AsSpan()) > 0)
                    max = a;
            }
        }

        // handle the odd trailing element
        if(i < len) {
            string last = data[i];
            if(last.AsSpan().SequenceCompareTo(min.AsSpan()) < 0)
                min = last;
            else if(last.AsSpan().SequenceCompareTo(max.AsSpan()) > 0)
                max = last;
        }

        stats.MinValue = min;
        stats.MaxValue = max;
    }

    public static void Compute(ReadOnlySpan<ReadOnlyMemory<char>> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        // Pairwise min/max: ~1.5 comparisons per element instead of ~2.
        ReadOnlyMemory<char> min = data[0];
        ReadOnlyMemory<char> max = min;
        int i = 1;
        int len = data.Length;

        for(; i + 1 < len; i += 2) {
            ReadOnlyMemory<char> a = data[i];
            ReadOnlyMemory<char> b = data[i + 1];

            if(a.Span.SequenceCompareTo(b.Span) <= 0) {
                if(a.Span.SequenceCompareTo(min.Span) < 0)
                    min = a;
                if(b.Span.SequenceCompareTo(max.Span) > 0)
                    max = b;
            } else {
                if(b.Span.SequenceCompareTo(min.Span) < 0)
                    min = b;
                if(a.Span.SequenceCompareTo(max.Span) > 0)
                    max = a;
            }
        }

        if(i < len) {
            ReadOnlyMemory<char> last = data[i];
            if(last.Span.SequenceCompareTo(min.Span) < 0)
                min = last;
            else if(last.Span.SequenceCompareTo(max.Span) > 0)
                max = last;
        }

        stats.MinValue = min.ToString();
        stats.MaxValue = max.ToString();
    }

    public static void Compute(ReadOnlySpan<BigDecimal> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        BigInteger min = data[0].UnscaledValue;
        BigInteger max = min;
        int minIdx = 0;
        int maxIdx = 0;

        for(int i = 1; i < data.Length; i++) {
            BigInteger v = data[i].UnscaledValue;
            if(v < min) {
                min = v;
                minIdx = i;
            }
            if(v > max) {
                max = v;
                maxIdx = i;
            }
        }

        stats.MinValue = data[minIdx];
        stats.MaxValue = data[maxIdx];
    }

    public static void Compute(ReadOnlySpan<DateTime> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        long min = data[0].Ticks;
        long max = min;
        int minIdx = 0;
        int maxIdx = 0;

        for(int i = 1; i < data.Length; i++) {
            long v = data[i].Ticks;
            if(v < min) { min = v; minIdx = i; }
            if(v > max) { max = v; maxIdx = i; }
        }

        stats.MinValue = data[minIdx];
        stats.MaxValue = data[maxIdx];
    }

    public static void Compute(ReadOnlySpan<DateOnly> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        int min = data[0].DayNumber;
        int max = min;
        int minIdx = 0;
        int maxIdx = 0;

        for(int i = 1; i < data.Length; i++) {
            int v = data[i].DayNumber;
            if(v < min) { min = v; minIdx = i; }
            if(v > max) { max = v; maxIdx = i; }
        }

        stats.MinValue = data[minIdx];
        stats.MaxValue = data[maxIdx];
    }

    public static void Compute(ReadOnlySpan<TimeOnly> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        long min = data[0].Ticks;
        long max = min;
        int minIdx = 0;
        int maxIdx = 0;

        for(int i = 1; i < data.Length; i++) {
            long v = data[i].Ticks;
            if(v < min) { min = v; minIdx = i; }
            if(v > max) { max = v; maxIdx = i; }
        }

        stats.MinValue = data[minIdx];
        stats.MaxValue = data[maxIdx];
    }

    public static void Compute(ReadOnlySpan<TimeSpan> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        long min = data[0].Ticks;
        long max = min;
        int minIdx = 0;
        int maxIdx = 0;

        for(int i = 1; i < data.Length; i++) {
            long v = data[i].Ticks;
            if(v < min) { min = v; minIdx = i; }
            if(v > max) { max = v; maxIdx = i; }
        }

        stats.MinValue = data[minIdx];
        stats.MaxValue = data[maxIdx];
    }

    public static void Compute(ReadOnlySpan<Interval> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        int minIdx = 0;
        int maxIdx = 0;

        for(int i = 1; i < data.Length; i++) {
            ref readonly Interval cur = ref data[i];
            ref readonly Interval mn = ref data[minIdx];
            ref readonly Interval mx = ref data[maxIdx];

            if(cur.Months < mn.Months ||
               (cur.Months == mn.Months && cur.Days < mn.Days) ||
               (cur.Months == mn.Months && cur.Days == mn.Days && cur.Millis < mn.Millis))
                minIdx = i;

            if(cur.Months > mx.Months ||
               (cur.Months == mx.Months && cur.Days > mx.Days) ||
               (cur.Months == mx.Months && cur.Days == mx.Days && cur.Millis > mx.Millis))
                maxIdx = i;
        }

        stats.MinValue = data[minIdx];
        stats.MaxValue = data[maxIdx];
    }

    public static void Compute(ReadOnlySpan<Guid> data, DataColumnStatistics? stats) {
        if(stats == null)
            return;

        if(data.IsEmpty) {
            stats.MinValue = null;
            stats.MaxValue = null;
            return;
        }

        int minIdx = 0;
        int maxIdx = 0;

        for(int i = 1; i < data.Length; i++) {
            if(data[i].CompareTo(data[minIdx]) < 0)
                minIdx = i;
            if(data[i].CompareTo(data[maxIdx]) > 0)
                maxIdx = i;
        }

        stats.MinValue = data[minIdx];
        stats.MaxValue = data[maxIdx];
    }

    private static void MinMax<T>(ReadOnlySpan<T> span, out T min, out T max) where T : struct, INumber<T> {
        min = span.IsEmpty ? default(T) : span[0];
        max = min;
        foreach(T i in span) {
            if(i < min)
                min = i;
            if(i > max)
                max = i;
        }
    }

}
