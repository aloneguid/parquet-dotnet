using System.Numerics;

namespace System {
    static class SpanExtensions {
        public static int ReadInt32(this Span<byte> span) {
            if(BitConverter.IsLittleEndian)
                return (int)span[0] | ((int)span[1] << 8) | ((int)span[2] << 16) | ((int)span[3] << 24);
            return ((int)span[0] << 24) | ((int)span[1] << 16) | ((int)span[2] << 8) | (int)span[3];
        }

        // All of these could be replaced with generic math, but we don't have access to it due to supporting older than .NET 6

        public static void MinMax(this ReadOnlySpan<byte> span, out byte min, out byte max) {
            min = span.IsEmpty ? default(byte) : span[0];
            max = min;
            foreach(byte i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<sbyte> span, out sbyte min, out sbyte max) {
            min = span.IsEmpty ? default(sbyte) : span[0];
            max = min;
            foreach(sbyte i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<short> span, out short min, out short max) {
            min = span.IsEmpty ? default(short) : span[0];
            max = min;
            foreach(short i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<ushort> span, out ushort min, out ushort max) {
            min = span.IsEmpty ? default(ushort) : span[0];
            max = min;
            foreach(ushort i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<int> span, out int min, out int max) {
            min = span.IsEmpty ? default(int) : span[0];
            max = min;
            foreach(int i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<uint> span, out uint min, out uint max) {
            min = span.IsEmpty ? default(uint) : span[0];
            max = min;
            foreach(uint i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<long> span, out long min, out long max) {
            min = span.IsEmpty ? default(long) : span[0];
            max = min;
            foreach(long i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<ulong> span, out ulong min, out ulong max) {
            min = span.IsEmpty ? default(ulong) : span[0];
            max = min;
            foreach(ulong i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<BigInteger> span, out BigInteger min, out BigInteger max) {
            min = span.IsEmpty ? default(BigInteger) : span[0];
            max = min;
            foreach(BigInteger i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<decimal> span, out decimal min, out decimal max) {
            min = span.IsEmpty ? default(decimal) : span[0];
            max = min;
            foreach(decimal i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<double> span, out double min, out double max) {
            min = span.IsEmpty ? default(double) : span[0];
            max = min;
            foreach(double i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<float> span, out float min, out float max) {
            min = span.IsEmpty ? default(float) : span[0];
            max = min;
            foreach(float i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<DateTime> span, out DateTime min, out DateTime max) {
            min = span.IsEmpty ? default(DateTime) : span[0];
            max = min;
            foreach(DateTime i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<DateTimeOffset> span, out DateTimeOffset min, out DateTimeOffset max) {
            min = span.IsEmpty ? default(DateTimeOffset) : span[0];
            max = min;
            foreach(DateTimeOffset i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<TimeSpan> span, out TimeSpan min, out TimeSpan max) {
            min = span.IsEmpty ? default(TimeSpan) : span[0];
            max = min;
            foreach(TimeSpan i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }

        public static void MinMax(this ReadOnlySpan<string> span, out string min, out string max) {
            min = span.IsEmpty ? default(string) : span[0];
            max = min;
            foreach(string s in span) {
                int cmp = s.CompareTo(min);
                if(cmp < 0)
                    min = s;
                else if(cmp > 0)
                    max = s;
            }
        }
    }
}