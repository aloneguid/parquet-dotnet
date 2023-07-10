﻿using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Schema;

namespace Parquet {
    static class OtherExtensions {
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static int GetBitWidth(this int value) {
            for(int i = 0; i < 64; i++) {
                if(value == 0)
                    return i;
                value >>= 1;
            }

            return 1;
        }

        public static DateTimeOffset FromUnixMilliseconds(this long unixMilliseconds) {
            return UnixEpoch.AddMilliseconds(unixMilliseconds);
        }

        public static DateTime AsUnixMillisecondsInDateTime(this long unixMilliseconds) {
            return UnixEpoch.AddMilliseconds(unixMilliseconds);
        }

        public static long ToUnixMilliseconds(this DateTime dto) {
            TimeSpan diff = dto - UnixEpoch;
            return (long)diff.TotalMilliseconds;
        }

        public static DateTime AsUnixDaysInDateTime(this int unixDays) {
            return UnixEpoch.AddDays(unixDays);
        }

        public static int ToUnixDays(this DateTime dto) {
            TimeSpan diff = dto - UnixEpoch;
            return (int)diff.TotalDays;
        }

#if NET6_0_OR_GREATER
        public static int ToUnixDays(this DateOnly dto) {
            TimeSpan diff = new DateTime(dto.Year, dto.Month, dto.Day) - UnixEpoch;
            return (int)diff.TotalDays;
        }
#endif

        public static DateTime ToUtc(this DateTime dto) =>
            dto.Kind == DateTimeKind.Unspecified
                ? DateTime.SpecifyKind(dto, DateTimeKind.Utc)
                : dto.ToUniversalTime();

        public static string AddPath(this string s, params string[] parts) {
            var path = new List<string>(parts.Length + 1);

            if(s != null)
                path.Add(s);
            if(parts != null)
                path.AddRange(parts.Where(p => p != null));

            return string.Join(ParquetSchema.PathSeparator, path);
        }

        public static bool EqualTo(this Array left, Array right) {
            if(left.Length != right.Length)
                return false;

            for(int i = 0; i < left.Length; i++) {
                object? il = left.GetValue(i);
                object? ir = right.GetValue(i);

                if(il == null || ir == null) {
                    return il == null && ir == null;
                }

                if(!il.Equals(ir))
                    return false;
            }

            return true;
        }

        public static Exception NotImplemented(string reason) {
            return new NotImplementedException($"{reason} is not yet implemented, and we are fully aware of it. From here you can either raise an issue on GitHub, or implemented it and raise a PR.");
        }
    }
}