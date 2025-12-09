using System;
using System.IO;
using System.Text;

namespace Parquet.Test.Util; 

static class Extensions {
    /// <summary>
    /// Converts to MemoryStream with a specific encoding
    /// </summary>
    public static MemoryStream? ToMemoryStream(this string? s, Encoding? encoding) {
        if(s == null)
            return null;
        if(encoding == null)
            encoding = Encoding.UTF8;

        return new MemoryStream(encoding.GetBytes(s));
    }

    /// <summary>
    /// Converts to MemoryStream in UTF-8 encoding
    /// </summary>
    public static MemoryStream? ToMemoryStream(this string? s) {
        // ReSharper disable once IntroduceOptionalParameters.Global
        return ToMemoryStream(s, null);
    }

    /// <summary>
    /// Strips time from the date structure
    /// </summary>
    public static DateTime RoundToDay(this DateTime time) {
        return new DateTime(time.Year, time.Month, time.Day);
    }

    /// <summary>
    /// Strips off details after seconds
    /// </summary>
    /// <param name="time"></param>
    /// <returns></returns>
    public static DateTime RoundToSecond(this DateTime time) {
        return new DateTime(time.Year, time.Month, time.Day, time.Hour, time.Minute, time.Second, time.Kind);
    }

    /// <summary>
    /// Strips off details after milliseconds
    /// </summary>
    /// <param name="time"></param>
    /// <returns></returns>
    public static DateTime RoundToMillisecond(this DateTime time) {
        return new DateTime(time.Year, time.Month, time.Day, time.Hour, time.Minute, time.Second, time.Millisecond, time.Kind);
    }

#if NET7_0_OR_GREATER
    /// <summary>
    /// Strips off details after microseconds
    /// </summary>
    /// <param name="time"></param>
    /// <returns></returns>
    public static DateTime RoundToMicrosecond(this DateTime time) {
        return new DateTime(time.Year, time.Month, time.Day, time.Hour, time.Minute, time.Second, time.Millisecond, time.Microsecond, time.Kind);
    }
#endif
}
