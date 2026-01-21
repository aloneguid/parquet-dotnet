using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Parquet.Test.Util; 
static class RandomGenerator {
    private static readonly RandomNumberGenerator Rnd = RandomNumberGenerator.Create();

    //get a cryptographically strong double between 0 and 1
    private static double NextCryptoDouble() {
        //fill-in array with 8 random  bytes
        byte[] b = new byte[sizeof(double)];
        Rnd.GetBytes(b);

        //i don't understand this
        ulong ul = BitConverter.ToUInt64(b, 0) / (1 << 11);
        double d = ul / (double)(1UL << 53);
        return d;
    }

    private static int NextCryptoInt() {
        byte[] b = new byte[sizeof(int)];
        Rnd.GetBytes(b);
        return BitConverter.ToInt32(b, 0);
    }

    /// <summary>
    /// Generates a random boolean
    /// </summary>
    public static bool RandomBool {
        get {
            return NextCryptoDouble() >= 0.5d;
        }
    }

    /// <summary>
    /// Generates a random long number between 0 and max
    /// </summary>
    public static long RandomLong => GetRandomLong(0, long.MaxValue);

    /// <summary>
    /// Generates a random integer between 0 and max
    /// </summary>
    public static int RandomInt {
        get {
            return NextCryptoInt();
        }
    }

    /// <summary>
    /// Returns random double
    /// </summary>
    public static double RandomDouble {
        get {
            return NextCryptoDouble();
        }
    }

    /// <summary>
    /// Generates a random integer until max parameter
    /// </summary>
    /// <param name="max">Maximum integer value, excluding</param>
    /// <returns></returns>
    public static int GetRandomInt(int max) {
        return GetRandomInt(0, max);
    }

    /// <summary>
    /// Generates a random integer number in range
    /// </summary>
    /// <param name="min">Minimum value, including</param>
    /// <param name="max">Maximum value, excluding</param>
    public static int GetRandomInt(int min, int max) {
        return (int)Math.Round(NextCryptoDouble() * (max - min - 1)) + min;
    }

    /// <summary>
    /// Generates a random long number in range
    /// </summary>
    /// <param name="min">Minimum value, including</param>
    /// <param name="max">Maximum value, excluding</param>
    public static long GetRandomLong(long min, long max) {
        double d = NextCryptoDouble();
        return (long)Math.Round(d * (max - min - 1)) + min;
    }

    /// <summary>
    /// Generates a random enum value by type
    /// </summary>
    public static Enum? RandomEnum(Type t) {
        Array values = Enum.GetValues(t);

        object? value = values.GetValue(GetRandomInt(values.Length));

        return value == null ? default : (Enum)value;
    }

#if !NETSTANDARD16
    /// <summary>
    /// Generates a random enum value
    /// </summary>
    /// <typeparam name="T">Enumeration type</typeparam>
    public static T? GetRandomEnum<T>() where T : struct {
        //can't limit generics to enum http://connect.microsoft.com/VisualStudio/feedback/details/386194/allow-enum-as-generic-constraint-in-c

        if(!typeof(T).IsEnum)
            throw new ArgumentException("T must be an enum");

        return (T?)(object?)RandomEnum(typeof(T));
    }
#endif

    /// <summary>
    /// Generates a random date in range
    /// </summary>
    /// <param name="minValue">Minimum date, including</param>
    /// <param name="maxValue">Maximum date, excluding</param>
    public static DateTime GetRandomDate(DateTime minValue, DateTime maxValue) {
        long randomTicks = GetRandomLong(minValue.Ticks, maxValue.Ticks);

        return new DateTime(randomTicks);
    }

    /// <summary>
    /// Generates a random date value
    /// </summary>
    public static DateTime RandomDate {
        get { return GetRandomDate(DateTime.MinValue, DateTime.MaxValue); }
    }

    /// <summary>
    /// Generates a random string. Never returns null.
    /// </summary>
    public static string RandomString {
        get {
            string path = Path.GetRandomFileName();
            path = path.Replace(".", "");
            return path;
        }
    }

    /// <summary>
    /// Generates a random string
    /// </summary>
    /// <param name="length">string length</param>
    /// <param name="allowNulls">Whether to allow to return null values</param>
    public static string? GetRandomString(int length, bool allowNulls) {
        if(allowNulls && RandomLong % 2 == 0)
            return null;

        var builder = new StringBuilder();
        char ch;
        for(int i = 0; i < length; i++) {
            ch = Convert.ToChar(Convert.ToInt32(Math.Floor((26 * RandomDouble) + 65)));
            builder.Append(ch);
        }

        return builder.ToString();
    }

    /// <summary>
    /// Generates a random URL in format "http://random.com/random.random
    /// </summary>
    /// <param name="allowNulls">Whether to allow to return nulls</param>
    public static Uri? GetRandomUri(bool allowNulls) {
        if(allowNulls && RandomLong % 2 == 0)
            return null;

        return new Uri($"http://{RandomString}.com/{RandomString}.{GetRandomString(3, false)}");
    }

    /// <summary>
    /// Generates a random URL in format "http://random.com/random.random. Never returns null values.
    /// </summary>
    public static Uri? RandomUri => GetRandomUri(false);

    /// <summary>
    /// Generates a random sequence of bytes of a specified size
    /// </summary>
    public static byte[] GetRandomBytes(int minSize, int maxSize) {
        int size = minSize == maxSize ? minSize : GetRandomInt(minSize, maxSize);
        byte[] data = new byte[size];
        Rnd.GetBytes(data);
        return data;
    }
}
