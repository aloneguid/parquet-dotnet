/*
 _   _      _   ____
| \ | | ___| |_| __ )  _____  __
|  \| |/ _ \ __|  _ \ / _ \ \/ /
| |\  |  __/ |_| |_) | (_) >  <
|_| \_|\___|\__|____/ \___/_/\_\   v4.3.0 by @aloneguid

https://github.com/aloneguid/netbox
*/



// FILE: src/NetBox/TempFile.cs

namespace NetBox {
    using global::System;
    using global::System.IO;

    /// <summary>
    /// Represents a temporary file that is deleted on dispose. The files are created in user's temp directory.
    /// </summary>
    class TempFile : IDisposable {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ext">Optional extension, defaults to .tmp</param>
        public TempFile(string? ext = null) {
            if(ext == null)
                ext = ".tmp";

            if(!ext.StartsWith("."))
                ext = "." + ext;

            string name = Guid.NewGuid().ToString() + ext;

            FullPath = Path.Combine(Path.GetTempPath(), name);
        }

        /// <summary>
        /// Full path to the temp file. It's not created by this class.
        /// </summary>
        public string FullPath { get; }

        /// <summary>
        /// Implicit conversion to string (full path).
        /// </summary>
        /// <param name="tf"></param>
        public static implicit operator string(TempFile tf) => tf.FullPath;

        /// <summary>
        /// Returns full path value
        /// </summary>
        /// <returns></returns>
        public override string ToString() => FullPath;

        public void Dispose() {
            if(File.Exists(FullPath)) {
                File.Delete(FullPath);
            }
        }
    }
}

// FILE: src/NetBox/WebUtility.cs

namespace NetBox {
    using global::System;
    using global::System.Text;

    /// <summary>
    /// This class is ported from .NET 4.6 to support URL encoding/decoding functionality which is missing in .NET Standard
    /// </summary>
    static class WebUtility {
        public static string? UrlEncode(string? value) {
            if(value == null)
                return null;

            byte[] bytes = Encoding.UTF8.GetBytes(value);
            byte[]? ue = UrlEncode(bytes, 0, bytes.Length, false);
            if(ue == null)
                return null;
            return Encoding.UTF8.GetString(ue);
        }

        public static string? UrlDecode(string? encodedValue) {
            if(encodedValue == null)
                return null;

            return UrlDecodeInternal(encodedValue, Encoding.UTF8);
        }

        private static byte[]? UrlEncode(byte[]? bytes, int offset, int count, bool alwaysCreateNewReturnValue) {
            byte[]? encoded = UrlEncode(bytes, offset, count);

            return (alwaysCreateNewReturnValue && (encoded != null) && (encoded == bytes))
                ? (byte[])encoded.Clone()
                : encoded;
        }

        private static byte[]? UrlEncode(byte[]? bytes, int offset, int count) {
            if(bytes == null)
                return null;

            if(!ValidateUrlEncodingParameters(bytes, offset, count)) {
                return null;
            }

            int cSpaces = 0;
            int cUnsafe = 0;

            // count them first
            for(int i = 0; i < count; i++) {
                char ch = (char)bytes[offset + i];

                if(ch == ' ')
                    cSpaces++;
                else if(!IsUrlSafeChar(ch))
                    cUnsafe++;
            }

            // nothing to expand?
            if(cSpaces == 0 && cUnsafe == 0) {
                // DevDiv 912606: respect "offset" and "count"
                if(0 == offset && bytes.Length == count) {
                    return bytes;
                } else {
                    byte[] subarray = new byte[count];
                    Buffer.BlockCopy(bytes, offset, subarray, 0, count);
                    return subarray;
                }
            }

            // expand not 'safe' characters into %XX, spaces to +s
            byte[] expandedBytes = new byte[count + (cUnsafe * 2)];
            int pos = 0;

            for(int i = 0; i < count; i++) {
                byte b = bytes[offset + i];
                char ch = (char)b;

                if(IsUrlSafeChar(ch)) {
                    expandedBytes[pos++] = b;
                } else if(ch == ' ') {
                    expandedBytes[pos++] = (byte)'+';
                } else {
                    expandedBytes[pos++] = (byte)'%';
                    expandedBytes[pos++] = (byte)IntToHex((b >> 4) & 0xf);
                    expandedBytes[pos++] = (byte)IntToHex(b & 0x0f);
                }
            }

            return expandedBytes;
        }

        private static bool ValidateUrlEncodingParameters(byte[]? bytes, int offset, int count) {
            if(bytes == null && count == 0)
                return false;
            if(bytes == null) {
                throw new ArgumentNullException("bytes");
            }
            if(offset < 0 || offset > bytes.Length) {
                throw new ArgumentOutOfRangeException("offset");
            }
            if(count < 0 || offset + count > bytes.Length) {
                throw new ArgumentOutOfRangeException("count");
            }

            return true;
        }

        private static bool IsUrlSafeChar(char ch) {
            if((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9'))
                return true;

            switch(ch) {
                case '-':
                case '_':
                case '.':
                case '!':
                case '*':
                case '(':
                case ')':
                    return true;
            }

            return false;
        }

        private static char IntToHex(int n) {
            if(n <= 9)
                return (char)(n + (int)'0');
            else
                return (char)(n - 10 + (int)'A');
        }

        private static string? UrlDecodeInternal(string? value, Encoding encoding) {
            if(value == null) {
                return null;
            }

            int count = value.Length;
            UrlDecoder helper = new UrlDecoder(count, encoding);

            // go through the string's chars collapsing %XX and
            // appending each char as char, with exception of %XX constructs
            // that are appended as bytes

            for(int pos = 0; pos < count; pos++) {
                char ch = value[pos];

                if(ch == '+') {
                    ch = ' ';
                } else if(ch == '%' && pos < count - 2) {
                    int h1 = HexToInt(value[pos + 1]);
                    int h2 = HexToInt(value[pos + 2]);

                    if(h1 >= 0 && h2 >= 0) {     // valid 2 hex chars
                        byte b = (byte)((h1 << 4) | h2);
                        pos += 2;

                        // don't add as char
                        helper.AddByte(b);
                        continue;
                    }
                }

                if((ch & 0xFF80) == 0)
                    helper.AddByte((byte)ch); // 7 bit have to go as bytes because of Unicode
                else
                    helper.AddChar(ch);
            }

            return helper.GetString();
        }

        private class UrlDecoder {
            private readonly int _bufferSize;

            // Accumulate characters in a special array
            private int _numChars;
            private readonly char[] _charBuffer;

            // Accumulate bytes for decoding into characters in a special array
            private int _numBytes;
            private byte[]? _byteBuffer;

            // Encoding to convert chars to bytes
            private readonly Encoding _encoding;

            private void FlushBytes() {
                if(_numBytes > 0 && _byteBuffer != null) {
                    _numChars += _encoding.GetChars(_byteBuffer, 0, _numBytes, _charBuffer, _numChars);
                    _numBytes = 0;
                }
            }

            internal UrlDecoder(int bufferSize, Encoding encoding) {
                _bufferSize = bufferSize;
                _encoding = encoding;

                _charBuffer = new char[bufferSize];
                // byte buffer created on demand
            }

            internal void AddChar(char ch) {
                if(_numBytes > 0)
                    FlushBytes();

                _charBuffer[_numChars++] = ch;
            }

            internal void AddByte(byte b) {
                if(_byteBuffer == null)
                    _byteBuffer = new byte[_bufferSize];

                _byteBuffer[_numBytes++] = b;
            }

            internal string GetString() {
                if(_numBytes > 0)
                    FlushBytes();

                if(_numChars > 0)
                    return new string(_charBuffer, 0, _numChars);
                else
                    return string.Empty;
            }
        }

        private static int HexToInt(char h) {
            return (h >= '0' && h <= '9') ? h - '0' :
            (h >= 'a' && h <= 'f') ? h - 'a' + 10 :
            (h >= 'A' && h <= 'F') ? h - 'A' + 10 :
            -1;
        }
    }
}

// FILE: src/NetBox/Ascii85.cs

namespace NetBox {
    using global::System;
    using global::System.IO;
    using global::System.Text;

    /// <summary>
    /// C# implementation of ASCII85 encoding. 
    /// Based on C code from http://www.stillhq.com/cgi-bin/cvsweb/ascii85/
    /// </summary>
    /// <remarks>
    /// Jeff Atwood
    /// http://www.codinghorror.com/blog/archives/000410.html
    /// </remarks>
    class Ascii85 {
        /// <summary>
        /// Prefix mark that identifies an encoded ASCII85 string
        /// </summary>
        public string PrefixMark = "<~";
        /// <summary>
        /// Suffix mark that identifies an encoded ASCII85 string
        /// </summary>
        public string SuffixMark = "~>";
        /// <summary>
        /// Maximum line length for encoded ASCII85 string; 
        /// set to zero for one unbroken line.
        /// </summary>
        public int LineLength = 75;

        private const int _asciiOffset = 33;
        private readonly byte[] _encodedBlock = new byte[5];
        private readonly byte[] _decodedBlock = new byte[4];
        private uint _tuple = 0;
        private int _linePos = 0;

        private readonly uint[] pow85 = { 85 * 85 * 85 * 85, 85 * 85 * 85, 85 * 85, 85, 1 };

        private static readonly Ascii85 _instance = new Ascii85();

        public static Ascii85 Instance => _instance;

        /// <summary>
        /// Decodes an ASCII85 encoded string into the original binary data
        /// </summary>
        /// <param name="s">ASCII85 encoded string</param>
        /// <param name="enforceMarks">enforce marks</param>
        /// <returns>byte array of decoded binary data</returns>
        public byte[] Decode(string s, bool enforceMarks) {
            if(enforceMarks) {
                if(!s.StartsWith(PrefixMark) | !s.EndsWith(SuffixMark)) {
                    throw new Exception("ASCII85 encoded data should begin with '" + PrefixMark +
                       "' and end with '" + SuffixMark + "'");
                }
            }

            // strip prefix and suffix if present
            if(s.StartsWith(PrefixMark)) {
                s = s.Substring(PrefixMark.Length);
            }
            if(s.EndsWith(SuffixMark)) {
                s = s.Substring(0, s.Length - SuffixMark.Length);
            }

            MemoryStream ms = new MemoryStream();
            int count = 0;
            bool processChar = false;

            foreach(char c in s) {
                switch(c) {
                    case 'z':
                        if(count != 0) {
                            throw new Exception("The character 'z' is invalid inside an ASCII85 block.");
                        }
                        _decodedBlock[0] = 0;
                        _decodedBlock[1] = 0;
                        _decodedBlock[2] = 0;
                        _decodedBlock[3] = 0;
                        ms.Write(_decodedBlock, 0, _decodedBlock.Length);
                        processChar = false;
                        break;
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0':
                    case '\f':
                    case '\b':
                        processChar = false;
                        break;
                    default:
                        if(c < '!' || c > 'u') {
                            throw new Exception("Bad character '" + c + "' found. ASCII85 only allows characters '!' to 'u'.");
                        }
                        processChar = true;
                        break;
                }

                if(processChar) {
                    _tuple += ((uint)(c - _asciiOffset) * pow85[count]);
                    count++;
                    if(count == _encodedBlock.Length) {
                        DecodeBlock();
                        ms.Write(_decodedBlock, 0, _decodedBlock.Length);
                        _tuple = 0;
                        count = 0;
                    }
                }
            }

            // if we have some bytes left over at the end..
            if(count != 0) {
                if(count == 1) {
                    throw new Exception("The last block of ASCII85 data cannot be a single byte.");
                }
                count--;
                _tuple += pow85[count];
                DecodeBlock(count);
                for(int i = 0; i < count; i++) {
                    ms.WriteByte(_decodedBlock[i]);
                }
            }

            return ms.ToArray();
        }

        /// <summary>
        /// Encodes binary data into a plaintext ASCII85 format string
        /// </summary>
        /// <param name="ba">binary data to encode</param>
        /// <param name="enforceMarks">enforce marks</param>
        /// <returns>ASCII85 encoded string</returns>
        public string Encode(byte[] ba, bool enforceMarks) {
            StringBuilder sb = new StringBuilder(ba.Length * (_encodedBlock.Length / _decodedBlock.Length));
            _linePos = 0;

            if(enforceMarks) {
                AppendString(sb, PrefixMark);
            }

            int count = 0;
            _tuple = 0;
            foreach(byte b in ba) {
                if(count >= _decodedBlock.Length - 1) {
                    _tuple |= b;
                    if(_tuple == 0) {
                        AppendChar(sb, 'z');
                    } else {
                        EncodeBlock(sb);
                    }
                    _tuple = 0;
                    count = 0;
                } else {
                    _tuple |= (uint)(b << (24 - (count * 8)));
                    count++;
                }
            }

            // if we have some bytes left over at the end..
            if(count > 0) {
                EncodeBlock(count + 1, sb);
            }

            if(enforceMarks) {
                AppendString(sb, SuffixMark);
            }
            return sb.ToString();
        }

        private void EncodeBlock(StringBuilder sb) {
            EncodeBlock(_encodedBlock.Length, sb);
        }

        private void EncodeBlock(int count, StringBuilder sb) {
            for(int i = _encodedBlock.Length - 1; i >= 0; i--) {
                _encodedBlock[i] = (byte)((_tuple % 85) + _asciiOffset);
                _tuple /= 85;
            }

            for(int i = 0; i < count; i++) {
                char c = (char)_encodedBlock[i];
                AppendChar(sb, c);
            }

        }

        private void DecodeBlock() {
            DecodeBlock(_decodedBlock.Length);
        }

        private void DecodeBlock(int bytes) {
            for(int i = 0; i < bytes; i++) {
                _decodedBlock[i] = (byte)(_tuple >> (24 - (i * 8)));
            }
        }

        private void AppendString(StringBuilder sb, string s) {
            if(LineLength > 0 && (_linePos + s.Length > LineLength)) {
                _linePos = 0;
                sb.Append('\n');
            } else {
                _linePos += s.Length;
            }
            sb.Append(s);
        }

        private void AppendChar(StringBuilder sb, char c) {
            sb.Append(c);
            _linePos++;
            if(LineLength > 0 && (_linePos >= LineLength)) {
                _linePos = 0;
                sb.Append('\n');
            }
        }

    }
}

// FILE: src/NetBox/ByteFormat.cs

namespace NetBox {
    using global::System;

    static class ByteFormat {
        //http://en.wikipedia.org/wiki/Kibibyte

        private const long Kb = 1000;         //kilobyte
        private const long KiB = 1024;        //kikibyte
        private const long Mb = Kb * 1000;      //megabyte
        private const long MiB = KiB * 1024;    //memibyte
        private const long Gb = Mb * 1000;      //gigabyte
        private const long GiB = MiB * 1024;    //gigibyte
        private const long Tb = Gb * 1000;      //terabyte
        private const long TiB = GiB * 1024;    //tebibyte
        private const long Pb = Tb * 1024;      //petabyte
        private const long PiB = TiB * 1024;    //pepibyte

        public enum Standard {
            /// <summary>
            ///  International System of Units
            /// </summary>
            Si,

            /// <summary>
            /// International Electrotechnical Commission
            /// </summary>
            Iec
        }

        /// <summary>
        /// Returns the best formatted string representation of a byte value
        /// </summary>
        /// <param name="bytes">number of bytes</param>
        /// <param name="st"></param>
        /// <returns>formatted string</returns>
        private static string ToString(long bytes, Standard st = Standard.Iec) {
            return ToString(bytes, st, null);
        }

        /// <summary>
        /// Returns the best formatted string representation of a byte value
        /// </summary>
        /// <param name="bytes">number of bytes</param>
        /// <param name="st"></param>
        /// <param name="customFormat">Defines a custom numerical format for the conversion.
        /// If this parameters is null or empty the default format will be used 0.00</param>
        /// <returns>formatted string</returns>
        public static string ToString(long bytes, Standard st, string? customFormat) {
            if(bytes == 0)
                return "0";

            if(string.IsNullOrEmpty(customFormat))
                customFormat = "0.00";

            string result;
            bool isNegative = bytes < 0;
            bytes = Math.Abs(bytes);

            if(st == Standard.Si) {
                if(bytes < Mb)
                    result = BytesToKb(bytes, customFormat);

                else if(bytes < Gb)
                    result = BytesToMb(bytes, customFormat);

                else if(bytes < Tb)
                    result = BytesToGb(bytes, customFormat);

                else if(bytes < Pb)
                    result = BytesToTb(bytes, customFormat);

                else
                    result = BytesToPb(bytes, customFormat);
            } else {
                if(bytes < MiB)
                    result = BytesToKib(bytes, customFormat);

                else if(bytes < GiB)
                    result = BytesToMib(bytes, customFormat);

                else if(bytes < TiB)
                    result = BytesToGib(bytes, customFormat);

                else if(bytes < PiB)
                    result = BytesToTib(bytes, customFormat);

                else
                    result = BytesToPib(bytes, customFormat);
            }

            return isNegative ? ("-" + result) : (result);
        }

        private static string BytesToPb(long bytes, string? customFormat) {
            double tb = bytes / ((double)Pb);
            return tb.ToString(customFormat) + " PB";
        }
        private static string BytesToPib(long bytes, string? customFormat) {
            double tb = bytes / ((double)PiB);
            return tb.ToString(customFormat) + " PiB";
        }

        private static string BytesToTb(long bytes, string? customFormat) {
            double tb = bytes / ((double)Tb);
            return tb.ToString(customFormat) + " TB";
        }
        private static string BytesToTib(long bytes, string? customFormat) {
            double tb = bytes / ((double)TiB);
            return tb.ToString(customFormat) + " TiB";
        }

        private static string BytesToGb(long bytes, string? customFormat) {
            double gb = bytes / ((double)Gb);
            return gb.ToString(customFormat) + " GB";
        }
        private static string BytesToGib(long bytes, string? customFormat) {
            double gb = bytes / ((double)GiB);
            return gb.ToString(customFormat) + " GiB";
        }

        private static string BytesToMb(long bytes, string? customFormat) {
            double mb = bytes / ((double)Mb);
            return mb.ToString(customFormat) + " MB";
        }
        private static string BytesToMib(long bytes, string? customFormat) {
            double mb = bytes / ((double)MiB);
            return mb.ToString(customFormat) + " MiB";
        }

        private static string BytesToKb(long bytes, string? customFormat) {
            double kb = bytes / ((double)Kb);
            return kb.ToString(customFormat) + " KB";
        }
        private static string BytesToKib(long bytes, string? customFormat) {
            double kb = bytes / ((double)KiB);
            return kb.ToString(customFormat) + " KiB";
        }
    }
}

// FILE: src/NetBox/Performance/TimeMeasure.cs

namespace NetBox.Performance
{
   using global::System;
   using global::System.Diagnostics;

   /// <summary>
   /// Measures a time slice as precisely as possible
   /// </summary>
   class TimeMeasure : IDisposable
   {
      private readonly Stopwatch _sw = new Stopwatch();

      /// <summary>
      /// Creates the measure object
      /// </summary>
      public TimeMeasure()
      {
         _sw.Start();
      }

      /// <summary>
      /// Returns number of elapsed ticks since the start of measure.
      /// The measuring process will continue running.
      /// </summary>
      public long ElapsedTicks => _sw.ElapsedTicks;

      /// <summary>
      /// Returns number of elapsed milliseconds since the start of measure.
      /// The measuring process will continue running.
      /// </summary>
      public long ElapsedMilliseconds => _sw.ElapsedMilliseconds;


      /// <summary>
      /// Gets time elapsed from the time this measure was created
      /// </summary>
      public TimeSpan Elapsed => _sw.Elapsed;

      /// <summary>
      /// Stops measure object if still running
      /// </summary>
      public void Dispose()
      {
         if (_sw.IsRunning)
         {
            _sw.Stop();
         }
      }
   }
}


// FILE: src/NetBox/Generator/RandomGenerator.cs

namespace NetBox.Generator {
    using global::System;
    using global::System.IO;
    using global::System.Security.Cryptography;
    using global::System.Text;

    /// <summary>
    /// Generates random data using <see cref="RandomNumberGenerator"/> for increased security
    /// </summary>
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
}

// FILE: src/NetBox/System/DateTimeExtensions.cs

namespace System {
    /// <summary>
    /// <see cref="DateTime"/> extension methods
    /// </summary>
    static class DateTimeExtensions {
        /// <summary>
        /// Strips time from the date structure
        /// </summary>
        public static DateTime RoundToDay(this DateTime time) {
            return new DateTime(time.Year, time.Month, time.Day);
        }

        /// <summary>
        /// Changes to the end of day time, i.e. hours, minutes and seconds are changed to 23:59:59
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public static DateTime EndOfDay(this DateTime time) {
            return new DateTime(time.Year, time.Month, time.Day, 23, 59, 59);
        }

        /// <summary>
        /// Rounds to the closest minute
        /// </summary>
        /// <param name="time">Input date</param>
        /// <param name="round">Closest minute i.e. 15, 30, 45 etc.</param>
        /// <param name="roundLeft">Whether to use minimum or maximum value. For example
        /// when time is 13:14 and rounding is to every 15 minutes, when this parameter is true
        /// the result it 13:00, otherwise 13:15</param>
        /// <returns></returns>
        public static DateTime RoundToMinute(this DateTime time, int round, bool roundLeft) {
            int minute = time.Minute;
            int leftover = minute % round;
            if(leftover == 0)
                return time;
            int addHours = 0;
            minute -= leftover;

            if(!roundLeft)
                minute += round;
            if(minute > 59) {
                minute = minute % 60;
                addHours = 1;
            }

            return new DateTime(time.Year, time.Month, time.Day, time.Hour + addHours, minute, 0);
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

        /// <summary>
        /// Returns true if the date is today's date.
        /// </summary>
        public static bool IsToday(this DateTime time) {
            DateTime now = DateTime.UtcNow;

            return now.Year == time.Year &&
               now.Month == time.Month &&
               now.Day == time.Day;
        }

        /// <summary>
        /// Returns true if the date is tomorrow's date.
        /// </summary>
        public static bool IsTomorrow(this DateTime time) {
            TimeSpan diff = DateTime.UtcNow - time;

            return diff.TotalDays >= 1 && diff.TotalDays < 2;
        }

        /// <summary>
        /// Returns date in "HH:mm" format
        /// </summary>
        public static string ToHourMinuteString(this DateTime time) {
            return time.ToString("HH:mm");
        }

        /// <summary>
        /// Formats date in ISO 8601 format
        /// </summary>
        public static string ToIso8601DateString(this DateTime time) {
            return time.ToString("o");
        }
    }
}

// FILE: src/NetBox/System/NumericExtensions.cs

namespace System {
    using NetBox;

    /// <summary>
    /// <see cref="long"/> extension methods
    /// </summary>
    static class LongExtensions {
        /// <summary>
        /// Converts number to readable size string in IEC format, i.e. 1024 converts to "1.02 KiB"
        /// </summary>
        public static string ToFileSizeString(this long number) {
            return ByteFormat.ToString(number, ByteFormat.Standard.Iec, null);
        }

        /// <summary>
        /// Converts number to readable size string in SI format, i.e. 1024 converts to "1.02 KB"
        /// </summary>
        public static string ToFileSizeUiString(this long number) {
            return ByteFormat.ToString(number, ByteFormat.Standard.Si, null);
        }
    }

    static class IntExtensions {
        /// <summary>
        /// Converts number to readable size string in IEC format, i.e. 1024 converts to "1.02 KiB"
        /// </summary>
        public static string ToFileSizeString(this int number) {
            return ByteFormat.ToString(number, ByteFormat.Standard.Iec, null);
        }

        /// <summary>
        /// Converts number to readable size string in SI format, i.e. 1024 converts to "1.02 KB"
        /// </summary>
        public static string ToFileSizeUiString(this int number) {
            return ByteFormat.ToString(number, ByteFormat.Standard.Si, null);
        }

        /// <summary>
        /// Converts number to seconds
        /// </summary>
        /// <param name="number">Number of seconds</param>
        /// <returns>Timespan values</returns>
        public static TimeSpan Seconds(this int number) {
            return TimeSpan.FromSeconds(number);
        }

        /// <summary>
        /// Converts number to minutes
        /// </summary>
        /// <param name="number">Number of minutes</param>
        /// <returns>Timespan value</returns>
        public static TimeSpan Minutes(this int number) {
            return TimeSpan.FromMinutes(number);
        }

        /// <summary>
        /// Converts number to hours 
        /// </summary>
        /// <param name="number">Number of hours</param>
        /// <returns>Timespan value</returns>
        public static TimeSpan Hours(this int number) {
            return TimeSpan.FromHours(number);
        }
    }
}

// FILE: src/NetBox/System/TaskExtensions.cs

namespace System
{
   using global::System.Threading.Tasks;
   using System.Diagnostics.CodeAnalysis;

   /// <summary>
   /// Task utility methods
   /// </summary>
   static class TaskExtensions
   {
      /// <summary>
      /// Fire-and-forget without compiler warnings
      /// </summary>
      /// <param name="task"></param>
      [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "task")]
      public static void Forget(this Task task)
      {
      }
   }
}


// FILE: src/NetBox/System/GuidExtensions.cs


namespace System {
    using NetBox;

    static class GuidExtensions {
        public static string ToShortest(this Guid g) {
            return Ascii85.Instance.Encode(g.ToByteArray(), true);
        }
    }
}


// FILE: src/NetBox/System/ByteArrayExtensions.cs

namespace System {
    using System.Security.Cryptography;
    using Crypto = System.Security.Cryptography;

    /// <summary>
    /// Byte array extensions methods
    /// </summary>
    static class ByteArrayExtensions {
        private static readonly char[] LowerCaseHexAlphabet = "0123456789abcdef".ToCharArray();
        private static readonly char[] UpperCaseHexAlphabet = "0123456789ABCDEF".ToCharArray();
        private static readonly Crypto.MD5 _md5 = Crypto.MD5.Create();
        private static readonly Crypto.SHA256 _sha256 = Crypto.SHA256.Create();


        /// <summary>
        /// Converts byte array to hexadecimal string
        /// </summary>
        public static string? ToHexString(this byte[]? bytes) {
            return ToHexString(bytes, true);
        }

        private static string? ToHexString(this byte[]? bytes, bool lowerCase) {
            if(bytes == null)
                return null;

            char[] alphabet = lowerCase ? LowerCaseHexAlphabet : UpperCaseHexAlphabet;

            int len = bytes.Length;
            char[] result = new char[len * 2];

            int i = 0;
            int j = 0;

            while(i < len) {
                byte b = bytes[i++];
                result[j++] = alphabet[b >> 4];
                result[j++] = alphabet[b & 0xF];
            }

            return new string(result);
        }

        public static byte[]? MD5(this byte[]? bytes) {
            if(bytes == null)
                return null;

            return _md5.ComputeHash(bytes);
        }

        public static byte[]? SHA256(this byte[]? bytes) {
            if(bytes == null)
                return null;

            return _sha256.ComputeHash(bytes);
        }

        public static byte[]? HMACSHA256(this byte[]? data, byte[] key) {
            if(data == null)
                return null;

            var alg = new Crypto.HMACSHA256(key);
            return alg.ComputeHash(data);
        }
    }
}

// FILE: src/NetBox/System/StringExtensions.cs

namespace System {
    using System.Net.Http;
    using global::System.Threading.Tasks;
    using NetBox;
    using global::System.Collections.Generic;
    using global::System.IO;
    using global::System.Linq;
    using global::System.Text;
    using global::System.Text.RegularExpressions;

    /// <summary>
    /// String extensions.
    /// </summary>
    static class StringExtensions {
        private const string HtmlStripPattern = @"<(.|\n)*?>";

        static readonly char[] Invalid = Path.GetInvalidFileNameChars();

        /// <summary>
        /// Converts hex string to byte array
        /// </summary>
        /// <param name="hex"></param>
        /// <returns></returns>
        public static byte[]? FromHexToBytes(this string? hex) {
            if(hex == null)
                return null;

            byte[] raw = new byte[hex.Length / 2];
            for(int i = 0; i < raw.Length; i++) {
                try {
                    raw[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
                } catch(FormatException) {
                    return null;
                }
            }
            return raw;
        }

        #region [ HTML Helpers ]

        /// <summary>
        /// Strips HTML string from any tags leaving text only.
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static string? StripHtml(this string? s) {
            if(s == null)
                return null;

            //This pattern Matches everything found inside html tags;
            //(.|\n) - > Look for any character or a new line
            // *?  -> 0 or more occurences, and make a non-greedy search meaning
            //That the match will stop at the first available '>' it sees, and not at the last one
            //(if it stopped at the last one we could have overlooked
            //nested HTML tags inside a bigger HTML tag..)

            return Regex.Replace(s, HtmlStripPattern, string.Empty);
        }

        #endregion

        #region [ Encoding ]

        /// <summary>
        /// Encodes a string to BASE64 format
        /// </summary>
        public static string? Base64Encode(this string? s) {
            if(s == null)
                return null;

            return Convert.ToBase64String(Encoding.UTF8.GetBytes(s));
        }

        /// <summary>
        /// Decodes a BASE64 encoded string
        /// </summary>
        public static string? Base64Decode(this string? s) {
            if(s == null)
                return null;

            byte[] data = Convert.FromBase64String(s);

            return Encoding.UTF8.GetString(data, 0, data.Length);
        }


        /// <summary>
        /// Decodes a BASE64 encoded string to byte array
        /// </summary>
        /// <param name="s">String to decode</param>
        /// <returns>Byte array</returns>
        public static byte[]? Base64DecodeAsBytes(this string? s) {
            if(s == null)
                return null;

            return Convert.FromBase64String(s);
        }

        /// <summary>
        /// Converts shortest guid representation back to Guid. See <see cref="GuidExtensions.ToShortest(Guid)"/>
        /// on how to convert Guid to string.
        /// </summary>
        public static Guid FromShortestGuid(this string s) {
            byte[] guidBytes = Ascii85.Instance.Decode(s, false);
            return new Guid(guidBytes);
        }

        /// <summary>
        /// URL-encodes input string
        /// </summary>
        /// <param name="value">String to encode</param>
        /// <returns>Encoded string</returns>
        public static string? UrlEncode(this string? value) {
            return WebUtility.UrlEncode(value);
        }

        /// <summary>
        /// URL-decodes input string
        /// </summary>
        /// <param name="value">String to decode</param>
        /// <returns>Decoded string</returns>
        public static string? UrlDecode(this string? value) {
            return WebUtility.UrlDecode(value);
        }

        public static byte[]? UTF8Bytes(this string? s) {
            if(s == null)
                return null;

            return Encoding.UTF8.GetBytes(s);
        }

        #endregion

        #region [ Hashing ]

        public static string? MD5(this string? s) {
            if(s == null)
                return null;

            return Encoding.UTF8.GetBytes(s).MD5().ToHexString();
        }

        public static string? SHA256(this string? s) {
            if(s == null)
                return null;

            return Encoding.UTF8.GetBytes(s).SHA256().ToHexString();
        }

        public static byte[]? HMACSHA256(this string? s, byte[] key) {
            if(s == null)
                return null;

            return Encoding.UTF8.GetBytes(s).HMACSHA256(key);
        }


        #endregion

        #region [ Stream Conversion ]

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

        #endregion

        #region [ String Manipulation ]

        private static bool FindTagged(ref string s, ref string startToken, ref string endToken, bool includeOuterTokens, out int startIdx, out int length) {
            int idx0 = s.IndexOf(startToken, StringComparison.Ordinal);

            if(idx0 != -1) {
                int idx1 = s.IndexOf(endToken, idx0 + startToken.Length, StringComparison.Ordinal);

                if(idx1 != -1) {
                    startIdx = includeOuterTokens ? idx0 : idx0 + startToken.Length;
                    length = includeOuterTokens
                                 ? (idx1 - idx0 + endToken.Length)
                                 : (idx1 - idx0 - startToken.Length);

                    return true;
                }
            }

            startIdx = length = -1;
            return false;
        }

        /// <summary>
        /// Looks for <paramref name="startTag"/> and <paramref name="endTag"/> followed in sequence and when found returns the text between them.
        /// </summary>
        /// <param name="s">Input string</param>
        /// <param name="startTag">Start tag</param>
        /// <param name="endTag">End tag</param>
        /// <param name="includeOuterTags">When set to true, returns the complete phrase including start and end tag value,
        /// otherwise only inner text returned</param>
        /// <returns></returns>
        public static string? FindTagged(this string s, string? startTag, string? endTag, bool includeOuterTags) {
            if(startTag == null)
                throw new ArgumentNullException(nameof(startTag));
            if(endTag == null)
                throw new ArgumentNullException(nameof(endTag));

            int start, length;
            if(FindTagged(ref s, ref startTag, ref endTag, includeOuterTags, out start, out length)) {
                return s.Substring(start, length);
            }

            return null;
        }

        /// <summary>
        /// Looks for <paramref name="startTag"/> and <paramref name="endTag"/> followed in sequence, and if found
        /// performs a replacement of text inside them with <paramref name="replacementText"/>
        /// </summary>
        /// <param name="s">Input string</param>
        /// <param name="startTag">Start tag</param>
        /// <param name="endTag">End tag</param>
        /// <param name="replacementText">Replacement text</param>
        /// <param name="replaceOuterTokens">When set to true, not only the text between tags is replaced, but the whole
        /// phrase with <paramref name="startTag"/>, text between tags and <paramref name="endTag"/></param>
        /// <returns></returns>
        public static string ReplaceTagged(this string s, string startTag, string endTag, string replacementText, bool replaceOuterTokens) {
            if(startTag == null)
                throw new ArgumentNullException(nameof(startTag));
            if(endTag == null)
                throw new ArgumentNullException(nameof(endTag));
            //if (replacementText == null) throw new ArgumentNullException("replacementText");

            int start, length;

            if(FindTagged(ref s, ref startTag, ref endTag, replaceOuterTokens, out start, out length)) {
                s = s.Remove(start, length);

                if(replacementText != null)
                    s = s.Insert(start, replacementText);
            }

            return s;
        }

        /// <summary>
        /// Converts a string with spaces to a camel case version, for example
        /// "The camel string" is converted to "TheCamelString"
        /// </summary>
        public static string? SpacedToCamelCase(this string? s) {
            if(s == null)
                return null;

            string[] parts = s.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            var b = new StringBuilder();
            foreach(string part in parts) {
                string? uc = part.Capitalize();
                b.Append(uc);
            }
            return b.ToString();
        }

        /// <summary>
        /// Transforms the string so that the first letter is uppercase and the rest of them are lowercase
        /// </summary>
        public static string? Capitalize(this string? s) {
            if(s == null)
                return null;
            var b = new StringBuilder();

            for(int i = 0; i < s.Length; i++) {
                b.Append(i == 0 ? char.ToUpper(s[i]) : char.ToLower(s[i]));
            }

            return b.ToString();
        }

        /// <summary>
        /// Pythonic approach to slicing strings
        /// </summary>
        /// <param name="s">Input string</param>
        /// <param name="start">Is the start index to slice from. It can be either positive or negative.
        /// Negative value indicates that the index is taken from the end of the string.</param>
        /// <param name="end">Is the index to slice to. It can be either positive or negative.
        /// Negative value indicates that the index is taken from the end of the string.</param>
        /// <returns>Sliced string</returns>
        public static string? Slice(this string? s, int? start, int? end) {
            if(s == null)
                return null;
            if(start == null && end == null)
                return s;

            int si = start ?? 0;
            int ei = end ?? s.Length;

            if(si < 0) {
                si = s.Length + si;
                if(si < 0)
                    si = 0;
            }

            if(si > s.Length) {
                si = s.Length - 1;
            }

            if(ei < 0) {
                ei = s.Length + ei;
                if(ei < 0)
                    ei = 0;
            }

            if(ei > s.Length) {
                ei = s.Length;
            }

            return s.Substring(si, ei - si);
        }

        /// <summary>
        /// Splits the string into key and value using the provided delimiter values. Both key and value are trimmed as well.
        /// </summary>
        /// <param name="s">Input string. When null returns null immediately.</param>
        /// <param name="delimiter">List of delmiters between key and value. This method searches for all the provided
        /// delimiters, and splits by the first left-most one.</param>
        /// <returns>A tuple of two values where the first one is the key and second is the value. If none of the delimiters
        /// are found the second value of the tuple is null and the first value is the input string</returns>
        public static Tuple<string, string?>? SplitByDelimiter(this string? s, params string?[]? delimiter) {
            if(s == null)
                return null;

            string key;
            string? value;

            if(delimiter == null || delimiter.Length == 0) {
                key = s.Trim();
                value = null;
            } else {

                List<int> indexes = delimiter.Where(d => d != null).Select(d => s.IndexOf(d!)).Where(d => d != -1).ToList();

                if(indexes.Count == 0) {
                    key = s.Trim();
                    value = null;
                } else {
                    int idx = indexes.OrderBy(i => i).First();
                    key = s.Substring(0, idx);
                    value = s.Substring(idx + 1);
                }
            }

            return new Tuple<string, string?>(key, value);
        }

        /// <summary>
        /// Splits text line by line and removes lines containing specific substring
        /// </summary>
        public static string RemoveLinesContaining(this string input, string substring, StringComparison stringComparison = StringComparison.CurrentCulture) {
            if(string.IsNullOrEmpty(input))
                return input;

            var result = new StringBuilder();

            using(var sr = new StringReader(input)) {
                string? line;
                while((line = sr.ReadLine()) != null) {
                    if(line.IndexOf(substring, stringComparison) != -1)
                        continue;

                    result.AppendLine(line);
                }
            }

            return result.ToString();
        }


        #endregion

        #region [ JSON ]

        /// <summary>
        /// Escapes a string for JSON encoding
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static string? ToEscapedJsonValueString(this string? s) {
            if(s == null)
                return null;

            string result = string.Empty;
            for(int i = 0; i < s.Length; i++) {
                char c = s[i];
                string ec;

                switch(c) {
                    case '\t':
                        ec = "\\t";
                        break;
                    case '\n':
                        ec = "\\n";
                        break;
                    case '\r':
                        ec = "\\r";
                        break;
                    case '\f':
                        ec = "\\f";
                        break;
                    case '\b':
                        ec = "\\b";
                        break;
                    case '\\':
                        ec = "\\\\";
                        break;
                    case '\u0085': // Next Line
                        ec = "\\u0085";
                        break;
                    case '\u2028': // Line Separator
                        ec = "\\u2028";
                        break;
                    case '\u2029': // Paragraph Separator
                        ec = "\\u2029";
                        break;
                    default:
                        ec = new string(c, 1);
                        break;
                }

                result += ec;
            }

            return result;
        }

        #endregion

        #region [ Networking ]

        /// <summary>
        /// Treat this string as URL and download it as stream
        /// </summary>
        /// <param name="uri"></param>
        /// <returns></returns>
        public static async Task<TempFile?> DownloadUrlToTempFile(this string? uri) {
            if(string.IsNullOrEmpty(uri))
                return null;

            var tf = new TempFile();

            using(Stream src = await new HttpClient().GetStreamAsync(uri)) {
                using(Stream tgt = File.Create(tf)) {
                    await src.CopyToAsync(tgt);
                }
            }

            return tf;
        }

        #endregion
    }
}

// FILE: src/NetBox/System/EnumerableExtensions.cs

namespace System {
    using System.Collections;
    using System.Diagnostics;
    using global::System.Collections.Generic;
    using global::System.Linq;

    /// <summary>
    /// <see cref="System.IEquatable{T}"/> extension methods
    /// </summary>
    static class EnumerableExtensions {

#if NET6_0_OR_GREATER
#else
        // .Chunk<> polyfill available from .net 6 and higher

        /// <summary>
        /// Split the elements of a sequence into chunks of size at most <paramref name="size"/>.
        /// </summary>
        /// <remarks>
        /// Every chunk except the last will be of size <paramref name="size"/>.
        /// The last chunk will contain the remaining elements and may be of a smaller size.
        /// </remarks>
        /// <param name="source">
        /// An <see cref="IEnumerable{T}"/> whose elements to chunk.
        /// </param>
        /// <param name="size">
        /// Maximum size of each chunk.
        /// </param>
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// <returns>
        /// An <see cref="IEnumerable{T}"/> that contains the elements the input sequence split into chunks of size <paramref name="size"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="source"/> is null.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="size"/> is below 1.
        /// </exception>
        public static IEnumerable<TSource[]> Chunk<TSource>(this IEnumerable<TSource> source, int size) {
            if(source is null)
                throw new ArgumentNullException(nameof(source));

            if(size < 1)
                throw new ArgumentOutOfRangeException(nameof(size), "must be >= 1");

            return ChunkIterator(source, size);
        }

        private static IEnumerable<TSource[]> ChunkIterator<TSource>(IEnumerable<TSource> source, int size) {
            using IEnumerator<TSource> e = source.GetEnumerator();

            // Before allocating anything, make sure there's at least one element.
            if(e.MoveNext()) {
                // Now that we know we have at least one item, allocate an initial storage array. This is not
                // the array we'll yield.  It starts out small in order to avoid significantly overallocating
                // when the source has many fewer elements than the chunk size.
                int arraySize = Math.Min(size, 4);
                int i;
                do {
                    var array = new TSource[arraySize];

                    // Store the first item.
                    array[0] = e.Current;
                    i = 1;

                    if(size != array.Length) {
                        // This is the first chunk. As we fill the array, grow it as needed.
                        for(; i < size && e.MoveNext(); i++) {
                            if(i >= array.Length) {
                                arraySize = (int)Math.Min((uint)size, 2 * (uint)array.Length);
                                Array.Resize(ref array, arraySize);
                            }

                            array[i] = e.Current;
                        }
                    } else {
                        // For all but the first chunk, the array will already be correctly sized.
                        // We can just store into it until either it's full or MoveNext returns false.
                        TSource[] local = array; // avoid bounds checks by using cached local (`array` is lifted to iterator object as a field)
                        Debug.Assert(local.Length == size);
                        for(; (uint)i < (uint)local.Length && e.MoveNext(); i++) {
                            local[i] = e.Current;
                        }
                    }

                    if(i != array.Length) {
                        Array.Resize(ref array, i);
                    }

                    yield return array;
                }
                while(i >= size && e.MoveNext());
            }
        }

        // TryGetNonEnumeratedCount polyfill from .NET 6 and higher

        /// <summary>
        ///   Attempts to determine the number of elements in a sequence without forcing an enumeration.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of <paramref name="source" />.</typeparam>
        /// <param name="source">A sequence that contains elements to be counted.</param>
        /// <param name="count">
        ///     When this method returns, contains the count of <paramref name="source" /> if successful,
        ///     or zero if the method failed to determine the count.</param>
        /// <returns>
        ///   <see langword="true" /> if the count of <paramref name="source"/> can be determined without enumeration;
        ///   otherwise, <see langword="false" />.
        /// </returns>
        /// <remarks>
        ///   The method performs a series of type tests, identifying common subtypes whose
        ///   count can be determined without enumerating; this includes <see cref="ICollection{T}"/>,
        ///   <see cref="ICollection"/> as well as internal types used in the LINQ implementation.
        ///
        ///   The method is typically a constant-time operation, but ultimately this depends on the complexity
        ///   characteristics of the underlying collection implementation.
        /// </remarks>
        public static bool TryGetNonEnumeratedCount<TSource>(this IEnumerable<TSource> source, out int count) {
            if(source is null)
                throw new ArgumentNullException(nameof(source));

            if(source is ICollection<TSource> collectionoft) {
                count = collectionoft.Count;
                return true;
            }

            if(source is ICollection collection) {
                count = collection.Count;
                return true;
            }

            count = 0;
            return false;
        }

#endif

        /// <summary>
        /// Performs a specific action on each element of the sequence
        /// </summary>
        public static IEnumerable<T> ForEach<T>(this IEnumerable<T> source, Action<T> action) {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(action == null)
                throw new ArgumentNullException(nameof(action));

            foreach(T element in source) {
                action(element);

                yield return element;
            }
        }
    }
}

// FILE: src/NetBox/System/StreamExtensions.cs

namespace System {
    using global::System.Collections.Generic;
    using global::System.IO;
    using global::System.Text;

    /// <summary>
    /// <see cref="Stream"/> extension
    /// </summary>
    static class StreamExtensions {
        #region [ General ]

        /// <summary>
        /// Attemps to get the size of this stream by reading the Length property, otherwise returns 0.
        /// </summary>
        public static bool TryGetSize(this Stream s, out long size) {
            try {
                size = s.Length;
                return true;
            } catch(NotSupportedException) {

            } catch(ObjectDisposedException) {

            }

            size = 0;
            return false;
        }

        /// <summary>
        /// Attemps to get the size of this stream by reading the Length property, otherwise returns 0.
        /// </summary>
        public static long? TryGetSize(this Stream s) {
            long size;
            if(TryGetSize(s, out size)) {
                return size;
            }

            return null;
        }

        #endregion

        #region [ Seek and Read ]

        /// <summary>
        /// Reads the stream until a specified sequence of bytes is reached.
        /// </summary>
        /// <returns>Bytes before the stop sequence</returns>
        public static byte[] ReadUntil(this Stream s, byte[] stopSequence) {
            byte[] buf = new byte[1];
            var result = new List<byte>(50);
            int charsMatched = 0;

            while(s.Read(buf, 0, 1) == 1) {
                byte b = buf[0];
                result.Add(b);

                if(b == stopSequence[charsMatched]) {
                    if(++charsMatched == stopSequence.Length) {
                        break;
                    }
                } else {
                    charsMatched = 0;
                }

            }
            return result.ToArray();
        }

        #endregion

        #region [ Stream Conversion ]

        /// <summary>
        /// Reads all stream in memory and returns as byte array
        /// </summary>
        public static byte[]? ToByteArray(this Stream? stream) {
            if(stream == null)
                return null;
            using(var ms = new MemoryStream()) {
                stream.CopyTo(ms);
                return ms.ToArray();
            }
        }

        /// <summary>
        /// Converts the stream to string using specified encoding. This is done by reading the stream into
        /// byte array first, then applying specified encoding on top.
        /// </summary>
        public static string? ToString(this Stream? stream, Encoding encoding) {
            if(stream == null)
                return null;
            if(encoding == null)
                throw new ArgumentNullException(nameof(encoding));

            using(StreamReader reader = new StreamReader(stream, encoding)) {
                return reader.ReadToEnd();
            }
        }

        #endregion

    }
}

// FILE: src/NetBox/IO/NonCloseableStream.cs

namespace NetBox.IO
{
   using global::System;
   using global::System.IO;

   /// <summary>
   /// Represents a stream that ignores <see cref="IDisposable"/> operations i.e. cannot be closed by the client
   /// </summary>
   class NonCloseableStream : DelegatedStream
   {
      /// <summary>
      /// Creates an instance of this class
      /// </summary>
      /// <param name="master">Master stream to delegate operations to</param>
      public NonCloseableStream(Stream master) : base(master)
      {

      }

      /// <summary>
      /// Overrides this call to do nothing
      /// </summary>
      /// <param name="disposing"></param>
      protected override void Dispose(bool disposing)
      {
         //does nothing on purpose
      }
   }
}


// FILE: src/NetBox/IO/DelegatedStream.cs

namespace NetBox.IO
{
   using global::System;
   using global::System.IO;

   /// <summary>
   /// Makes stream members virtual instead of abstract, allowing to override only specific behaviors.
   /// </summary>
   class DelegatedStream : Stream
   {
      private readonly Stream _master;

      /// <summary>
      /// Creates an instance of non-closeable stream
      /// </summary>
      /// <param name="master"></param>
      public DelegatedStream(Stream master)
      {
         _master = master ?? throw new ArgumentNullException(nameof(master));
      }

      /// <summary>
      /// Calls <see cref="GetCanRead"/>
      /// </summary>
      public override bool CanRead => GetCanRead();

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      protected virtual bool GetCanRead()
      {
         return _master.CanRead;
      }

      /// <summary>
      /// Calls <see cref="GetCanSeek"/>
      /// </summary>
      public override bool CanSeek => GetCanSeek();

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      protected virtual bool GetCanSeek()
      {
         return _master.CanSeek;
      }

      /// <summary>
      /// Calls <see cref="GetCanWrite"/>
      /// </summary>
      public override bool CanWrite => GetCanWrite();

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      protected virtual bool GetCanWrite()
      {
         return _master.CanWrite;
      }

      /// <summary>
      /// Calls <see cref="GetLength"/>
      /// </summary>
      public override long Length => _master.Length;

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      protected virtual long GetLength()
      {
         return _master.Length;
      }

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      public override long Position { get => _master.Position; set => _master.Position = value; }

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      public override void Flush()
      {
         _master.Flush();
      }

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      public override int Read(byte[] buffer, int offset, int count)
      {
         return _master.Read(buffer, offset, count);
      }

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      public override long Seek(long offset, SeekOrigin origin)
      {
         return _master.Seek(offset, origin);
      }

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      public override void SetLength(long value)
      {
         _master.SetLength(value);
      }

      /// <summary>
      /// Delegates to master by default
      /// </summary>
      /// <returns></returns>
      public override void Write(byte[] buffer, int offset, int count)
      {
         _master.Write(buffer, offset, count);
      }
   }
}


// FILE: src/NetBox/FileFormats/Csv/CsvReader.cs

namespace NetBox.FileFormats.Csv {
    using global::System.Collections.Generic;
    using global::System.IO;
    using global::System.Linq;
    using global::System.Runtime.CompilerServices;
    using global::System.Text;

    /// <summary>
    /// Reads data from a CSV file. Fast and reliable, supports:
    /// - newline characters
    /// - double quotes
    /// - commas
    /// </summary>
    class CsvReader {
        private readonly StreamReader _reader;
        private readonly char[] _buffer;
        private const int BufferSize = 1024 * 10; //10k buffer
        private int _pos;
        private int _size = -1;
        private readonly List<char> _chars = new List<char>();
        private readonly List<string> _row = new List<string>();
        private ValueState _lastState = ValueState.None;

        private enum ValueState {
            None,
            HasMore,
            EndOfLine,
            EndOfFile
        }

        /// <summary>
        /// Creates an instance from an open stream and encoding
        /// </summary>
        public CsvReader(Stream stream, Encoding encoding) {
            _reader = new StreamReader(stream, encoding);
            _buffer = new char[BufferSize];
        }

        /// <summary>
        /// Reads all file as a dictionary of column name to list of values
        /// </summary>
        /// <param name="content">File content</param>
        /// <param name="hasColumns">When true, the first line of the file includes columns</param>
        /// <returns>Dictionary mapping the column name to the list of values</returns>
        public static Dictionary<string, List<string>> ReadAllFromContent(string content, bool hasColumns = true) {
            var result = new Dictionary<string, List<string>>();

            using(var ms = new MemoryStream(Encoding.UTF8.GetBytes(content))) {
                var reader = new CsvReader(ms, Encoding.UTF8);

                string[]? columnNames = hasColumns ? reader.ReadNextRow() : null;

                string[]? values;
                while((values = reader.ReadNextRow()) != null) {
                    if(columnNames == null) {
                        columnNames = Enumerable.Range(1, values.Length).Select(v => v.ToString()).ToArray();
                    }


                    for(int i = 0; i < values.Length; i++) {
                        if(!result.TryGetValue(columnNames[i], out List<string>? list)) {
                            list = new List<string>();
                            result[columnNames[i]] = list;
                        }

                        list.Add(values[i]);
                    }

                }
            }

            return result;
        }

        /// <summary>
        /// Reads next row of data if available.
        /// </summary>
        /// <returns>Null when end of file is reached, or array of strings for each column.</returns>
        public string[]? ReadNextRow() {
            if(ValueState.EndOfFile == _lastState)
                return null;

            _row.Clear();
            _chars.Clear();

            while(ValueState.HasMore == (_lastState = ReadNextValue())) {
                _row.Add(Str());
                _chars.Clear();
            }

            _row.Add(Str());

            return _row.ToArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string Str() {
            return new string(_chars.ToArray());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueState ReadNextValue() {
            int curr, next;
            bool quoted = false;
            short state = 0;
            while(NextChars(out curr, out next)) {
                switch(state) {
                    case 0:  //value start
                        if(curr == CsvFormat.ValueQuote) {
                            //if the value starts with quote it:
                            // - ends with quote
                            // - double quote must be transformed into single quote
                            // - column separator (usuallly ',') can be contained within the value
                            // - line separator '\r' can be inside the value and must be transforted to a proper line feed
                            quoted = true;
                            state = 1;
                        } else if(IsLineEndChar(curr)) {
                            while(IsLineEndChar(next)) {
                                NextChars(out curr, out next);
                            }

                            return next == -1 ? ValueState.EndOfFile : ValueState.EndOfLine;
                        } else if(CsvFormat.ValueSeparator == curr) {
                            //start from value separator, meaning it's an empty value
                            return next == -1 ? ValueState.EndOfFile : ValueState.HasMore;
                        } else {
                            //if the value doesn't start with quote:
                            // - it can't contain column separator or quote characters inside
                            // - it can't contain line separators
                            _chars.Add((char)curr);

                            if(CsvFormat.ValueSeparator == next) {
                                state = 2;
                            } else {
                                state = 1;
                            }
                        }
                        break;

                    case 1:  //reading value
                        if(quoted) {
                            switch(curr) {
                                case CsvFormat.ValueQuote:
                                    if(next == CsvFormat.ValueQuote) {
                                        //escaped quote, make a single one
                                        _chars.Add(CsvFormat.ValueQuote);

                                        //fast-forward to the next character
                                        _pos++;
                                    } else if(next == CsvFormat.ValueSeparator || next == '\r' || next == '\n') {
                                        //this is the end of value
                                        state = 2;
                                    } else {
                                        throw new IOException($"unexpected character {next} after {curr} at position {_pos}");
                                    }
                                    break;
                                case '\r':
                                    _chars.Add('\r');
                                    _chars.Add('\n');
                                    break;
                                default:
                                    _chars.Add((char)curr);
                                    break;
                            }
                        } else {
                            _chars.Add((char)curr);

                            //simple and most common case
                            if(next == CsvFormat.ValueSeparator || next == '\r' || next == '\n') {
                                state = 2;
                            }
                        }
                        break;

                    case 2:  //end of value
                             //if the character after end of value (curr) is a value separator it's not the end of line
                        bool hasMore = (curr == CsvFormat.ValueSeparator);

                        if(!hasMore) {
                            while(IsLineEndChar(next)) {
                                NextChars(out curr, out next);
                            }
                        }

                        return hasMore
                           ? ValueState.HasMore
                           : (next == -1 ? ValueState.EndOfFile : ValueState.EndOfLine);
                }

            }

            return ValueState.EndOfFile;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NextChars(out int curr, out int next) {
            if(_pos >= _size) {
                NextBlock();

                if(_size == 0) {
                    curr = next = -1;
                    return false;
                }
            }
            curr = _buffer[_pos++];


            if(_pos >= _size) {
                NextBlock();

                if(_size == 0) {
                    next = -1;
                    return true;
                }
            }
            next = _buffer[_pos];
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NextBlock() {
            _size = _reader.ReadBlock(_buffer, 0, BufferSize);
            _pos = 0;
            return _size > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsLineEndChar(int ch) {
            return ch == '\r' || ch == '\n';
        }
    }
}

// FILE: src/NetBox/FileFormats/Csv/CsvFormat.cs

namespace NetBox.FileFormats.Csv {
    static class CsvFormat {
        public const char ValueSeparator = ',';
        public const char ValueQuote = '"';
        public static readonly string ValueQuoteStr = "\"";
        public static readonly string ValueQuoteStrStr = "\"\"";
        private static readonly char[] QuoteMark = new[] { ValueSeparator, ValueQuote, '\r', '\n' };

        public static readonly char[] NewLine = { '\r', '\n' };

        private const string ValueLeftBracket = "\"";
        private const string ValueRightBracket = "\"";

        private const string ValueEscapeFind = "\"";
        private const string ValueEscapeValue = "\"\"";

        /// <summary>
        /// Implemented according to RFC4180 http://tools.ietf.org/html/rfc4180
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string EscapeValue(string value) {
            if(string.IsNullOrEmpty(value)) {
                return string.Empty;
            }

            //the values have to be quoted if they contain either quotes themselves,
            //value separators, or newline characters
            if(value.IndexOfAny(QuoteMark) == -1) {
                return value;
            }

            return ValueQuoteStr +
               value
                  .Replace(ValueQuoteStr, ValueQuoteStrStr)
                  .Replace("\r\n", "\r")
                  .Replace("\n", "\r") +
               ValueQuoteStr;
        }

        public static string? UnescapeValue(string value) {
            if(value == null)
                return null;

            return value;
        }
    }
}

// FILE: src/NetBox/FileFormats/Csv/CsvWriter.cs

namespace NetBox.FileFormats.Csv
{
   using global::System;
   using global::System.Collections.Generic;
   using global::System.IO;
   using global::System.Text;

   /// <summary>
   /// Writes data to a CSV file. Fast and reliable, supports:
   /// - newline characters
   /// - double quotes
   /// - commas
   /// </summary>
   class CsvWriter
   {
      private readonly Stream _destination;
      private readonly Encoding _encoding;
      private readonly byte[] _newLine;
      private readonly byte[] _separator;
      private bool _firstRowWritten;

      /// <summary>
      /// Creates a new instance of CsvWriter which uses UTF8 encoding
      /// </summary>
      /// <param name="destination"></param>
      public CsvWriter(Stream destination)
         : this(destination, Encoding.UTF8)
      {

      }

      /// <summary>
      /// Creates a new instance of CsvWriter on disk with UTF8 encoding
      /// </summary>
      /// <param name="fileName">File name or path</param>
      public CsvWriter(string fileName)
         : this(File.Create(fileName), Encoding.UTF8)
      {

      }

      /// <summary>
      /// Creates a new instance of CsvWriter and allows to specify the writer encoding
      /// </summary>
      /// <param name="destination"></param>
      /// <param name="encoding"></param>
      public CsvWriter(Stream destination, Encoding encoding)
      {
         if (destination == null) throw new ArgumentNullException("destination");
         if (encoding == null) throw new ArgumentNullException("encoding");
         if (!destination.CanWrite) throw new ArgumentException("must be writeable", "destination");

         _destination = destination;
         _encoding = encoding;
         _separator = new byte[] { (byte)CsvFormat.ValueSeparator };
         _newLine = _encoding.GetBytes(CsvFormat.NewLine);
      }

      /// <summary>
      /// Writes a row of data
      /// </summary>
      public void Write(params string[] values)
      {
         Write((IEnumerable<string>)values);
      }

      /// <summary>
      /// Writes a row of data
      /// </summary>
      public void Write(IEnumerable<string> values)
      {
         if (values == null) return;

         if (_firstRowWritten) _destination.Write(_newLine, 0, _newLine.Length);

         int i = 0;
         foreach (string column in values)
         {
            if (i != 0) _destination.Write(_separator, 0, _separator.Length);

            byte[] escaped = _encoding.GetBytes(CsvFormat.EscapeValue(column));
            _destination.Write(escaped, 0, escaped.Length);
            i++;
         }

         _firstRowWritten = true;
      }
   }
}


// FILE: src/NetBox/FileFormats/Ini/IniEntity.cs

namespace NetBox.FileFormats.Ini
{
   abstract class IniEntity
   {
   }
}


// FILE: src/NetBox/FileFormats/Ini/IniComment.cs

namespace NetBox.FileFormats.Ini
{
   class IniComment : IniEntity
   {
      public const string CommentSeparator = ";";

      public IniComment(string value)
      {
         Value = value;
      }

      public string Value { get; set; }

      public override string ToString() => Value;
   }
}


// FILE: src/NetBox/FileFormats/Ini/IniSection.cs

namespace NetBox.FileFormats.Ini {
    using global::System.Linq;
    using global::System;
    using global::System.Collections.Generic;
    using global::System.IO;

    class IniSection {
        public const string SectionKeySeparator = ".";

        private readonly List<IniEntity> _entities = new List<IniEntity>();
        private readonly Dictionary<string, IniKeyValue> _keyToValue = new Dictionary<string, IniKeyValue>();

        /// <summary>
        /// Section name. Null name indicates global section (or no section, depending on the context)
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name">Pass null to work with global section</param>
        public IniSection(string? name) {
            if(name != null) {
                if(name.StartsWith("["))
                    name = name.Substring(1);
                if(name.EndsWith("]"))
                    name = name.Substring(0, name.Length - 1);
            }

            Name = name;
        }

        public void Add(IniEntity entity) {
            _entities.Add(entity);

            IniKeyValue? ikv = entity as IniKeyValue;
            if(ikv != null) {
                _keyToValue[ikv.Key] = ikv;
            }
        }

        /// <summary>
        /// Get key names in this section
        /// </summary>
        public string[] Keys => _keyToValue.Select(p => p.Key).ToArray();

        public IniKeyValue? Set(string key, string? value) {
            if(value == null) {
                if(_keyToValue.TryGetValue(key, out IniKeyValue? ikv)) {
                    _keyToValue.Remove(key);
                    return ikv;
                }
                return null;
            } else {
                if(_keyToValue.TryGetValue(key, out IniKeyValue? ikv)) {
                    ikv.Value = value;
                } else {
                    ikv = new IniKeyValue(key, value, null);
                    Add(ikv);
                }
                return ikv;
            }
        }

        public static void SplitKey(string fullKey, out string? sectionName, out string keyName) {
            int idx = fullKey.IndexOf(SectionKeySeparator, StringComparison.CurrentCulture);

            if(idx == -1) {
                sectionName = null;
                keyName = fullKey;
            } else {
                sectionName = fullKey.Substring(0, idx);
                keyName = fullKey.Substring(idx + 1);
            }
        }

        public void WriteTo(StreamWriter writer) {
            foreach(IniEntity entity in _entities) {
                IniKeyValue? ikv = entity as IniKeyValue;
                if(ikv != null) {
                    writer.Write($"{ikv.Key}{IniKeyValue.KeyValueSeparator}{ikv.Value}");
                    if(ikv.Comment != null) {
                        writer.Write(" ");
                        writer.Write(IniComment.CommentSeparator);
                        writer.Write(ikv.Comment.Value);
                    }
                    writer.WriteLine();
                    continue;
                }

                IniComment? comment = entity as IniComment;
                if(comment != null) {
                    writer.Write(IniComment.CommentSeparator);
                    writer.WriteLine(comment.Value);
                }
            }
        }

        public override string ToString() {
            return Name ?? string.Empty;
        }
    }
}

// FILE: src/NetBox/FileFormats/Ini/IniKeyValue.cs

namespace NetBox.FileFormats.Ini {
    using global::System;

    internal class IniKeyValue : IniEntity {
        public const string KeyValueSeparator = "=";

        public IniKeyValue(string key, string value, string? comment) {
            if(key == null)
                throw new ArgumentNullException(nameof(key));

            Key = key;
            Value = value;
            Comment = comment == null ? null : new IniComment(comment);
        }

        public string Key { get; }

        public string Value { get; set; }

        public IniComment? Comment { get; }

        public static IniKeyValue? FromLine(string line, bool parseInlineComments) {
            int idx = line.IndexOf(KeyValueSeparator, StringComparison.CurrentCulture);
            if(idx == -1)
                return null;

            string key = line.Substring(0, idx).Trim();
            string value = line.Substring(idx + 1).Trim();
            string? comment = null;

            if(parseInlineComments) {
                idx = value.LastIndexOf(IniComment.CommentSeparator, StringComparison.CurrentCulture);
                if(idx != -1) {
                    comment = value.Substring(idx + 1).Trim();
                    value = value.Substring(0, idx).Trim();
                }
            }

            return new IniKeyValue(key, value, comment);
        }

        public override string ToString() {
            return $"{Value}";
        }
    }
}

// FILE: src/NetBox/FileFormats/Ini/StructuredIniFile.cs

namespace NetBox.FileFormats.Ini {
    using global::System;
    using global::System.Collections.Generic;
    using global::System.IO;
    using global::System.Linq;
    using global::System.Text;

    class StructuredIniFile {
        private const string _sectionBegin = "[";
        private const string _sectionEnd = "]";
        private static readonly char[] _sectionTrims = { '[', ']' };

        private readonly IniSection _globalSection;
        private readonly List<IniSection> _sections = new List<IniSection>();
        private readonly Dictionary<string, IniKeyValue> _fullKeyNameToValue = new Dictionary<string, IniKeyValue>(StringComparer.InvariantCultureIgnoreCase);

        public StructuredIniFile() {
            _globalSection = new IniSection(null);
            _sections.Add(_globalSection);
        }

        public string[] SectionNames => _sections.Where(s => s.Name != null).Select(s => s.Name!).ToArray();

        public string[]? GetSectionKeys(string sectionName) {
            IniSection? section = _sections.FirstOrDefault(s => s.Name == sectionName);
            if(section == null)
                return null;

            return section.Keys;
        }

        public string? this[string key] {
            get {
                if(key == null)
                    return null;

                return !_fullKeyNameToValue.TryGetValue(key, out IniKeyValue? value) ? null : value.Value;
            }
            set {
                if(key == null)
                    return;

                IniSection.SplitKey(key, out string? sectionName, out string keyName);
                IniSection? section = sectionName == null
                   ? _globalSection
                   : _sections.FirstOrDefault(s => s.Name == sectionName);
                if(section == null) {
                    section = new IniSection(sectionName);
                    _sections.Add(section);
                }
                IniKeyValue? ikv = section.Set(keyName, value);

                //update the local cache
                if(ikv != null) {
                    if(value == null) {
                        _fullKeyNameToValue.Remove(key);
                    } else {
                        _fullKeyNameToValue[key] = ikv;
                    }
                }
            }
        }

        public static StructuredIniFile FromString(string content, bool parseInlineComments = true) {
            using(Stream input = new MemoryStream(Encoding.UTF8.GetBytes(content))) {
                return FromStream(input, parseInlineComments);
            }
        }

        public static StructuredIniFile FromStream(Stream inputStream, bool parseInlineComments = true) {
            if(inputStream == null)
                throw new ArgumentNullException(nameof(inputStream));

            var file = new StructuredIniFile();

            using(var reader = new StreamReader(inputStream)) {
                IniSection section = file._globalSection;

                string? line;
                while((line = reader.ReadLine()) != null) {
                    line = line.Trim();

                    if(line.StartsWith(_sectionBegin)) {
                        //start new section
                        line = line.Trim();
                        section = new IniSection(line);
                        file._sections.Add(section);
                    } else if(line.StartsWith(IniComment.CommentSeparator)) {
                        //whole line is a comment
                        string comment = line.Substring(1).Trim();
                        section.Add(new IniComment(comment));
                    } else {
                        IniKeyValue? ikv = IniKeyValue.FromLine(line, parseInlineComments);
                        if(ikv == null)
                            continue;

                        section.Add(ikv);
                        string fullKey = section.Name == null
                           ? ikv.Key
                           : $"{section.Name}{IniSection.SectionKeySeparator}{ikv.Key}";
                        file._fullKeyNameToValue[fullKey] = ikv;

                    }
                }
            }

            return file;
        }

        public void WriteTo(Stream outputStream) {
            if(outputStream == null)
                throw new ArgumentNullException(nameof(outputStream));

            using(var writer = new StreamWriter(outputStream)) {
                foreach(IniSection section in _sections) {
                    if(section.Name != null) {
                        writer.WriteLine();
                        writer.WriteLine($"{_sectionBegin}{section.Name}{_sectionEnd}");
                    }

                    section.WriteTo(writer);
                }
            }
        }

        //private static 
    }
}