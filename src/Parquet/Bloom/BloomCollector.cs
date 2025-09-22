using System;
using System.Buffers.Binary;
using System.Text;

namespace Parquet.Bloom
{
    /// <summary>
    /// Incrementally builds a Split-Block Bloom Filter for a single column chunk.
    /// Accepts raw values in their physical form and inserts their PLAIN-encoded bytes
    /// (as specified by the Parquet bloom filter spec).
    /// </summary>
    internal sealed class BloomCollector : IDisposable {
        public SplitBlockBloomFilter Filter { get; }

        public BloomCollector(int blocks) {
            if(blocks <= 0)
                throw new ArgumentOutOfRangeException(nameof(blocks));
            this.Filter = new SplitBlockBloomFilter(blocks);
        }

        public void Dispose() { /* nothing to free */ }

        // ---- Insert helpers for common physical types (PLAIN encoding) ----

        /// <summary>Insert a nullable boolean (PLAIN: 1 byte 0/1).</summary>
        public void AddBoolean(bool? v) {
            if(!v.HasValue)
                return;
            byte b = v.Value ? (byte)1 : (byte)0;
            Filter.Insert(new byte[] { b });
        }

        /// <summary>Insert a nullable Int32 (PLAIN little-endian 4 bytes).</summary>
        public void AddInt32(int? v) {
            if(!v.HasValue)
                return;
            Span<byte> buf = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(buf, v.Value);
            Filter.Insert(buf.ToArray());
        }

        /// <summary>Insert a nullable Int64 (PLAIN little-endian 8 bytes).</summary>
        public void AddInt64(long? v) {
            if(!v.HasValue)
                return;
            Span<byte> buf = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(buf, v.Value);
            Filter.Insert(buf.ToArray());
        }

        public void AddInt96(DateTime? v) {
            if(!v.HasValue)
                return;

            // Parquet INT96 is a 12-byte little-endian value consisting of:
            // - first 8 bytes: nanoseconds since midnight (little-endian)
            // - next 4 bytes: Julian day (little-endian)

            DateTime dt = v.Value.ToUniversalTime();
            int julianDay = (int)(dt - new DateTime(4713, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalDays + 1721424; // Convert to Julian Day
            long nanosSinceMidnight = (long)(dt - dt.Date).TotalMilliseconds * 1_000_000; // Convert to nanoseconds since midnight

            byte[] buf = new byte[12];
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(0, 8), nanosSinceMidnight);
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(8, 4), julianDay);

            Filter.Insert(buf);
        }

        /// <summary>Insert a nullable float (PLAIN little-endian 4 bytes, IEEE 754).</summary>
        public void AddFloat(float? v) {
            if(!v.HasValue)
                return;

            byte[] buf = new byte[4];
            Buffer.BlockCopy(new float[] { v.Value }, 0, buf, 0, 4);
            if(!BitConverter.IsLittleEndian) {
                Array.Reverse(buf);
            }
            Filter.Insert(buf);
        }

        /// <summary>Insert a nullable double (PLAIN little-endian 8 bytes, IEEE 754).</summary>
        public void AddDouble(double? v) {
            if(!v.HasValue)
                return;

            byte[] buf = new byte[8];
            Buffer.BlockCopy(new double[] { v.Value }, 0, buf, 0, 8);
            if(!BitConverter.IsLittleEndian) {
                Array.Reverse(buf);
            }
            Filter.Insert(buf);
        }

        /// <summary>Insert a UTF-8 string (PLAIN: length prefix is NOT included for bloom hashing).</summary>
        public void AddString(string? s) {
            if(s == null)
                return;
            byte[] utf8 = Encoding.UTF8.GetBytes(s);
            Filter.Insert(utf8);
        }

        /// <summary>Insert a BYTE_ARRAY slice (PLAIN hashing uses the bytes only; no length prefix).</summary>
        public void AddByteArray(byte[]? bytes) {
            if(bytes == null)
                return;
            Filter.Insert(bytes);
        }

        /// <summary>Insert FIXED_LEN_BYTE_ARRAY (bytes as-is).</summary>
        public void AddFixed(byte[]? bytes) {
            if(bytes == null)
                return;
            Filter.Insert(bytes);
        }
    }
}