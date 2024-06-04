// This file is generated with T4
// https://learn.microsoft.com/en-us/visualstudio/modeling/text-template-control-blocks?view=vs-2022
// Because of this, performance is truly great!
// Hint: prefer Rider to edit .tt as it support syntax highlighting
namespace Parquet.Extensions {

    using System;
    using System.Numerics;
    using Parquet.File.Values.Primitives;

    static class UntypedArrayExtensions {

        #region [ Null Counting ]

        public static int CalculateNullCountFast(this Array array, int offset, int count) {
            Type? t = array.GetType().GetElementType();
            if(t != null && !t.IsNullable()) return 0;

            if(t == typeof(bool?)) {
                return CalculateNullCount((bool?[])array, offset, count);
            }
            if(t == typeof(byte?)) {
                return CalculateNullCount((byte?[])array, offset, count);
            }
            if(t == typeof(sbyte?)) {
                return CalculateNullCount((sbyte?[])array, offset, count);
            }
            if(t == typeof(short?)) {
                return CalculateNullCount((short?[])array, offset, count);
            }
            if(t == typeof(ushort?)) {
                return CalculateNullCount((ushort?[])array, offset, count);
            }
            if(t == typeof(int?)) {
                return CalculateNullCount((int?[])array, offset, count);
            }
            if(t == typeof(uint?)) {
                return CalculateNullCount((uint?[])array, offset, count);
            }
            if(t == typeof(long?)) {
                return CalculateNullCount((long?[])array, offset, count);
            }
            if(t == typeof(ulong?)) {
                return CalculateNullCount((ulong?[])array, offset, count);
            }
            if(t == typeof(BigInteger?)) {
                return CalculateNullCount((BigInteger?[])array, offset, count);
            }
            if(t == typeof(float?)) {
                return CalculateNullCount((float?[])array, offset, count);
            }
            if(t == typeof(double?)) {
                return CalculateNullCount((double?[])array, offset, count);
            }
            if(t == typeof(decimal?)) {
                return CalculateNullCount((decimal?[])array, offset, count);
            }
            if(t == typeof(DateTime?)) {
                return CalculateNullCount((DateTime?[])array, offset, count);
            }
            if(t == typeof(TimeSpan?)) {
                return CalculateNullCount((TimeSpan?[])array, offset, count);
            }
            if(t == typeof(Interval?)) {
                return CalculateNullCount((Interval?[])array, offset, count);
            }
            if(t == typeof(string)) {
                return CalculateNullCount((string[])array, offset, count);
            }
            if(t == typeof(byte[])) {
                return CalculateNullCount((byte[][])array, offset, count);
            }
            if(t == typeof(Guid?)) {
                return CalculateNullCount((Guid?[])array, offset, count);
            }
#if NET6_0_OR_GREATER
            if(t == typeof(DateOnly?)) {
                return CalculateNullCount((DateOnly?[])array, offset, count);
            }
            if(t == typeof(TimeOnly?)) {
                return CalculateNullCount((TimeOnly?[])array, offset, count);
            }
#endif            
            throw new NotSupportedException($"cannot count nulls in type {t}");
        }

        private static int CalculateNullCount(bool?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(byte?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(sbyte?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(short?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(ushort?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(int?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(uint?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(long?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(ulong?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(BigInteger?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(float?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(double?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(decimal?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(DateTime?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(TimeSpan?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(Interval?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(string[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(byte[][] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(Guid?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
#if NET6_0_OR_GREATER
        private static int CalculateNullCount(DateOnly?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
        private static int CalculateNullCount(TimeOnly?[] array, int offset, int count) {
            int r = 0;
            for(int i = offset; i < count; i++) {
                if(array[i] == null) {
                    r++;
                }
            }
            return r;
        }
#endif
    #endregion

    #region [ Null Packing ]

    public static void PackNullsFast(this Array array,
            int offset, int count,
            Array packedData,
            Span<int> dest,
            int fillerValue) {

            Type? t = array.GetType().GetElementType();
            if(t == null)
                throw new ArgumentException("cannot detect element type", nameof(array));

            if(!t.IsNullable()) {
                Array.Copy(array, offset, packedData, 0, count);
                dest.Fill(fillerValue);
                return;
            }

            if(t == typeof(bool?)) {
                PackNullsTypeFast((bool?[])array,
                    offset, count,
                    (bool[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(byte?)) {
                PackNullsTypeFast((byte?[])array,
                    offset, count,
                    (byte[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(sbyte?)) {
                PackNullsTypeFast((sbyte?[])array,
                    offset, count,
                    (sbyte[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(short?)) {
                PackNullsTypeFast((short?[])array,
                    offset, count,
                    (short[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(ushort?)) {
                PackNullsTypeFast((ushort?[])array,
                    offset, count,
                    (ushort[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(int?)) {
                PackNullsTypeFast((int?[])array,
                    offset, count,
                    (int[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(uint?)) {
                PackNullsTypeFast((uint?[])array,
                    offset, count,
                    (uint[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(long?)) {
                PackNullsTypeFast((long?[])array,
                    offset, count,
                    (long[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(ulong?)) {
                PackNullsTypeFast((ulong?[])array,
                    offset, count,
                    (ulong[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(BigInteger?)) {
                PackNullsTypeFast((BigInteger?[])array,
                    offset, count,
                    (BigInteger[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(float?)) {
                PackNullsTypeFast((float?[])array,
                    offset, count,
                    (float[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(double?)) {
                PackNullsTypeFast((double?[])array,
                    offset, count,
                    (double[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(decimal?)) {
                PackNullsTypeFast((decimal?[])array,
                    offset, count,
                    (decimal[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(DateTime?)) {
                PackNullsTypeFast((DateTime?[])array,
                    offset, count,
                    (DateTime[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(TimeSpan?)) {
                PackNullsTypeFast((TimeSpan?[])array,
                    offset, count,
                    (TimeSpan[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(Interval?)) {
                PackNullsTypeFast((Interval?[])array,
                    offset, count,
                    (Interval[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(string)) {
                PackNullsTypeFast((string[])array,
                    offset, count,
                    (string[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(byte[])) {
                PackNullsTypeFast((byte[][])array,
                    offset, count,
                    (byte[][])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(Guid?)) {
                PackNullsTypeFast((Guid?[])array,
                    offset, count,
                    (Guid[])packedData,
                    dest, fillerValue);
                return;
            }
 
#if NET6_0_OR_GREATER
            if(t == typeof(DateOnly?)) {
                PackNullsTypeFast((DateOnly?[])array,
                    offset, count,
                    (DateOnly[])packedData,
                    dest, fillerValue);
                return;
            }
            if(t == typeof(TimeOnly?)) {
                PackNullsTypeFast((TimeOnly?[])array,
                    offset, count,
                    (TimeOnly[])packedData,
                    dest, fillerValue);
                return;
            }
#endif
            throw new NotSupportedException($"cannot pack type {t}");
        }

        private static void PackNullsTypeFast(bool?[] array,
            int offset, int count,
            bool[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                bool? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (bool)value;
                }
            }
        }

        private static void PackNullsTypeFast(byte?[] array,
            int offset, int count,
            byte[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                byte? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (byte)value;
                }
            }
        }

        private static void PackNullsTypeFast(sbyte?[] array,
            int offset, int count,
            sbyte[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                sbyte? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (sbyte)value;
                }
            }
        }

        private static void PackNullsTypeFast(short?[] array,
            int offset, int count,
            short[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                short? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (short)value;
                }
            }
        }

        private static void PackNullsTypeFast(ushort?[] array,
            int offset, int count,
            ushort[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                ushort? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (ushort)value;
                }
            }
        }

        private static void PackNullsTypeFast(int?[] array,
            int offset, int count,
            int[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                int? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (int)value;
                }
            }
        }

        private static void PackNullsTypeFast(uint?[] array,
            int offset, int count,
            uint[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                uint? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (uint)value;
                }
            }
        }

        private static void PackNullsTypeFast(long?[] array,
            int offset, int count,
            long[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                long? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (long)value;
                }
            }
        }

        private static void PackNullsTypeFast(ulong?[] array,
            int offset, int count,
            ulong[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                ulong? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (ulong)value;
                }
            }
        }

        private static void PackNullsTypeFast(BigInteger?[] array,
            int offset, int count,
            BigInteger[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                BigInteger? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (BigInteger)value;
                }
            }
        }

        private static void PackNullsTypeFast(float?[] array,
            int offset, int count,
            float[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                float? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (float)value;
                }
            }
        }

        private static void PackNullsTypeFast(double?[] array,
            int offset, int count,
            double[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                double? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (double)value;
                }
            }
        }

        private static void PackNullsTypeFast(decimal?[] array,
            int offset, int count,
            decimal[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                decimal? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (decimal)value;
                }
            }
        }

        private static void PackNullsTypeFast(DateTime?[] array,
            int offset, int count,
            DateTime[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                DateTime? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (DateTime)value;
                }
            }
        }

        private static void PackNullsTypeFast(TimeSpan?[] array,
            int offset, int count,
            TimeSpan[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                TimeSpan? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (TimeSpan)value;
                }
            }
        }

        private static void PackNullsTypeFast(Interval?[] array,
            int offset, int count,
            Interval[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                Interval? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (Interval)value;
                }
            }
        }

        private static void PackNullsTypeFast(string[] array,
            int offset, int count,
            string[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                string value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (string)value;
                }
            }
        }

        private static void PackNullsTypeFast(byte[][] array,
            int offset, int count,
            byte[][] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                byte[] value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (byte[])value;
                }
            }
        }

        private static void PackNullsTypeFast(Guid?[] array,
            int offset, int count,
            Guid[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                Guid? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (Guid)value;
                }
            }
        }

#if NET6_0_OR_GREATER
        private static void PackNullsTypeFast(DateOnly?[] array,
            int offset, int count,
            DateOnly[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                DateOnly? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (DateOnly)value;
                }
            }
        }

        private static void PackNullsTypeFast(TimeOnly?[] array,
            int offset, int count,
            TimeOnly[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                TimeOnly? value = array[i];

                if(value == null) {
                    dest[y] = fillerValue - 1;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = (TimeOnly)value;
                }
            }
        }

#endif
    #endregion

    #region [ Null Unpacking ]

    public static void UnpackNullsFast(this Array array,
        Span<int> flags, int fillFlag,
        Array result) {

        Type? t = array.GetType().GetElementType();
        if(t == null)
            throw new ArgumentException("cannot detect element type", nameof(array));

        
        if(t == typeof(bool)) {
            UnpackNullsTypeFast((bool[])array,
                flags, fillFlag,
                (bool?[])result);
            return;
        }
        if(t == typeof(byte)) {
            UnpackNullsTypeFast((byte[])array,
                flags, fillFlag,
                (byte?[])result);
            return;
        }
        if(t == typeof(sbyte)) {
            UnpackNullsTypeFast((sbyte[])array,
                flags, fillFlag,
                (sbyte?[])result);
            return;
        }
        if(t == typeof(short)) {
            UnpackNullsTypeFast((short[])array,
                flags, fillFlag,
                (short?[])result);
            return;
        }
        if(t == typeof(ushort)) {
            UnpackNullsTypeFast((ushort[])array,
                flags, fillFlag,
                (ushort?[])result);
            return;
        }
        if(t == typeof(int)) {
            UnpackNullsTypeFast((int[])array,
                flags, fillFlag,
                (int?[])result);
            return;
        }
        if(t == typeof(uint)) {
            UnpackNullsTypeFast((uint[])array,
                flags, fillFlag,
                (uint?[])result);
            return;
        }
        if(t == typeof(long)) {
            UnpackNullsTypeFast((long[])array,
                flags, fillFlag,
                (long?[])result);
            return;
        }
        if(t == typeof(ulong)) {
            UnpackNullsTypeFast((ulong[])array,
                flags, fillFlag,
                (ulong?[])result);
            return;
        }
        if(t == typeof(BigInteger)) {
            UnpackNullsTypeFast((BigInteger[])array,
                flags, fillFlag,
                (BigInteger?[])result);
            return;
        }
        if(t == typeof(float)) {
            UnpackNullsTypeFast((float[])array,
                flags, fillFlag,
                (float?[])result);
            return;
        }
        if(t == typeof(double)) {
            UnpackNullsTypeFast((double[])array,
                flags, fillFlag,
                (double?[])result);
            return;
        }
        if(t == typeof(decimal)) {
            UnpackNullsTypeFast((decimal[])array,
                flags, fillFlag,
                (decimal?[])result);
            return;
        }
        if(t == typeof(DateTime)) {
            UnpackNullsTypeFast((DateTime[])array,
                flags, fillFlag,
                (DateTime?[])result);
            return;
        }
        if(t == typeof(TimeSpan)) {
            UnpackNullsTypeFast((TimeSpan[])array,
                flags, fillFlag,
                (TimeSpan?[])result);
            return;
        }
        if(t == typeof(Interval)) {
            UnpackNullsTypeFast((Interval[])array,
                flags, fillFlag,
                (Interval?[])result);
            return;
        }
        if(t == typeof(string)) {
            UnpackNullsTypeFast((string[])array,
                flags, fillFlag,
                (string[])result);
            return;
        }
        if(t == typeof(byte[])) {
            UnpackNullsTypeFast((byte[][])array,
                flags, fillFlag,
                (byte[][])result);
            return;
        }
        if(t == typeof(Guid)) {
            UnpackNullsTypeFast((Guid[])array,
                flags, fillFlag,
                (Guid?[])result);
            return;
        }
#if NET6_0_OR_GREATER
        if(t == typeof(DateOnly)) {
            UnpackNullsTypeFast((DateOnly[])array,
                flags, fillFlag,
                (DateOnly?[])result);
            return;
        }
        if(t == typeof(TimeOnly)) {
            UnpackNullsTypeFast((TimeOnly[])array,
                flags, fillFlag,
                (TimeOnly?[])result);
            return;
        }
#endif            
        throw new NotSupportedException($"cannot pack type {t}");

    }

    private static void UnpackNullsTypeFast(bool[] array,
        Span<int> flags, int fillFlag,
        bool?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(byte[] array,
        Span<int> flags, int fillFlag,
        byte?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(sbyte[] array,
        Span<int> flags, int fillFlag,
        sbyte?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(short[] array,
        Span<int> flags, int fillFlag,
        short?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(ushort[] array,
        Span<int> flags, int fillFlag,
        ushort?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(int[] array,
        Span<int> flags, int fillFlag,
        int?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(uint[] array,
        Span<int> flags, int fillFlag,
        uint?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(long[] array,
        Span<int> flags, int fillFlag,
        long?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(ulong[] array,
        Span<int> flags, int fillFlag,
        ulong?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(BigInteger[] array,
        Span<int> flags, int fillFlag,
        BigInteger?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(float[] array,
        Span<int> flags, int fillFlag,
        float?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(double[] array,
        Span<int> flags, int fillFlag,
        double?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(decimal[] array,
        Span<int> flags, int fillFlag,
        decimal?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(DateTime[] array,
        Span<int> flags, int fillFlag,
        DateTime?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(TimeSpan[] array,
        Span<int> flags, int fillFlag,
        TimeSpan?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(Interval[] array,
        Span<int> flags, int fillFlag,
        Interval?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(string[] array,
        Span<int> flags, int fillFlag,
        string[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(byte[][] array,
        Span<int> flags, int fillFlag,
        byte[][] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(Guid[] array,
        Span<int> flags, int fillFlag,
        Guid?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

#if NET6_0_OR_GREATER
    private static void UnpackNullsTypeFast(DateOnly[] array,
        Span<int> flags, int fillFlag,
        DateOnly?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

    private static void UnpackNullsTypeFast(TimeOnly[] array,
        Span<int> flags, int fillFlag,
        TimeOnly?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }

#endif
    #endregion

    #region [ Dictionary Explosion ]

    public static void ExplodeFast(this Array dictionary,
            Span<int> indexes,
            Array result, int resultOffset, int resultCount) {
        Type? t = dictionary.GetType().GetElementType();
        if(t == null)
            throw new ArgumentException("cannot detect element type", nameof(dictionary));

        if(t == typeof(bool)) {
            ExplodeTypeFast((bool[])dictionary,
                indexes, (bool[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(byte)) {
            ExplodeTypeFast((byte[])dictionary,
                indexes, (byte[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(sbyte)) {
            ExplodeTypeFast((sbyte[])dictionary,
                indexes, (sbyte[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(short)) {
            ExplodeTypeFast((short[])dictionary,
                indexes, (short[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(ushort)) {
            ExplodeTypeFast((ushort[])dictionary,
                indexes, (ushort[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(int)) {
            ExplodeTypeFast((int[])dictionary,
                indexes, (int[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(uint)) {
            ExplodeTypeFast((uint[])dictionary,
                indexes, (uint[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(long)) {
            ExplodeTypeFast((long[])dictionary,
                indexes, (long[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(ulong)) {
            ExplodeTypeFast((ulong[])dictionary,
                indexes, (ulong[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(BigInteger)) {
            ExplodeTypeFast((BigInteger[])dictionary,
                indexes, (BigInteger[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(float)) {
            ExplodeTypeFast((float[])dictionary,
                indexes, (float[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(double)) {
            ExplodeTypeFast((double[])dictionary,
                indexes, (double[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(decimal)) {
            ExplodeTypeFast((decimal[])dictionary,
                indexes, (decimal[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(DateTime)) {
            ExplodeTypeFast((DateTime[])dictionary,
                indexes, (DateTime[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(TimeSpan)) {
            ExplodeTypeFast((TimeSpan[])dictionary,
                indexes, (TimeSpan[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(Interval)) {
            ExplodeTypeFast((Interval[])dictionary,
                indexes, (Interval[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(string)) {
            ExplodeTypeFast((string[])dictionary,
                indexes, (string[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(byte[])) {
            ExplodeTypeFast((byte[][])dictionary,
                indexes, (byte[][])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(Guid)) {
            ExplodeTypeFast((Guid[])dictionary,
                indexes, (Guid[])result, resultOffset, resultCount);
            return;
        }
#if NET6_0_OR_GREATER
        if(t == typeof(DateOnly)) {
            ExplodeTypeFast((DateOnly[])dictionary,
                indexes, (DateOnly[])result, resultOffset, resultCount);
            return;
        }
        if(t == typeof(TimeOnly)) {
            ExplodeTypeFast((TimeOnly[])dictionary,
                indexes, (TimeOnly[])result, resultOffset, resultCount);
            return;
        }
#endif            
        throw new NotSupportedException($"cannot pack type {t}");
    }

    private static void ExplodeTypeFast(bool[] dictionary,
        Span<int> indexes,
        bool[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(byte[] dictionary,
        Span<int> indexes,
        byte[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(sbyte[] dictionary,
        Span<int> indexes,
        sbyte[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(short[] dictionary,
        Span<int> indexes,
        short[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(ushort[] dictionary,
        Span<int> indexes,
        ushort[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(int[] dictionary,
        Span<int> indexes,
        int[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(uint[] dictionary,
        Span<int> indexes,
        uint[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(long[] dictionary,
        Span<int> indexes,
        long[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(ulong[] dictionary,
        Span<int> indexes,
        ulong[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(BigInteger[] dictionary,
        Span<int> indexes,
        BigInteger[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(float[] dictionary,
        Span<int> indexes,
        float[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(double[] dictionary,
        Span<int> indexes,
        double[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(decimal[] dictionary,
        Span<int> indexes,
        decimal[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(DateTime[] dictionary,
        Span<int> indexes,
        DateTime[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(TimeSpan[] dictionary,
        Span<int> indexes,
        TimeSpan[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(Interval[] dictionary,
        Span<int> indexes,
        Interval[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(string[] dictionary,
        Span<int> indexes,
        string[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(byte[][] dictionary,
        Span<int> indexes,
        byte[][] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(Guid[] dictionary,
        Span<int> indexes,
        Guid[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

#if NET6_0_OR_GREATER
    private static void ExplodeTypeFast(DateOnly[] dictionary,
        Span<int> indexes,
        DateOnly[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

    private static void ExplodeTypeFast(TimeOnly[] dictionary,
        Span<int> indexes,
        TimeOnly[] result, int resultOffset, int resultCount) {

        for(int i = 0; i < resultCount; i++) {
            int index = indexes[i];
            if(index < dictionary.Length) {
                // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                Array.Copy(dictionary, index, result, resultOffset + i, 1);
            }
        }
    }

#endif
    #endregion

    }
}