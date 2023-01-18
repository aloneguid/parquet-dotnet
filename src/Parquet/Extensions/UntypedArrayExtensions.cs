






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
            Type t = array.GetType().GetElementType();
            if(!t.IsNullable()) return 0;


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

    #endregion

    #region [ Null Packing ]

    public static void PackNullsFast(this Array array,
            int offset, int count,
            Array packedData,
            Span<int> dest,
            int fillerValue) {

            Type t = array.GetType().GetElementType();
            if(!t.IsNullable()) return;


            if(t == typeof(bool?)) {
                PackNulls((bool?[])array,
                    offset, count,
                    (bool?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(byte?)) {
                PackNulls((byte?[])array,
                    offset, count,
                    (byte?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(sbyte?)) {
                PackNulls((sbyte?[])array,
                    offset, count,
                    (sbyte?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(short?)) {
                PackNulls((short?[])array,
                    offset, count,
                    (short?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(ushort?)) {
                PackNulls((ushort?[])array,
                    offset, count,
                    (ushort?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(int?)) {
                PackNulls((int?[])array,
                    offset, count,
                    (int?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(uint?)) {
                PackNulls((uint?[])array,
                    offset, count,
                    (uint?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(long?)) {
                PackNulls((long?[])array,
                    offset, count,
                    (long?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(ulong?)) {
                PackNulls((ulong?[])array,
                    offset, count,
                    (ulong?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(BigInteger?)) {
                PackNulls((BigInteger?[])array,
                    offset, count,
                    (BigInteger?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(float?)) {
                PackNulls((float?[])array,
                    offset, count,
                    (float?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(double?)) {
                PackNulls((double?[])array,
                    offset, count,
                    (double?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(decimal?)) {
                PackNulls((decimal?[])array,
                    offset, count,
                    (decimal?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(DateTime?)) {
                PackNulls((DateTime?[])array,
                    offset, count,
                    (DateTime?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(TimeSpan?)) {
                PackNulls((TimeSpan?[])array,
                    offset, count,
                    (TimeSpan?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(Interval?)) {
                PackNulls((Interval?[])array,
                    offset, count,
                    (Interval?[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(string)) {
                PackNulls((string[])array,
                    offset, count,
                    (string[])packedData,
                    dest, fillerValue);
            }

            if(t == typeof(byte[])) {
                PackNulls((byte[][])array,
                    offset, count,
                    (byte[][])packedData,
                    dest, fillerValue);
            }
            
            throw new NotSupportedException($"cannot pack type {t}");
        }


        private static void PackNulls(bool?[] array,
            int offset, int count,
            bool?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                bool? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(byte?[] array,
            int offset, int count,
            byte?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                byte? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(sbyte?[] array,
            int offset, int count,
            sbyte?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                sbyte? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(short?[] array,
            int offset, int count,
            short?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                short? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(ushort?[] array,
            int offset, int count,
            ushort?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                ushort? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(int?[] array,
            int offset, int count,
            int?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                int? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(uint?[] array,
            int offset, int count,
            uint?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                uint? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(long?[] array,
            int offset, int count,
            long?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                long? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(ulong?[] array,
            int offset, int count,
            ulong?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                ulong? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(BigInteger?[] array,
            int offset, int count,
            BigInteger?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                BigInteger? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(float?[] array,
            int offset, int count,
            float?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                float? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(double?[] array,
            int offset, int count,
            double?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                double? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(decimal?[] array,
            int offset, int count,
            decimal?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                decimal? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(DateTime?[] array,
            int offset, int count,
            DateTime?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                DateTime? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(TimeSpan?[] array,
            int offset, int count,
            TimeSpan?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                TimeSpan? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(Interval?[] array,
            int offset, int count,
            Interval?[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                Interval? value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(string[] array,
            int offset, int count,
            string[] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                string value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }


        private static void PackNulls(byte[][] array,
            int offset, int count,
            byte[][] packedArray,
            Span<int> dest,
            int fillerValue) {

            for(int i = offset, y = 0, ir = 0; i < (offset + count); i++, y++) {
                byte[] value = array[i];

                if(value == null) {
                    dest[y] = 0;
                }
                else {
                    dest[y] = fillerValue;
                    packedArray[ir++] = value;
                }
            }
        }



    #endregion

    #region [ Null Unpacking ]

    public static void UnpackNullsFast(this Array array,
        int[] flags, int fillFlag,
        Array result) {

        Type t = array.GetType().GetElementType();
        


        if(t == typeof(bool)) {
            UnpackNulls((bool[])array,
                flags, fillFlag,
                (bool?[])result);
            return;
        }


        if(t == typeof(byte)) {
            UnpackNulls((byte[])array,
                flags, fillFlag,
                (byte?[])result);
            return;
        }


        if(t == typeof(sbyte)) {
            UnpackNulls((sbyte[])array,
                flags, fillFlag,
                (sbyte?[])result);
            return;
        }


        if(t == typeof(short)) {
            UnpackNulls((short[])array,
                flags, fillFlag,
                (short?[])result);
            return;
        }


        if(t == typeof(ushort)) {
            UnpackNulls((ushort[])array,
                flags, fillFlag,
                (ushort?[])result);
            return;
        }


        if(t == typeof(int)) {
            UnpackNulls((int[])array,
                flags, fillFlag,
                (int?[])result);
            return;
        }


        if(t == typeof(uint)) {
            UnpackNulls((uint[])array,
                flags, fillFlag,
                (uint?[])result);
            return;
        }


        if(t == typeof(long)) {
            UnpackNulls((long[])array,
                flags, fillFlag,
                (long?[])result);
            return;
        }


        if(t == typeof(ulong)) {
            UnpackNulls((ulong[])array,
                flags, fillFlag,
                (ulong?[])result);
            return;
        }


        if(t == typeof(BigInteger)) {
            UnpackNulls((BigInteger[])array,
                flags, fillFlag,
                (BigInteger?[])result);
            return;
        }


        if(t == typeof(float)) {
            UnpackNulls((float[])array,
                flags, fillFlag,
                (float?[])result);
            return;
        }


        if(t == typeof(double)) {
            UnpackNulls((double[])array,
                flags, fillFlag,
                (double?[])result);
            return;
        }


        if(t == typeof(decimal)) {
            UnpackNulls((decimal[])array,
                flags, fillFlag,
                (decimal?[])result);
            return;
        }


        if(t == typeof(DateTime)) {
            UnpackNulls((DateTime[])array,
                flags, fillFlag,
                (DateTime?[])result);
            return;
        }


        if(t == typeof(TimeSpan)) {
            UnpackNulls((TimeSpan[])array,
                flags, fillFlag,
                (TimeSpan?[])result);
            return;
        }


        if(t == typeof(Interval)) {
            UnpackNulls((Interval[])array,
                flags, fillFlag,
                (Interval?[])result);
            return;
        }


        if(t == typeof(string)) {
            UnpackNulls((string[])array,
                flags, fillFlag,
                (string[])result);
            return;
        }


        if(t == typeof(byte[])) {
            UnpackNulls((byte[][])array,
                flags, fillFlag,
                (byte[][])result);
            return;
        }
            
        throw new NotSupportedException($"cannot pack type {t}");

    }



    private static void UnpackNulls(bool[] array,
        int[] flags, int fillFlag,
        bool?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(byte[] array,
        int[] flags, int fillFlag,
        byte?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(sbyte[] array,
        int[] flags, int fillFlag,
        sbyte?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(short[] array,
        int[] flags, int fillFlag,
        short?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(ushort[] array,
        int[] flags, int fillFlag,
        ushort?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(int[] array,
        int[] flags, int fillFlag,
        int?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(uint[] array,
        int[] flags, int fillFlag,
        uint?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(long[] array,
        int[] flags, int fillFlag,
        long?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(ulong[] array,
        int[] flags, int fillFlag,
        ulong?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(BigInteger[] array,
        int[] flags, int fillFlag,
        BigInteger?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(float[] array,
        int[] flags, int fillFlag,
        float?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(double[] array,
        int[] flags, int fillFlag,
        double?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(decimal[] array,
        int[] flags, int fillFlag,
        decimal?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(DateTime[] array,
        int[] flags, int fillFlag,
        DateTime?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(TimeSpan[] array,
        int[] flags, int fillFlag,
        TimeSpan?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(Interval[] array,
        int[] flags, int fillFlag,
        Interval?[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(string[] array,
        int[] flags, int fillFlag,
        string[] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    private static void UnpackNulls(byte[][] array,
        int[] flags, int fillFlag,
        byte[][] result) {

        int iarray = 0;
        for(int i = 0; i < flags.Length; i++) {
            int level = flags[i];

            if(level == fillFlag) {
                result[i] = array[iarray++];
            }
        }
    }



    #endregion

    #region [ Dictionary Explosion ]

    public static void ExplodeFast(this Array dictionary,
            Span<int> indexes,
            Array result, int resultOffset, int resultCount) {

    }

    #endregion

    }
}