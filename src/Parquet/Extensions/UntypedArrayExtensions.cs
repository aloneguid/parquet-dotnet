






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
            
            throw new NotSupportedException($"nullable type {t} is not supported");
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

    // todo

    #endregion

    #region [ Null Unpacking ]

    // todo

    #endregion

    }
}