


using System;
using System.Numerics;
using Parquet.Schema;
using Parquet.File.Values.Primitives;
using SType = System.Type;

namespace Parquet.Encodings {
    static class SchemaMapper {
        [Obsolete]
        public static DataType? FindDataType(SType type) {
            if(type == typeof(bool)) return DataType.Boolean;
            if(type == typeof(byte)) return DataType.Byte;
            if(type == typeof(sbyte)) return DataType.SignedByte;
            if(type == typeof(short)) return DataType.Int16;
            if(type == typeof(ushort)) return DataType.UnsignedInt16;
            if(type == typeof(int)) return DataType.Int32;
            if(type == typeof(uint)) return DataType.UnsignedInt32;
            if(type == typeof(long)) return DataType.Int64;
            if(type == typeof(ulong)) return DataType.UnsignedInt64;
            if(type == typeof(BigInteger)) return DataType.Int96;
            if(type == typeof(byte[])) return DataType.ByteArray;
            if(type == typeof(string)) return DataType.String;
            if(type == typeof(float)) return DataType.Float;
            if(type == typeof(double)) return DataType.Double;
            if(type == typeof(decimal)) return DataType.Decimal;
            if(type == typeof(DateTime)) return DataType.DateTimeOffset;
            if(type == typeof(Interval)) return DataType.Interval;
            if(type == typeof(TimeSpan)) return DataType.TimeSpan;
            return null;
        }

        [Obsolete]
        public static SType? FindSystemType(DataType type) {
            if(type == DataType.Boolean) return typeof(bool);
            if(type == DataType.Byte) return typeof(byte);
            if(type == DataType.SignedByte) return typeof(sbyte);
            if(type == DataType.Int16) return typeof(short);
            if(type == DataType.UnsignedInt16) return typeof(ushort);
            if(type == DataType.Int32) return typeof(int);
            if(type == DataType.UnsignedInt32) return typeof(uint);
            if(type == DataType.Int64) return typeof(long);
            if(type == DataType.UnsignedInt64) return typeof(ulong);
            if(type == DataType.Int96) return typeof(BigInteger);
            if(type == DataType.ByteArray) return typeof(byte[]);
            if(type == DataType.String) return typeof(string);
            if(type == DataType.Float) return typeof(float);
            if(type == DataType.Double) return typeof(double);
            if(type == DataType.Decimal) return typeof(decimal);
            if(type == DataType.DateTimeOffset) return typeof(DateTime);
            if(type == DataType.Interval) return typeof(Interval);
            if(type == DataType.TimeSpan) return typeof(TimeSpan);
            return null;
        }
    }
}