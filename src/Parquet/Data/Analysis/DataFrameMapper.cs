using System;
using System.Numerics;
using Microsoft.Data.Analysis;

namespace Parquet.Data.Analysis {
    static class DataFrameMapper {
        public static DataFrameColumn ToDataFrameColumn(DataColumn dc) {
            
            if(dc.Field.ClrType == typeof(bool)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<bool>(dc.Field.Name, (bool[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<bool>(dc.Field.Name, (bool?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(int)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<int>(dc.Field.Name, (int[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<int>(dc.Field.Name, (int?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(uint)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<uint>(dc.Field.Name, (uint[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<uint>(dc.Field.Name, (uint?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(long)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<long>(dc.Field.Name, (long[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<long>(dc.Field.Name, (long?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(ulong)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<ulong>(dc.Field.Name, (ulong[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<ulong>(dc.Field.Name, (ulong?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(byte)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<byte>(dc.Field.Name, (byte[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<byte>(dc.Field.Name, (byte?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(sbyte)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<sbyte>(dc.Field.Name, (sbyte[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<sbyte>(dc.Field.Name, (sbyte?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(DateTime)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<DateTime>(dc.Field.Name, (DateTime[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<DateTime>(dc.Field.Name, (DateTime?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(TimeSpan)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<TimeSpan>(dc.Field.Name, (TimeSpan[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<TimeSpan>(dc.Field.Name, (TimeSpan?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(decimal)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<decimal>(dc.Field.Name, (decimal[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<decimal>(dc.Field.Name, (decimal?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(float)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<float>(dc.Field.Name, (float[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<float>(dc.Field.Name, (float?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(double)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<double>(dc.Field.Name, (double[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<double>(dc.Field.Name, (double?[])dc.Data);
                }
            }
            
            // special case
            if(dc.Field.ClrType == typeof(string)) {
                return new StringDataFrameColumn(dc.Field.Name, (string[])dc.Data);
            }

            throw new NotSupportedException(dc.Field.ClrType.Name);
        }
    }
}