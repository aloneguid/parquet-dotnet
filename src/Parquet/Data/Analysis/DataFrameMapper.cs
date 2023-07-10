using System;
using System.Linq;
using System.Numerics;
using Microsoft.Data.Analysis;

namespace Parquet.Data.Analysis {
    static class DataFrameMapper {
        public static DataFrameColumn ToDataFrameColumn(DataColumn dc) {
            string colName = string.Join("_", dc.Field.Path.ToList());

            if(dc.Field.ClrType == typeof(bool)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<bool>(colName, (bool[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<bool>(colName, (bool?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(int)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<int>(colName, (int[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<int>(colName, (int?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(uint)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<uint>(colName, (uint[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<uint>(colName, (uint?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(long)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<long>(colName, (long[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<long>(colName, (long?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(ulong)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<ulong>(colName, (ulong[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<ulong>(colName, (ulong?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(byte)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<byte>(colName, (byte[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<byte>(colName, (byte?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(sbyte)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<sbyte>(colName, (sbyte[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<sbyte>(colName, (sbyte?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(DateTime)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<DateTime>(colName, (DateTime[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<DateTime>(colName, (DateTime?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(TimeSpan)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<TimeSpan>(colName, (TimeSpan[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<TimeSpan>(colName, (TimeSpan?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(decimal)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<decimal>(colName, (decimal[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<decimal>(colName, (decimal?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(float)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<float>(colName, (float[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<float>(colName, (float?[])dc.Data);
                }
            }
            if(dc.Field.ClrType == typeof(double)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<double>(colName, (double[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<double>(colName, (double?[])dc.Data);
                }
            }
                        // special case
            if(dc.Field.ClrType == typeof(string)) {
                return new StringDataFrameColumn(colName, (string[])dc.Data);
            }

            throw new NotSupportedException(dc.Field.ClrType.Name);
        }

        public static void AppendValues(DataFrameColumn dfc, DataColumn dc) {
            if(dc.Field.ClrType == typeof(bool)) {
                var tdfc = (PrimitiveDataFrameColumn<bool>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(bool el in (bool[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(bool? el in (bool?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(int)) {
                var tdfc = (PrimitiveDataFrameColumn<int>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(int el in (int[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(int? el in (int?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(uint)) {
                var tdfc = (PrimitiveDataFrameColumn<uint>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(uint el in (uint[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(uint? el in (uint?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(long)) {
                var tdfc = (PrimitiveDataFrameColumn<long>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(long el in (long[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(long? el in (long?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(ulong)) {
                var tdfc = (PrimitiveDataFrameColumn<ulong>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(ulong el in (ulong[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(ulong? el in (ulong?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(byte)) {
                var tdfc = (PrimitiveDataFrameColumn<byte>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(byte el in (byte[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(byte? el in (byte?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(sbyte)) {
                var tdfc = (PrimitiveDataFrameColumn<sbyte>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(sbyte el in (sbyte[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(sbyte? el in (sbyte?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(DateTime)) {
                var tdfc = (PrimitiveDataFrameColumn<DateTime>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(DateTime el in (DateTime[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(DateTime? el in (DateTime?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(TimeSpan)) {
                var tdfc = (PrimitiveDataFrameColumn<TimeSpan>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(TimeSpan el in (TimeSpan[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(TimeSpan? el in (TimeSpan?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(decimal)) {
                var tdfc = (PrimitiveDataFrameColumn<decimal>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(decimal el in (decimal[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(decimal? el in (decimal?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(float)) {
                var tdfc = (PrimitiveDataFrameColumn<float>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(float el in (float[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(float? el in (float?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
            if(dc.Field.ClrType == typeof(double)) {
                var tdfc = (PrimitiveDataFrameColumn<double>)dfc;
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    foreach(double el in (double[])dc.Data) {
                        tdfc.Append(el);
                    }
                } else {
                    foreach(double? el in (double?[])dc.Data) {
                        tdfc.Append(el);
                    }
                }
                return;
            }
                        // special case
            if(dc.Field.ClrType == typeof(string)) {
                var tdfc = (StringDataFrameColumn)dfc;
                foreach(string el in (string[])dc.Data) {
                    tdfc.Append(el);
                }
                return;
            }

            throw new NotSupportedException(dc.Field.ClrType.Name);

        }

        public static Array GetTypedDataFast(DataFrameColumn col) {
            
            if(col.DataType == typeof(bool)) {
                return ((PrimitiveDataFrameColumn<bool>)col).ToArray();
            }
            
            if(col.DataType == typeof(int)) {
                return ((PrimitiveDataFrameColumn<int>)col).ToArray();
            }
            
            if(col.DataType == typeof(uint)) {
                return ((PrimitiveDataFrameColumn<uint>)col).ToArray();
            }
            
            if(col.DataType == typeof(long)) {
                return ((PrimitiveDataFrameColumn<long>)col).ToArray();
            }
            
            if(col.DataType == typeof(ulong)) {
                return ((PrimitiveDataFrameColumn<ulong>)col).ToArray();
            }
            
            if(col.DataType == typeof(byte)) {
                return ((PrimitiveDataFrameColumn<byte>)col).ToArray();
            }
            
            if(col.DataType == typeof(sbyte)) {
                return ((PrimitiveDataFrameColumn<sbyte>)col).ToArray();
            }
            
            if(col.DataType == typeof(DateTime)) {
                return ((PrimitiveDataFrameColumn<DateTime>)col).ToArray();
            }
            
            if(col.DataType == typeof(TimeSpan)) {
                return ((PrimitiveDataFrameColumn<TimeSpan>)col).ToArray();
            }
            
            if(col.DataType == typeof(decimal)) {
                return ((PrimitiveDataFrameColumn<decimal>)col).ToArray();
            }
            
            if(col.DataType == typeof(float)) {
                return ((PrimitiveDataFrameColumn<float>)col).ToArray();
            }
            
            if(col.DataType == typeof(double)) {
                return ((PrimitiveDataFrameColumn<double>)col).ToArray();
            }
            
            // special case
            if(col.DataType == typeof(string)) {
                return ((StringDataFrameColumn)col).ToArray();
            }

            throw new NotSupportedException($"type {col.DataType} is not supported (column: {col.Name})");
        }
    }
}