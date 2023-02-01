using System;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_BINARY_PACKED (https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5)
    /// fastparquet sample: https://github.com/dask/fastparquet/blob/c59e105537a8e7673fa30676dfb16d9fa5fb1cac/fastparquet/cencoding.pyx#L232
    /// golang sample: https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingread.go#L270
    /// 
    /// Supported Types: INT32, INT64
    /// </summary>
    static partial class DeltaBinaryPackedEncoder {

        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount) {
            Type? elementType = dest.GetType().GetElementType();
            if(elementType != null) {
                if(elementType == typeof(long) ) {
                    Span<long> span = ((long[])dest).AsSpan(destOffset);
                    return Decode(s, span);
                } else if (elementType == typeof(int)) {
                    Span<int> span = ((int[])dest).AsSpan(destOffset);
                    return Decode(s, span);
                }
                else {
                    throw new NotSupportedException($"only {Thrift.Type.INT32} and {Thrift.Type.INT64} are supported in {Thrift.Encoding.DELTA_BINARY_PACKED} but element type passed is {elementType}");
                }
            }

            throw new NotSupportedException($"element type {elementType} is not supported");
        }
    }
}
