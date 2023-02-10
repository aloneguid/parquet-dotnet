using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_BYTE_ARRAY = 7, aka "Delta Strings"
    /// Supported Types: BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
    /// see https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-strings-delta_byte_array--7
    /// </summary>
    /// <remarks>
    /// This encoding depends on DELTA_BINARY_PACKED and DELTA_LENGTH_BYTE_ARRAY
    /// </remarks>
    static class DeltaByteArrayEncoder {
        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount) {
            throw new NotImplementedException();
        }
    }
}
