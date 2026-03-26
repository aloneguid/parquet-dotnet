using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Encodings;

static class MissingValuesEncoder {
    public static void Encode<T, TNonNull>(ReadOnlyMemory<T> values, Span<TNonNull> dest, Span<int> definitionLevels) {
        for (int i = 0; i < values.Length; i++) {
            bool isNull = values.Span[i] is null;
        }
    }
}
