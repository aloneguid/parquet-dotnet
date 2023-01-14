using System;

namespace Parquet.Extensions {
    static class ArrayExtensions {

        /// <summary>
        /// Explodes dictionary and indexes into full array. This is used to unpack dictionary encoding.
        /// Result must be already pre-allocated before calling this method.
        /// </summary>
        public static void Explode(this Array dictionary,
            Span<int> indexes,
            Array result, int resultOffset, int resultCount) {
            for(int i = 0; i < resultCount; i++) {
                int index = indexes[i];
                if(index < dictionary.Length) {
                    // The following is way faster than using Array.Get/SetValue as it avoids boxing (x60 slower)
                    // It's still x5 slower than native typed operation as it emits "callvirt" IL instruction
                    Array.Copy(dictionary, index, result, resultOffset + i, 1);
                }
            }
        }
    }
}
