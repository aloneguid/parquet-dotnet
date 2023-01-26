using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Parquet.Collections {

    /// <summary>
    /// Optimised version of List that can convert list data to Span on request, supposably faster than .ToArray()
    /// </summary>
    class SpanBackedByteList : List<byte>, IDisposable {

        private static readonly ArrayPool<byte> Pool = ArrayPool<byte>.Shared;

        public void Dispose() {

        }

        /// <summary>
        /// Writes array to stream in the most effective way
        /// </summary>
        /// <param name="s"></param>
        public void Write(Stream s) {
            // get pooled array - there's a chance CPU optimises for copying on sunsequent reuse
            byte[] array = Pool.Rent(Count);
            try {
                CopyTo(array);

                s.Write(array, 0, Count);
            } finally {
                Pool.Return(array);
            }
        }
    }
}
