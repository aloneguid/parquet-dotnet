using System;

namespace Parquet.File.Values.Packer {
    /// <summary>
    /// Interface for packing and unpacking values.
    /// </summary>
    public interface IPacker {
        /// <summary>
        /// Interface for unpacking values from a byte array.
        /// </summary>
        /// <param name="inBytes"></param>
        /// <param name="inPos"></param>
        /// <param name="outData"></param>
        /// <param name="outPos"></param>
        public void Unpack8Values(ReadOnlySpan<byte> inBytes, int inPos, long[] outData, int outPos);
    }
}