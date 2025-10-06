using System;

namespace Parquet.Bloom {
    /// <summary>
    /// Sizing helpers for Split-Block Bloom Filters (SBBF).
    /// Maps a target false-positive probability (FPP) or explicit bits-per-value (BPV)
    /// to number of 256-bit blocks (z) and total bytes.
    /// </summary>
    public static class BloomSizing {
        /// <summary>
        /// Result of sizing the bloom filter for a column chunk / value set.
        /// </summary>
        public sealed class BloomPlan {
            /// <summary>Number of 256-bit blocks (must be &gt;= 1).</summary>
            public int Blocks { get; set; }

            /// <summary>Total bytes of the bitset (= Blocks * 32).</summary>
            public int NumBytes { get; set; }

            /// <summary>The bits-per-value used for this plan (derived or overridden).</summary>
            public double BitsPerValue { get; set; }

            /// <summary>The FPP target that was requested (if provided), otherwise null.</summary>
            public double? TargetFpp { get; set; }

            /// <summary>Estimated distinct values that were used in planning.</summary>
            public long EstimatedDistinctValues { get; set; }
        }

        /// <summary>
        /// Create a plan from either a target false-positive probability (FPP) or
        /// an explicit bits-per-value. If both are provided, BitsPerValue wins.
        /// </summary>
        /// <param name="estimatedDistinctValues">Estimated number of distinct, non-null values to insert (must be &gt;= 0).</param>
        /// <param name="targetFpp">Desired false-positive probability (0 &lt; FPP &lt; 1), or null.</param>
        /// <param name="bitsPerValueOverride">Explicit bits per value (must be &gt; 0) if provided; overrides targetFpp.</param>
        /// <returns>BloomPlan containing block count and bytes.</returns>
        public static BloomPlan Plan(long estimatedDistinctValues, double? targetFpp, double? bitsPerValueOverride) {
            if(estimatedDistinctValues < 0L) {
                throw new ArgumentOutOfRangeException("estimatedDistinctValues", "Must be >= 0.");
            }
            if(bitsPerValueOverride.HasValue && bitsPerValueOverride.Value <= 0.0) {
                throw new ArgumentOutOfRangeException("bitsPerValueOverride", "Must be > 0 when provided.");
            }
            if(targetFpp.HasValue) {
                if(!(targetFpp.Value > 0.0 && targetFpp.Value < 1.0)) {
                    throw new ArgumentOutOfRangeException("targetFpp", "FPP must be in (0,1) when provided.");
                }
            }

            // Choose bits-per-value
            double bpv;
            if(bitsPerValueOverride.HasValue) {
                bpv = bitsPerValueOverride.Value;
            } else {
                bpv = BloomSizing.BitsPerValueForFpp(targetFpp);
            }

            // Compute total bits and blocks. Each block is 256 bits.
            // Always allocate at least one block so we have a well-formed filter,
            // even for empty columns (z >= 1).
            long totalBitsLong = BloomSizing.CeilToLong(estimatedDistinctValues * bpv);
            long blocksLong = BloomSizing.CeilDiv(totalBitsLong, 256L);
            if(blocksLong < 1L) {
                blocksLong = 1L;
            }
            if(blocksLong >= (1L << 31)) {
                throw new OverflowException("Block count would exceed 2^31 - 1 per Parquet SBBF limit.");
            }

            int blocks = (int)blocksLong;
            int bytes = checked(blocks * 32);

            BloomPlan plan = new BloomPlan();
            plan.Blocks = blocks;
            plan.NumBytes = bytes;
            plan.BitsPerValue = bpv;
            plan.TargetFpp = targetFpp;
            plan.EstimatedDistinctValues = estimatedDistinctValues;
            return plan;
        }

        /// <summary>
        /// Map a desired false-positive probability to a bits-per-value budget for SBBF.
        /// This uses practical thresholds commonly used with Split-Block Bloom Filters.
        /// If no FPP is provided, defaults to ~1% (10.5 bits/value).
        /// </summary>
        public static double BitsPerValueForFpp(double? targetFpp) {
            if(!targetFpp.HasValue) {
                return 10.5; // default: ~1%
            }

            double fpp = targetFpp.Value;

            // Piecewise thresholds (approximate, conservative):
            //   ~10%  => 6.0  bits/value
            //   ~1%   => 10.5 bits/value
            //   ~0.1% => 16.9 bits/value
            //   ~0.01%=> 26.4 bits/value
            //   ~0.001%=>41.0 bits/value
            //
            // We choose the smallest bpv meeting (<=) the requested FPP.
            if(fpp <= 0.00001) // 0.001%
            {
                return 41.0;
            }
            if(fpp <= 0.0001) // 0.01%
            {
                return 26.4;
            }
            if(fpp <= 0.001) // 0.1%
            {
                return 16.9;
            }
            if(fpp <= 0.01) // 1%
            {
                return 10.5;
            }
            // Up to ~10%
            return 6.0;
        }

        private static long CeilDiv(long numerator, long denominator) {
            if(denominator <= 0L) {
                throw new ArgumentOutOfRangeException("denominator");
            }
            if(numerator <= 0L) {
                return 0L;
            }
            long q = numerator / denominator;
            long r = numerator % denominator;
            return r == 0L ? q : q + 1L;
        }

        private static long CeilToLong(double value) {
            if(value <= 0.0) {
                return 0L;
            }
            double c = Math.Ceiling(value);
            if(c > (double)long.MaxValue) {
                throw new OverflowException("Value too large.");
            }
            return (long)c;
        }
    }
}