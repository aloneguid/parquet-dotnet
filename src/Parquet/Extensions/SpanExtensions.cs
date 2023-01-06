namespace System {
    static class SpanExtensions {
        public static int ReadInt32(this Span<byte> span) {
            if(BitConverter.IsLittleEndian)
                return (int)span[0] | ((int)span[1] << 8) | ((int)span[2] << 16) | ((int)span[3] << 24);
            return ((int)span[0] << 24) | ((int)span[1] << 16) | ((int)span[2] << 8) | (int)span[3];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="span"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <remarks>
        /// All of these could be replaced with generic math, but we don't have access to it
        /// </remarks>
        public static void MinMax(this ReadOnlySpan<int> span, out int min, out int max) {
            min = span.IsEmpty ? default(int) : span[0];
            max = min;
            foreach(int i in span) {
                if(i < min)
                    min = i;
                if(i > max)
                    max = i;
            }
        }
    }
}