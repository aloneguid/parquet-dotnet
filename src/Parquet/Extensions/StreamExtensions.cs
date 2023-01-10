using System;
using System.IO;

namespace Parquet.Extensions {
    static class StreamExtensions {
        public static int ReadInt32(this Stream s) {
            byte[] tmp = new byte[sizeof(int)];
            s.Read(tmp, 0, sizeof(int));
            return BitConverter.ToInt32(tmp, 0);
        }
    }
}
