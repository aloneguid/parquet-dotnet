using System;
using System.IO;

namespace Parquet.File.Values {
    /// <summary>
    /// 
    /// </summary>
    public static class ValuesUtils {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        /// <exception cref="IOException"></exception>
        public static int ReadIntOnBytes(byte[] data)
        {
            switch (data.Length)
            {
                case 0:
                    return 0;
                case 1:
                    return (data[0]);
                case 2:
                    return (data[1] << 8) + (data[0]);
                case 3:
                    return (data[2] << 16) + (data[1] << 8) + (data[0]);
                case 4:
                    return BitConverter.ToInt32(data, 0);
                default:
                    throw new IOException($"encountered byte width ({data.Length}) that requires more than 4 bytes.");
            }
        }
    }
}