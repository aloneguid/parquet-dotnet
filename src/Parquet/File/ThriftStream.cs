using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Extensions;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Parquet.File {
    /// <summary>
    /// Utility methods to work with Thrift data in a stream
    /// </summary>
    static class ThriftIO {
        /// <summary>
        /// Reads typed structure from incoming stream, using pre-read optimisation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="inputStream"></param>
        /// <param name="size">size of the thrift data in side the stream</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async ValueTask<T> ReadAsync<T>(Stream inputStream, int size, CancellationToken cancellationToken = default) where T : TBase, new() {

            if(size <= 0)
                throw new ArgumentException("must be positive", nameof(size));

            var r = new T();
            byte[] buffer = await inputStream.ReadBytesExactlyAsync(size);
            var transport = new TMemoryBufferTransport(buffer, null);
            var proto = new TCompactProtocol(transport);
            await r.ReadAsync(proto, cancellationToken);
            return r;
        }

        /// <summary>
        /// Reads typed structure from incoming stream, potentially making a lot of reads and is slow
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="inputStream"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async ValueTask<T> ReadAsync<T>(Stream inputStream, CancellationToken cancellationToken = default) where T : TBase, new() {

            var r = new T();
            var transport = new TStreamTransport(inputStream, inputStream, null);
            var proto = new TCompactProtocol(transport);
            await r.ReadAsync(proto, cancellationToken);
            return r;
        }


        /// <summary>
        /// Writes types structure to the destination stream
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="outputStream"></param>
        /// <param name="obj"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Actual size of the object written</returns>
        public static async ValueTask<int> WriteAsync<T>(Stream outputStream, T obj, CancellationToken cancellationToken = default) where T : TBase, new() {
            var transport = new TMemoryBufferTransport(null);
            var proto = new TCompactProtocol(transport);
            await obj.WriteAsync(proto, cancellationToken);
            byte[] buffer = transport.GetBuffer();
            await outputStream.WriteAsync(buffer, 0, buffer.Length);
            return buffer.Length;
        }
    }
}