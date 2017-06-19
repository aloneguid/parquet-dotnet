using System.IO;
using Thrift.Protocol;
using Thrift.Transport;

namespace Parquet.File
{
   /// <summary>
   /// Utility methods to work with Thrift data in a stream
   /// </summary>
   class ThriftStream
   {
      private readonly TTransport _transport;
      private readonly TProtocol _protocol;

      public ThriftStream(Stream input)
      {
         _transport = new TStreamTransport(input, null);
         _protocol = new TCompactProtocol(_transport);
      }

      /// <summary>
      /// Reads typed structure from incoming stream
      /// </summary>
      /// <typeparam name="T"></typeparam>
      /// <returns></returns>
      public T Read<T>() where T : TBase, new()
      {
         var res = new T();
         res.Read(_protocol);
         return res;
      }
   }
}
