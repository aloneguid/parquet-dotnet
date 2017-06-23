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
      private readonly Stream _s;
      private readonly TTransport _transport;
      private readonly TProtocol _protocol;

      public ThriftStream(Stream s)
      {
         _s = s;
         _transport = new TStreamTransport(s, s);
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

      /// <summary>
      /// Writes types structure to the destination stream
      /// </summary>
      /// <typeparam name="T"></typeparam>
      /// <param name="obj"></param>
      /// <returns>Actual size of the object written</returns>
      public long Write<T>(T obj) where T : TBase, new()
      {
         long startPos = _s.Position;
         obj.Write(_protocol);
         return _s.Position - startPos;
      }
   }
}
