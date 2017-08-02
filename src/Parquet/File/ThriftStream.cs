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
      private readonly TProtocol _protocol;

      public ThriftStream(Stream s)
      {
         _s = s;
         TTransport transport = new TStreamTransport(s, s);
         _protocol = new TCompactProtocol(transport);
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
      /// <param name="rewind">When true, rewinds to the original position before writing</param>
      /// <returns>Actual size of the object written</returns>
      public int Write<T>(T obj, bool rewind = false) where T : TBase, new()
      {
         _s.Flush();
         long startPos = _s.Position;
         obj.Write(_protocol);
         _s.Flush();
         long size = _s.Position - startPos;
         if (rewind) _s.Seek(startPos, SeekOrigin.Begin);
         return (int)size;
      }
   }
}
