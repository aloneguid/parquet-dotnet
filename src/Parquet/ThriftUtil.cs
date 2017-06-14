using Thrift.Protocol;
using Thrift.Transport;

namespace System.IO
{
   static class ThriftUtil
   {
      public static T ThriftRead<T>(this Stream input) where T : TBase, new()
      {
         //note from spec: All thrift structures are serialized using the TCompactProtocol

         var trans = new TStreamTransport(input, null);
         var proto = new TCompactProtocol(trans);
         var res = new T();
         res.Read(proto);
         return res;
      }

      public static void ThriftWrite<T>(this Stream output, T obj) where T: TBase, new()
      {

      }
   }
}
