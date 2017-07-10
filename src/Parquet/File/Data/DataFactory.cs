using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.File.Data
{
   static class DataFactory
   {
      private static readonly Dictionary<CompressionMethod, (IDataWriter, IDataReader)> CompressionMethodToWorker = new Dictionary<CompressionMethod, (IDataWriter, IDataReader)>()
      {
         { CompressionMethod.None, (new UncompressedDataWriter(), new UncompressedDataReader()) },
         { CompressionMethod.Gzip, (new GzipDataWriter(), new GzipDataReader()) }
      };

      private static readonly Dictionary<CompressionMethod, Thrift.CompressionCodec> CompressionMethodToCodec = new Dictionary<CompressionMethod, Thrift.CompressionCodec>
      {
         { CompressionMethod.None, Thrift.CompressionCodec.UNCOMPRESSED },
         { CompressionMethod.Gzip, Thrift.CompressionCodec.GZIP }
      };

      public static Thrift.CompressionCodec GetThriftCompression(CompressionMethod method)
      {
         if (!CompressionMethodToCodec.TryGetValue(method, out Thrift.CompressionCodec thriftCodec))
            throw new NotSupportedException($"codec '{method}' is not supported");

         return thriftCodec;
      }

      public static IDataWriter GetWriter(CompressionMethod method)
      {
         return CompressionMethodToWorker[method].Item1;
      }

      public static IDataReader GetReader(CompressionMethod method)
      {
         return CompressionMethodToWorker[method].Item2;
      }

      public static IDataReader GetReader(Thrift.CompressionCodec thriftCodec)
      {
         if (!CompressionMethodToCodec.ContainsValue(thriftCodec))
            throw new NotSupportedException($"reader for compression '{thriftCodec}' is not supported.");

         CompressionMethod method = CompressionMethodToCodec.First(kv => kv.Value == thriftCodec).Key;

         return GetReader(method);
      }
   }
}
