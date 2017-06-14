using System;
using System.IO;
using System.Text;
using Parquet.Thrift;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format writer
   /// </summary>
   public class ParquetWriter : IDisposable
   {
      private readonly Stream _output;
      private readonly BinaryWriter _writer;
      private static readonly byte[] Magic = System.Text.Encoding.ASCII.GetBytes("PAR1");
      private readonly FileMetaData _meta = new FileMetaData();

      public ParquetWriter(Stream output)
      {
         _output = output ?? throw new ArgumentNullException(nameof(output));
         if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));

         //file starts with magic
         WriteMagic();

         _meta.Created_by = "parquet-dotnet";
         _meta.Version = 1;
      }

      public void Write(ParquetFrame frame)
      {
         //todo: detect frame schema
      }

      private void WriteMagic()
      {
         _output.Write(Magic, 0, Magic.Length);
      }

      public void Dispose()
      {
         //finalize file

         //write thrift metadata
         long pos = _output.Position;
         _output.ThriftWrite(_meta);
         long size = _output.Position - pos;

         //metadata size
         _writer.Write((int)size);  //4 bytes

         //end magic
         WriteMagic();              //4 bytes
      }
   }
}
