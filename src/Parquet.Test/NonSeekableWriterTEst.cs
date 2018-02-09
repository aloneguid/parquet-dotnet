using System;
using System.IO;
using NetBox.Generator;
using NetBox.IO;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class NonSeekableWriterTest
   {
      [Fact]
      public void Write_in_small_chunks_to_forward_only_stream()
      {
         var ms = new MemoryStream();
         var forwardOnly = new WriteableNonSeekableStream(ms);

         var ds = new DataSet(
            new DataField<int>("id"),
            new DataField<string>("nonsense"));
         ds.Add(1, RandomGenerator.RandomString);

         using (var writer = new ParquetWriter(forwardOnly))
         {
            writer.Write(ds);
            writer.Write(ds);
            writer.Write(ds);
         }

         ms.Position = 0;
         DataSet ds1 = ParquetReader.Read(ms);

         Assert.Equal(3, ds1.RowCount);

      }

      public class WriteableNonSeekableStream : DelegatedStream
      {
         public WriteableNonSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => false;

         public override bool CanRead => true;

         public override long Seek(long offset, SeekOrigin origin)
         {
            throw new NotSupportedException();
         }

         public override long Position
         {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
         }
      }

   }
}
