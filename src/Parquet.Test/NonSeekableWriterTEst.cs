using System;
using System.IO;
using System.Threading.Tasks;
using NetBox.IO;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class NonSeekableWriterTest
   {
      [Fact]
      public async Task Write_multiple_row_groups_to_forward_only_streamAsync()
      {
         var ms = new MemoryStream();
         var forwardOnly = new WriteableNonSeekableStream(ms);

         var schema = new Schema(
            new DataField<int>("id"),
            new DataField<string>("nonsense"));

         await using (ParquetWriter writer = await ParquetWriter.CreateParquetWriterAsync(schema, forwardOnly))
         {
            using (ParquetRowGroupWriter rgw = writer.CreateRowGroup())
            {
               await rgw.WriteColumnAsync(new DataColumn((DataField)schema[0], new[] { 1 })).ConfigureAwait(false);
               await rgw.WriteColumnAsync(new DataColumn((DataField)schema[1], new[] { "1" })).ConfigureAwait(false);
            }

            using (ParquetRowGroupWriter rgw = writer.CreateRowGroup())
            {
               await rgw.WriteColumnAsync(new DataColumn((DataField)schema[0], new[] { 2 })).ConfigureAwait(false);
               await rgw.WriteColumnAsync(new DataColumn((DataField)schema[1], new[] { "2" })).ConfigureAwait(false);
            }
         }

         ms.Position = 0;
         await using (ParquetReader reader = await ParquetReader.OpenFromStreamAsync(ms))
         {
            Assert.Equal(2, reader.RowGroupCount);

            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
            {
               Assert.Equal(1, rgr.RowCount);

               DataColumn column = await rgr.ReadColumnAsync((DataField)schema[0]).ConfigureAwait(false);
               Assert.Equal(1, column.Data.GetValue(0));
            }

            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(1))
            {
               Assert.Equal(1, rgr.RowCount);

               DataColumn column = await rgr.ReadColumnAsync((DataField)schema[0]).ConfigureAwait(false);
               Assert.Equal(2, column.Data.GetValue(0));

            }

         }
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