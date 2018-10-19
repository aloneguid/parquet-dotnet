using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using FlatBuffers;
using SharpArrow.Data;
using FB = org.apache.arrow.flatbuf;

namespace SharpArrow
{
   public class ArrowStream
   {
      private readonly Memory<byte> _streamMemory;

      public ArrowStream(Schema schema, Memory<byte> memory)
      {
         _streamMemory = memory;
         //MemoryMarshal.Cast
      }

      public void TempTest()
      {
         /*
          <metadata_size: int32>
         <metadata_flatbuffer: bytes>
         <padding>
         <message body>
         */

         Span<byte> span = _streamMemory.Span;

         int metaLength = span.ReadInt32();
         metaLength = 339;

         FB.Schema schema = FB.Schema.GetRootAsSchema(new ByteBuffer(span.Slice(4, metaLength).ToArray()));

         //stream always begins with schema

         /*foreach (FB.Block block in _records)
         {
            int length = BitConverter.ToInt32(_fileData.Slice(0, 4).Span.ToArray(), 0);

            byte[] messageData = _fileData.Slice((int)block.Offset + 4, block.MetaDataLength).ToArray();
            var message = Message.CreateFromData(messageData);
         }*/
      }

   }
}
