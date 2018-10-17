using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using FlatBuffers;
using SharpArrow.Data;
using FB = org.apache.arrow.flatbuf;

namespace SharpArrow
{


   /*
    * 
    * 
    * 

      See https://arrow.apache.org/docs/ipc.html
      
      File Format:
      
<magic number "ARROW1">
<empty padding bytes [to 8 byte boundary]>
<STREAMING FORMAT>
<FOOTER>
<FOOTER SIZE: int32>
<magic number "ARROW1">


      Streaming Format:

<SCHEMA>
<DICTIONARY 0>
...
<DICTIONARY k - 1>
<RECORD BATCH 0>
...
<RECORD BATCH n - 1>
<EOS [optional]: int32>



    */

   public class ArrowFile
   {
      private const string MagicTag = "ARROW1";
      private readonly List<FB.Block> _dictionaries = new List<FB.Block>();
      private readonly List<FB.Block> _records = new List<FB.Block>();
      private readonly Memory<byte> _fileData;
      private int _footerLength;

      public ArrowFile(Memory<byte> fileData)
      {
         _fileData = fileData;

         ValidateFile();
      }

      public Schema Schema { get; private set; }

      public ArrowStream GetStream()
      {
         Memory<byte> dataMemory = _fileData.Slice(8, _fileData.Length - MagicTag.Length - 4 - _footerLength);

         return new ArrowStream(Schema, dataMemory);
      }

      public void TempTest()
      {
         foreach (FB.Block block in _records)
         {
            /*
             <metadata_size: int32>
            <metadata_flatbuffer: bytes>
            <padding>
            <message body>
            */


            int length = BitConverter.ToInt32(_fileData.Slice(0, 4).Span.ToArray(), 0);

            byte[] messageData = _fileData.Slice((int)block.Offset + 4, block.MetaDataLength).ToArray();
            FB.Message message = FB.Message.GetRootAsMessage(new ByteBuffer(messageData));

            new RecordBatch(message);
         }
      }

      private void ValidateFile()
      {
         Span<byte> span = _fileData.Span;

         //check magic bytes at the beginning and the end of the file
         CheckMagic(span.Slice(0, MagicTag.Length));
         CheckMagic(span.Slice(span.Length - MagicTag.Length));

         //get footer length
         Span<byte> lengthSpan = span.Slice(span.Length - 4 - MagicTag.Length, 4);
         _footerLength = BitConverter.ToInt32(lengthSpan.ToArray(), 0);

         //get footer data (file contains redundant copy of the schema)
         Span<byte> footer = span.Slice(span.Length - 4 - _footerLength - MagicTag.Length, _footerLength);
         ReadFooter(footer);
      }

      private void ReadFooter(Span<byte> data)
      {
         FB.Footer root = FB.Footer.GetRootAsFooter(new ByteBuffer(data.ToArray()));

         //read schema
         Schema = new Schema(root.Schema.GetValueOrDefault());

         //read list of blocks for convenience
         for(int i = 0; i < root.RecordBatchesLength; i++)
         {
            FB.Block block = root.RecordBatches(i).GetValueOrDefault();

            _records.Add(block);
         }

         if (root.DictionariesLength > 0)
            throw new NotSupportedException();
      }

      private void CheckMagic(Span<byte> span)
      {
         byte[] sd = span.ToArray();
         string ms = Encoding.UTF8.GetString(sd);

         if(ms != MagicTag) throw new IOException("not an Arrow file");
      }
   }
}
