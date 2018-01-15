using System;
using System.IO;
using System.Text;
using FlatBuffers;
using org.apache.arrow.flatbuf;

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

   public class ArrowFile : IDisposable
   {
      private readonly Stream _s;
      private readonly BinaryReader _br;
      private const string MagicTag = "ARROW1";

      public ArrowFile(Stream s)
      {
         _s = s;
         _br = new BinaryReader(_s);
      }

      public void Read()
      {
         _s.Seek(0, SeekOrigin.Begin);
         ReadMagic(true);

         _s.Seek(-MagicTag.Length, SeekOrigin.End);
         ReadMagic(false);

         _s.Seek(-MagicTag.Length - 4, SeekOrigin.End);
         int footerLength = _br.ReadInt32();

         _s.Seek(-MagicTag.Length - 4 - footerLength, SeekOrigin.End);
         byte[] footerData = _br.ReadBytes(footerLength);
         ReadFooter(footerData);
      }

      private Footer ReadFooter(byte[] data)
      {
         Footer root = Footer.GetRootAsFooter(new ByteBuffer(data));
         return root;
      }

      private void ReadMagic(bool pad)
      {
         byte[] magic = _br.ReadBytes(MagicTag.Length);
         string ms = Encoding.UTF8.GetString(magic);

         if(ms != MagicTag) throw new IOException("not an Arrow file");

         if (pad)
         {
            _s.Seek(8 - MagicTag.Length, SeekOrigin.Current);
         }
      }



      public static void Int32()
      {
         var builder = new FlatBufferBuilder(1024);

         Offset<Schema> schema = Schema.CreateSchema(builder);


         //
         byte[] buf = builder.SizedByteArray();
      }

      public void Dispose()
      {
      }
   }
}
