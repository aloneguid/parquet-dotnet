using System;
using System.IO;
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

   public class ArrowFile
   {
      private readonly Stream _s;

      public ArrowFile(Stream s)
      {
         _s = s;
      }

      public static void Int32()
      {
         var builder = new FlatBufferBuilder(1024);

         Offset<Schema> schema = Schema.CreateSchema(builder);


         //
         byte[] buf = builder.SizedByteArray();
      }
   }
}
