using System;
using System.Collections.Generic;
using System.Text;
using FB = org.apache.arrow.flatbuf;

namespace SharpArrow.Data
{
   public class RecordBatch
   {
      private long _length;

      internal RecordBatch(FB.Message fb)
      {
         FB.RecordBatch fbRb = fb.Header<FB.RecordBatch>().GetValueOrDefault();

         _length = fbRb.Length;

         for(int i = 0; i < fbRb.NodesLength; i++)
         {
            FB.FieldNode fbNode = fbRb.Nodes(i).GetValueOrDefault();
         }
      }
   }
}
