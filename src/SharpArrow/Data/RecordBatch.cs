using System;
using System.Collections.Generic;
using System.Text;
using FB = org.apache.arrow.flatbuf;

namespace SharpArrow.Data
{
   public class RecordBatch : Message
   {
      private readonly List<FB.FieldNode> _nodes;
      private readonly List<FB.Buffer> _buffers;

      internal RecordBatch(FB.Message fbMessage) : base(fbMessage.BodyLength, fbMessage.HeaderType, fbMessage.Version)
      {
         FB.RecordBatch fb = fbMessage.Header<FB.RecordBatch>().GetValueOrDefault();
         _nodes = fb.GetNodes();
         _buffers = fb.GetBuffers();
      }
   }
}
