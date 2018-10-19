using System;
using System.Collections.Generic;
using System.Text;
using FB = org.apache.arrow.flatbuf;

/*
 * Auto-generated, please modify the .tt file. For instructions refer to https://docs.microsoft.com/en-us/visualstudio/modeling/design-time-code-generation-by-using-t4-text-templates?view=vs-2017
 */
namespace SharpArrow.Data
{
   static class FlatBuffersExtensions
   {

      /// <summary>
      /// Creates a list of <see cref="FB.Block"/>
      /// </summary>
      public static List<FB.Block> GetRecordBatches(this FB.Footer root)
      {
         int length = root.RecordBatchesLength;
         var result = new List<FB.Block>(length);

         for(int i = 0; i < length; i++)
         {
            FB.Block element = root.RecordBatches(i).GetValueOrDefault();
            result.Add(element);
         }

         return result;
      }

      /// <summary>
      /// Creates a list of <see cref="FB.FieldNode"/>
      /// </summary>
      public static List<FB.FieldNode> GetNodes(this FB.RecordBatch root)
      {
         int length = root.NodesLength;
         var result = new List<FB.FieldNode>(length);

         for(int i = 0; i < length; i++)
         {
            FB.FieldNode element = root.Nodes(i).GetValueOrDefault();
            result.Add(element);
         }

         return result;
      }

      /// <summary>
      /// Creates a list of <see cref="FB.Buffer"/>
      /// </summary>
      public static List<FB.Buffer> GetBuffers(this FB.RecordBatch root)
      {
         int length = root.BuffersLength;
         var result = new List<FB.Buffer>(length);

         for(int i = 0; i < length; i++)
         {
            FB.Buffer element = root.Buffers(i).GetValueOrDefault();
            result.Add(element);
         }

         return result;
      }
   }
}