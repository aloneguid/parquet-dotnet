using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.File.Streams
{
   interface IMarkStream
   {
      /// <summary>
      /// Crappy workaround to mark stream as finished for writing. To be deleted once Snappy supports streaming.
      /// </summary>
      void MarkWriteFinished();
   }
}
