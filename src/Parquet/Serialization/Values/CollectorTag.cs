using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Serialization.Values
{
   class CollectorTag
   {
      public delegate int CollectArrayDelegate(object instances, object result, int length);

      public CollectArrayDelegate Collect { get; set; }
   }
}
