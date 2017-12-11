using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.File
{
   class StatCounter
   {
      private readonly IList _list;

      public StatCounter(IList list)
      {
         _list = list;

         //todo: calculate stats
      }
   }
}
