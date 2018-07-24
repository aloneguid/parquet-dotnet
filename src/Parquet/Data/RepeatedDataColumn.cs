using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data
{
   //experimental
   class RepeatedDataColumn<T> : IEnumerable<T[]>
   {
      public RepeatedDataColumn(DataColumn dataColumn)
      {

      }

      public IEnumerator<T[]> GetEnumerator()
      {
         throw new NotImplementedException();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         throw new NotImplementedException();
      }
   }
}
