using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.Serialization.Values
{
   interface IColumnValuesExtractor
   {
      void ExtractToList(IEnumerable classInstances);
   }
}
