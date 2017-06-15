using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet
{
   /// <summary>
   /// Represents data within parquet file
   /// </summary>
   public class ParquetFrame
   {
      private readonly Dictionary<ParquetColumn, ICollection> _columnNameToData = new Dictionary<ParquetColumn, ICollection>();

      public ParquetFrame()
      {

      }

      public void Merge(ParquetFrame frame)
      {
         if (frame == null) return;

         foreach(var kv in frame._columnNameToData)
         {
            if(!_columnNameToData.TryGetValue(kv.Key, out ICollection v))
            {
               _columnNameToData[kv.Key] = kv.Value;
            }
            else
            {
               throw new NotImplementedException();
            }
         }
      }
   }
}
