using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Formats
{
   /// <summary>
   /// CSV Options
   /// </summary>
   public class CsvOptions
   {
      public bool HasHeaders { get; set; }

      public bool InferSchema { get; set; }
   }
}
