using System.Collections.Generic;

namespace Parquet.Data
{
   /// <summary>
   /// Public metadata
   /// </summary>
   public class DataSetMetadata
   {
      /// <summary>
      /// Gets the creator tag.
      /// </summary>
      public string CreatedBy { get; internal set; }

      /// <summary>
      /// Custom metadata properties
      /// </summary>
      public Dictionary<string, string> Custom { get; private set; } = new Dictionary<string, string>();
   }
}
