using Parquet.Data;

namespace Parquet
{

   /// <summary>
   /// Writer options
   /// </summary>
   public class WriterOptions
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="WriterOptions"/> class.
      /// </summary>
      public WriterOptions()
      {
         RowGroupsSize = 5000;
         UseDictionaryEncoding = true;
      }

      /// <summary>
      /// Gets or sets the size of the row group.
      /// </summary>
      public int RowGroupsSize { get; set; }

      /// <summary>
      /// Gets or sets the flag whether to use dictionary encoding when writing.
      /// </summary>
      public bool UseDictionaryEncoding { get; set; }

      internal EmulationMode EmulationMode { get; set; }
   }
}
