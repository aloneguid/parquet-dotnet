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
      }

      /// <summary>
      /// Gets or sets the size of the row group.
      /// </summary>
      public int RowGroupsSize { get; set; }
   }
}
