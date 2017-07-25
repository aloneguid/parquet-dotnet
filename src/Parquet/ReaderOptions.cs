using System;

namespace Parquet
{
   /// <summary>
   /// Reader options
   /// </summary>
   public class ReaderOptions
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="ReaderOptions"/> class.
      /// </summary>
      public ReaderOptions()
      {
         Offset = 0;
         Count = -1;
      }

      /// <summary>
      /// Gets or sets the offset.
      /// </summary>
      /// <value>
      /// The offset.
      /// </value>
      public int Offset { get; set; }

      /// <summary>
      /// Gets or sets the count.
      /// </summary>
      /// <value>
      /// The count.
      /// </value>
      public int Count { get; set; }

      internal void Validate()
      {
         if (Offset < 0) throw new ParquetException($"cannot read from negative offset {Offset}");
      }
   }
}
