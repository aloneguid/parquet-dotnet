namespace Parquet
{
   /// <summary>
   /// Parquet options
   /// </summary>
   public class ParquetOptions
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="ParquetOptions"/> class.
      /// </summary>
      public ParquetOptions()
      {
         TreatByteArrayAsString = false;

         TreatBigIntegersAsDates = true;
      }

      /// <summary>
      /// When true byte arrays will be treated as UTF-8 strings
      /// </summary>
      public bool TreatByteArrayAsString { get; set; }

      /// <summary>
      /// Gets or sets a value indicating whether big integers are always treated as dates
      /// </summary>
      public bool TreatBigIntegersAsDates { get; set; }
   }
}
