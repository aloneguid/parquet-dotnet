namespace Parquet
{
   /// <summary>
   /// Parquet options
   /// </summary>
   public class ParquetOptions
   {
      /// <summary>
      /// When true byte arrays will be treated as UTF-8 strings
      /// </summary>
      public bool TreatByteArrayAsString { get; set; } = false;

      /// <summary>
      /// Gets or sets a value indicating whether big integers are always treated as dates
      /// </summary>
      public bool TreatBigIntegersAsDates { get; set; } = true;
   }
}
