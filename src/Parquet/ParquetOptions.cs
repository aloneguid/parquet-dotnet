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

      /// <summary>
      /// Gets or sets the flag whether to use dictionary encoding when writing.
      /// Default value is null meaning that dictionary ecnoding will be used when appropriate.
      /// True enforces using it as much as possible.
      /// False completely disables dictionary encoding.
      /// </summary>
      public bool? UseDictionaryEncoding { get; set; }
   }
}
