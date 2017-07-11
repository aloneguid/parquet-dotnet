namespace Parquet
{
   /// <summary>
   /// Parquet compression method
   /// </summary>
   public enum CompressionMethod
   {
      /// <summary>
      /// No compression
      /// </summary>
      None,

      /// <summary>
      /// Gzip compression
      /// </summary>
      Gzip,

      /// <summary>
      /// Snappy compression 
      /// </summary>
      Snappy
   }
}
