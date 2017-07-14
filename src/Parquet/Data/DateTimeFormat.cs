namespace Parquet.Data
{
   /// <summary>
   /// Choice of representing dates
   /// </summary>
   public enum DateTimeFormat
   {
      /// <summary>
      /// The impala compatible date, which maps to INT96. This is the default datetime representation.
      /// </summary>
      Impala,


      /// <summary>
      /// This is the default Parquet datetime representation, but not default option for saving which is <see cref="Impala"/>.
      /// Stores date and time up to millisecond precision as INT64
      /// </summary>
      DateAndTime,

      /// <summary>
      /// Only stores a date. Time portion is truncated. Internally stored as INT32
      /// </summary>
      Date
   }
}
