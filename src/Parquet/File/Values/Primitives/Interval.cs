namespace Parquet.File.Values.Primitives
{
   /// <summary>
   /// A parquet interval type compatible with a Spark INTERVAL type
   /// 12 byte little Endian structure fits in an INT96 original type with an INTERVAL converted type
   /// </summary>
   public struct Interval
   {
      /// <summary>
      /// Used to create an interval type
      /// </summary>
      /// <param name="months">The month interval</param>
      /// <param name="days">The days interval</param>
      /// <param name="millis">The milliseconds interval</param>
      public Interval(int months, int days, int millis)
      {
         Months = months;
         Days = days;
         Millis = millis;
      }
      /// <summary>
      /// Returns the number of milliseconds in the type
      /// </summary>
      public int Millis { get; set; }

      /// <summary>
      /// Returns the number of days in the type
      /// </summary>
      public int Days { get; set; }

      /// <summary>
      /// Returns the number of months in type
      /// </summary>
      public int Months { get; set; }
   }
}