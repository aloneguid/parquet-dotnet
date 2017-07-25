using System;

namespace Parquet.Data
{
   /// <summary>
   /// General column statistics
   /// </summary>
   public class ColumnSummaryStats
   {
      /// <summary>
      /// Initialises a dataset with a column name
      /// </summary>
      /// <param name="columnName">A dataset columnname</param>
      public ColumnSummaryStats(string columnName)
      {
         ColumnName = columnName;
      }
      /// <summary>
      /// Contains the column name of the stats class
      /// </summary>
      public string ColumnName { get; set; }

      /// <summary>
      /// Number of null values
      /// </summary>
      public int NullCount;
      /// <summary>
      /// The max value for the column
      /// </summary>
      public double Max;
      /// <summary>
      /// The min value for the column
      /// </summary>
      public double Min;
      /// <summary>
      /// The mean value for the column
      /// </summary>
      public double Mean;
      /// <summary>
      /// The column standard deviation 
      /// </summary>
      public double StandardDeviation;

      /// <summary>Returns a string that represents the current object.</summary>
      /// <returns>A string that represents the current object.</returns>
      public override string ToString()
      {
         return
            $"ColumnName: {ColumnName}\nNullCount: {NullCount}\nMax: {Max}\nMin: {Min}\nMean: {Mean}\nStandard Deviation: {StandardDeviation}";
      }
   }
}
