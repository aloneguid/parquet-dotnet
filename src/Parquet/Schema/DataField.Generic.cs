namespace Parquet.Schema
{
   /// <summary>
   /// Element of dataset's schema. Provides a helper way to construct a schema element with .NET generics.
   /// <typeparamref name="T">Type of element in the column</typeparamref>
   /// </summary>
   public class DataField<T> : DataField
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="Field"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="nullable">Indicates if column can contain null values</param>
      public DataField(string name, bool? nullable = null) : base(name, typeof(T), nullable)
      {
      }
   }
}
