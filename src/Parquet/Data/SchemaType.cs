namespace Parquet.Data
{
   /// <summary>
   /// Type of schema
   /// </summary>
   public enum SchemaType
   {
      /// <summary>
      /// Contains actual values i.e. declared by a <see cref="DataField"/>
      /// </summary>
      Data,

      Map,

      Struct,

      List
   }
}
