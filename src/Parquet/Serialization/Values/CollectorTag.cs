namespace Parquet.Serialization.Values
{
   class CollectorTag
   {
      public delegate void CollectArrayDelegate(object instances, object result, int length);

      public CollectArrayDelegate Collect { get; set; }
   }
}
