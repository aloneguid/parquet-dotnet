namespace Parquet.Serialization.Values
{
   class AssignerTag
   {
      public delegate int AssignArrayDelegate(object columnArray, object classInstances, int length);

      public AssignArrayDelegate Assign { get; set; }
   }
}