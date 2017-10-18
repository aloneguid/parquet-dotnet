using Parquet.File.Values.Primitives;

namespace Parquet.Data
{
   /// <summary>
   /// Maps onto Parquet Interval type 
   /// </summary>
   public class IntervalSchemaElement : SchemaElement
   {
      /// <summary>
      /// Constructs a parquet interval type
      /// </summary>
      /// <param name="name">The name of the column</param>
      public IntervalSchemaElement(string name) : base(name)
      {
         Thrift.Type = Parquet.Thrift.Type.FIXED_LEN_BYTE_ARRAY;
         Thrift.Converted_type = Parquet.Thrift.ConvertedType.INTERVAL;
         Thrift.Type_length = 12;
         ElementType = ColumnType = typeof(Interval);
      }
   }
}