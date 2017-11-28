using System.Collections.Generic;
using System.Linq;
using Parquet.Data;

namespace Parquet.Json.Data
{
   class RelaxedField
   {
      public RelaxedField(string name, RelaxedField parent)
      {
         Name = name;
         Parent = parent;
      }

      public RelaxedField Parent { get; }

      public List<RelaxedField> Children { get; } = new List<RelaxedField>();

      public string Name { get; }

      public DataType? DataType { get; set; }

      public override string ToString()
      {
         return $"{Name} ({DataType})";
      }

      public Field ToField()
      {
         if(Children.Count == 0)
         {
            return new DataField(Name, DataType.Value, true, false);
         }

         return new StructField(Name, Children.Select(c => c.ToField()).ToArray());
      }
   }
}
