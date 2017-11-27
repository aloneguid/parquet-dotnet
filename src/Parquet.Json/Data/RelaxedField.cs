using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Json.Data
{
   class RelaxedField
   {
      public RelaxedField(string name)
      {
         Name = name;
      }

      public List<RelaxedField> Children { get; } = new List<RelaxedField>();

      public string Name { get; }
   }
}
