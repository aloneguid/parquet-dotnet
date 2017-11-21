using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data
{
   class ListField : Field
   {
      public Field Item { get; internal set; }

      internal ListField(string name) : base(name, SchemaType.List)
      {
      }
   }
}
