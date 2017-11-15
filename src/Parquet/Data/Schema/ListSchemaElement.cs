using System;
using System.Collections.Generic;
using System.Text;
using Parquet.DataTypes;

namespace Parquet.Data
{
   class ListSchemaElement : SchemaElement
   {
      public SchemaElement Item { get; internal set; }

      internal ListSchemaElement(string name) : base(name, DataType.List)
      {
      }
   }
}
