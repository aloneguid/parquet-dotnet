using System;
using System.Collections.Generic;
using System.Text;
using Parquet.DataTypes;

namespace Parquet.Data
{
   class MapSchemaElement : SchemaElement
   {
      public SchemaElement Key { get; }

      public SchemaElement Value { get; }

      internal MapSchemaElement(string name) : base(name, DataType.Dictionary)
      {
      }
   }
}
