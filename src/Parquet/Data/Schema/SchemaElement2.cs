using System;
using System.Collections.Generic;
using System.Text;
using Parquet.DataTypes;

namespace Parquet.Data
{
   /// <summary>
   /// Experimental:
   /// New schema element
   /// </summary>
   class SchemaElement2
   {
      private readonly List<SchemaElement2> _children = new List<SchemaElement2>();

      public SchemaElement2(string name, DataType dataType, SchemaElement2 parent)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));
         DataType = dataType;
         Parent = parent;
      }

      public string Name { get; }

      public DataType DataType { get; }

      public SchemaElement2 Parent { get; }

      public IList<SchemaElement2> Children => _children;

      public override string ToString()
      {
         return $"{Name} ({DataType})";
      }
   }
}
