using System;
using System.Collections.Generic;

namespace Parquet.Data
{
   /// <summary>
   /// Represents a structure i.e. a container for other fields.
   /// </summary>
   public class StructField : Field
   {
      private readonly List<Field> _elements = new List<Field>();

      /// <summary>
      /// Creates a new structure field 
      /// </summary>
      /// <param name="name">Structure name</param>
      /// <param name="elements">List of elements</param>
      public StructField(string name, params Field[] elements) : this(name)
      {
         if(elements == null || elements.Length == 0)
         {
            throw new ArgumentException($"structure '{name}' requires at least one element");
         }

         foreach(Field element in elements)
         {
            element.Path = $"{Path}{Schema.PathSeparator}{element.Path}";
            _elements.Add(element);
         }
      }

      private StructField(string name) : base(name, SchemaType.Structure)
      {

      }

      internal static StructField CreateWithNoElements(string name)
      {
         return new StructField(name);
      }

      /// <summary>
      /// Elements of this structure
      /// </summary>
      public IReadOnlyList<Field> Elements => _elements;

      internal override void Assign(Field se)
      {
         _elements.Add(se);
      }
   }
}
