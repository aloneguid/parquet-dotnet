using System;
using System.Collections.Generic;

namespace Parquet.Data
{
   /// <summary>
   /// Represents a structure i.e. a container for other fields.
   /// </summary>
   public class StructField : Field
   {
      private string _path;
      private readonly List<Field> _fields = new List<Field>();

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

         //path for structures has no weirdnes, yay!

         foreach(Field field in elements)
         {
            _fields.Add(field);
         }

         Path = name;
         PathPrefix = null;
      }

      internal override string PathPrefix
      {
         set
         {
            Path = value.AddPath(Name);

            foreach(Field field in _fields)
            {
               field.PathPrefix = Path;
            }
         }
      }

      private StructField(string name) : base(name, SchemaType.Struct)
      {

      }

      public override string ToString()
      {
         return $"structure of {_fields.Count}";
      }

      internal static StructField CreateWithNoElements(string name)
      {
         return new StructField(name);
      }

      /// <summary>
      /// Elements of this structure
      /// </summary>
      public IReadOnlyList<Field> Fields => _fields;

      internal override void Assign(Field se)
      {
         _fields.Add(se);
      }
   }
}
