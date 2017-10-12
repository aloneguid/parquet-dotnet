using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data
{
   /// <summary>
   /// Represents dataset schema
   /// </summary>
   public class Schema : IEquatable<Schema>
   {
      /// <summary>
      /// Symbol used to separate path parts in schema element path
      /// </summary>
      public const string PathSeparator = ".";

      /// <summary>
      /// Character used to separate path parts in schema element path
      /// </summary>
      public const char PathSeparatorChar = '.';

      private readonly List<SchemaElement> _elements;
      private Dictionary<string, SchemaElement> _pathToElement;

      /// <summary>
      /// Initializes a new instance of the <see cref="Schema"/> class from schema elements.
      /// </summary>
      /// <param name="elements">The elements.</param>
      public Schema(IEnumerable<SchemaElement> elements)
      {
         _elements = elements.ToList();
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="Schema"/> class.
      /// </summary>
      /// <param name="elements">The elements.</param>
      public Schema(params SchemaElement[] elements)
      {
         _elements = elements.ToList();
      }

      /// <summary>
      /// Gets the schema elements
      /// </summary>
      public IList<SchemaElement> Elements => _elements;

      /// <summary>
      /// Gets the elements in flat list without hierarchy
      /// </summary>
      internal ICollection<SchemaElement> FlatElements
      {
         get
         {
            var flatList = new List<SchemaElement>();

            void Walk(SchemaElement node, List<SchemaElement> list)
            {
               if(node.Children.Count > 0)
               {
                  foreach(SchemaElement child in node.Children)
                  {
                     Walk(child, list);
                  }
               }
               else
               {
                  list.Add(node);
               }
            }

            _elements.ForEach(e => Walk(e, flatList));

            return flatList;
         }
      }

      /// <summary>
      /// Gets the number of elements in the schema
      /// </summary>
      public int Length => _elements.Count;

      /// <summary>
      /// Gets the column names as string array
      /// </summary>
      public string[] ColumnNames => _elements.Select(e => e.Name).ToArray();

      /// <summary>
      /// Get schema element by index
      /// </summary>
      /// <param name="i">Index of schema element</param>
      /// <returns>Schema element</returns>
      public SchemaElement this[int i]
      {
         get { return _elements[i]; }
      }

      /// <summary>
      /// Get schema element by name
      /// </summary>
      /// <param name="name">Schema element name</param>
      /// <returns>Schema element</returns>
      public SchemaElement this[string name]
      {
         get
         {
            SchemaElement result = _elements.FirstOrDefault(e => e.Name == name);

            if (result == null) throw new ArgumentException($"schema element '{name}' not found", nameof(name));

            return result;
         }
      }

      /// <summary>
      /// Gets the column index by schema element
      /// </summary>
      /// <returns>Element index or -1 if not found</returns>
      public int GetElementIndex(SchemaElement schema)
      {
         for (int i = 0; i < _elements.Count; i++)
            if (schema.Equals(_elements[i])) return i;

         return -1;
      }

      internal SchemaElement this[Thrift.ColumnChunk value]
      {
         get
         {
            string path = string.Join(PathSeparator, value.Meta_data.Path_in_schema);

            if (_pathToElement == null) BuildPathCache();

            if (!_pathToElement.TryGetValue(path, out SchemaElement result))
               throw new ArgumentException($"cannot find schema element by path '{path}'", nameof(value));

            return result;
         }
      }

      /// <summary>
      /// Returns true if schema contains any nested structures declarations, false otherwise
      /// </summary>
      public bool HasNestedElements => _elements.Any(e => e.Children.Count > 0);

      private void BuildPathCache()
      {
         _pathToElement = new Dictionary<string, SchemaElement>();

         CachePath(Elements);
      }

      private void CachePath(IEnumerable<SchemaElement> elements)
      {
         foreach(SchemaElement element in elements)
         {
            _pathToElement[element.Path] = element;

            if (element.Children.Count > 0) CachePath(element.Children);
         }
      }

      /// <summary>
      /// Indicates whether the current object is equal to another object of the same type.
      /// </summary>
      /// <param name="other">An object to compare with this object.</param>
      /// <returns>
      /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
      /// </returns>
      public bool Equals(Schema other)
      {
         if (ReferenceEquals(null, other)) return false;
         if (ReferenceEquals(this, other)) return true;

         if (_elements.Count != other._elements.Count) return false;

         foreach(Tuple<SchemaElement, SchemaElement> pair in EnumerableEx.MultiIterate(_elements, other.Elements))
         {
            if (!pair.Item1.Equals(pair.Item2)) return false;
         }

         return true;
      }

      /// <summary>
      /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
      /// </summary>
      /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
      /// <returns>
      ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
      /// </returns>
      public override bool Equals(object obj)
      {
         if (ReferenceEquals(null, obj)) return false;
         if (ReferenceEquals(this, obj)) return true;
         if (obj.GetType() != GetType()) return false;

         return Equals((Schema) obj);
      }

      /// <summary>
      /// Returns a hash code for this instance.
      /// </summary>
      /// <returns>
      /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
      /// </returns>
      public override int GetHashCode()
      {
         return _elements.Aggregate(1, (current, se) => current * se.GetHashCode());
      }

      /// <summary>
      /// Shows schema in human readable form
      /// </summary>
      /// <returns></returns>
      public override string ToString()
      {
         var sb = new StringBuilder();

         int level = 0;
         sb.AppendLine("root");
         foreach (SchemaElement se in _elements)
         {
            ToString(sb, se, level);
         }

         return sb.ToString();
      }

      private void ToString(StringBuilder sb, SchemaElement se, int level)
      {
         sb.Append("|");
         for(int i = 0; i < level; i++)
         {
            sb.Append("    |");
         }
         sb.Append("-- ");
         sb.AppendLine(se.ToString());

         foreach(SchemaElement child in se.Children)
         {
            ToString(sb, child, level + 1);
         }
      }
   }
}
