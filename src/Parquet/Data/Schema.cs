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
      private readonly List<SchemaElement> _elements;
      private readonly Dictionary<string, SchemaElement> _pathToElement = new Dictionary<string, SchemaElement>();

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

      internal Schema(Thrift.FileMetaData fm, ParquetOptions formatOptions)
      {
         void Build(SchemaElement node, int i, int count, bool isRoot)
         {
            while (node.Children.Count < count)
            {
               Thrift.SchemaElement tse = fm.Schema[i];
               int childCount = tse.Num_children;
               bool isContainer = childCount > 0;

               SchemaElement parent = isRoot ? null : node;
               var mse = new SchemaElement(tse, parent, formatOptions, isContainer ? typeof(Row) : null);
               _pathToElement[mse.Path] = mse;
               node.Children.Add(mse);

               if (tse.Num_children > 0)
               {
                  Build(mse, i + 1, childCount, false);
               }

               i += childCount;
               i += 1;
            }
         }

         //extract schema tree
         var root = new SchemaElement<int>("root");
         Build(root, 1, fm.Schema[0].Num_children, true);

         _elements = root.Children.ToList();
      }

      /// <summary>
      /// Gets the schema elements
      /// </summary>
      public IList<SchemaElement> Elements => _elements;

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
            string path = string.Join(".", value.Meta_data.Path_in_schema);

            return _pathToElement[path];
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

         return !_elements.Where((t, i) => !t.Equals(other.Elements[i])).Any();
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
      public string Show()
      {
         var sb = new StringBuilder();

         foreach (SchemaElement se in _elements)
         {
            sb.AppendLine($"- name: '{se.Name}', type: {se.ElementType}, nullable: {se.IsNullable}");
         }

         return sb.ToString();
      }
   }
}
