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
         _elements = fm.Schema.Skip(1).Select(se => new SchemaElement(se, formatOptions)).ToList();
         foreach (Thrift.SchemaElement se in fm.Schema)
         {
            _pathToElement[se.Name] = new SchemaElement(se, formatOptions);
         }
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
      /// Gets the column index by schema element
      /// </summary>
      /// <returns>Element index or -1 if not found</returns>
      public int GetElementIndex(SchemaElement schema)
      {
         for (int i = 0; i < _elements.Count; i++)
            if (schema.Equals(_elements[i])) return i;

         return -1;
      }

      internal int GetMaxDefinitionLevel(Thrift.ColumnChunk cc)
      {
         int max = 0;

         foreach (string part in cc.Meta_data.Path_in_schema)
         {
            SchemaElement element = _pathToElement[part];
            if (element.Thrift.Repetition_type != Thrift.FieldRepetitionType.REQUIRED) max += 1;
         }

         return max;
      }

      internal SchemaElement this[Thrift.ColumnChunk value] => _pathToElement[value.Meta_data.Path_in_schema[0]];

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
