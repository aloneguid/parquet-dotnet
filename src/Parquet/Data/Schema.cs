using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data
{
   /// <summary>
   /// Represents dataset schema
   /// </summary>
   public class Schema
   {
      private List<SchemaElement> _elements;
      private readonly Dictionary<string, SchemaElement> _pathToElement = new Dictionary<string, SchemaElement>();


      /// <summary>
      /// Initializes a new instance of the <see cref="Schema"/> class from schema elements.
      /// </summary>
      /// <param name="elements">The elements.</param>
      public Schema(IEnumerable<SchemaElement> elements)
      {
         _elements = elements.ToList();
      }

      internal Schema(Thrift.FileMetaData fm)
      {
         _elements = fm.Schema.Skip(1).Select(se => new SchemaElement(se)).ToList();
         foreach (Thrift.SchemaElement se in fm.Schema)
         {
            _pathToElement[se.Name] = new SchemaElement(se);
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

      internal SchemaElement this[Thrift.ColumnChunk value]
      {
         get
         {
            return _pathToElement[value.Meta_data.Path_in_schema[0]];
         }
      }
   }
}
