using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Parquet.File;

namespace Parquet.Data
{
   /// <summary>
   /// Represents a data set
   /// </summary>
   public partial class DataSet
   {
      private readonly Schema _schema;
      private readonly Dictionary<string, IList> _pathToValues;
      private int _rowCount;
      private readonly DataSetMetadata _metadata = new DataSetMetadata();

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(Schema schema)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));

         _pathToValues = new Dictionary<string, IList>();
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(IEnumerable<SchemaElement> schema) : this(new Schema(schema))
      {
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(params SchemaElement[] schema) : this(new Schema(schema))
      {
      }

      /// <summary>
      /// Gets dataset schema
      /// </summary>
      public Schema Schema => _schema;

      /// <summary>
      /// Gets the public metadata
      /// </summary>
      public DataSetMetadata Metadata => _metadata;

      /// <summary>
      /// Gets the total row count in the source file this dataset was read from
      /// </summary>
      public long TotalRowCount { get; }

      internal DataSet(Schema schema,
         Dictionary<string, IList> pathToValues,
         long totalRowCount,
         string createdBy) : this(schema)
      {
         _pathToValues = pathToValues;
         _rowCount = pathToValues.Min(pv => pv.Value.Count);
         TotalRowCount = totalRowCount;
         _metadata.CreatedBy = createdBy;
      }


      /// <summary>
      /// Slices rows and returns list of all values in a particular column.
      /// </summary>
      /// <param name="schemaElement">Schema element</param>
      /// <param name="offset">The offset.</param>
      /// <param name="count">The count.</param>
      /// <returns>
      /// Column values
      /// </returns>
      /// <exception cref="ArgumentException"></exception>
      public IList GetColumn(SchemaElement schemaElement, int offset = 0, int count = -1)
      {
         if (schemaElement == null)
         {
            throw new ArgumentNullException(nameof(schemaElement));
         }

         if (!_pathToValues.TryGetValue(schemaElement.Path, out IList values))
         {
            return null;
         }

         //optimise for performance by not instantiating another list if you want all the column values
         if (offset == 0 && count == -1) return values;

         IList page = (IList)Activator.CreateInstance(values.GetType());
         int max = (count == -1)
            ? values.Count
            : Math.Min(offset + count, values.Count);
         for(int i = offset; i < max; i++)
         {
            page.Add(values[i]);
         }
         return page;
      }

      /// <summary>
      /// Gets the column as strong typed collection
      /// </summary>
      /// <typeparam name="T">Column element type</typeparam>
      /// <param name="schemaElement">Column schema</param>
      /// <returns>Strong typed collection</returns>
      public IReadOnlyCollection<T> GetColumn<T>(SchemaElement schemaElement)
      {
         return (List<T>)GetColumn(schemaElement);
      }

      /// <summary>
      /// Used to merge and add columns to a dataset 
      /// </summary>
      /// <param name="ds">A second dataset to merge this one with</param>
      public DataSet Merge(DataSet ds)
      {
         return new DataSetMerge().Merge(this, ds);
      }

      /// <summary>
      /// Adds the specified values.
      /// </summary>
      /// <param name="values">The values.</param>
      public void Add(params object[] values)
      {
         AddRow(new Row(values));
      }

      #region [ Row Manipulation ]

      private Row CreateRow(int index)
      {
         ValidateIndex(index);

         return new Row(
            _schema.Elements
               .Select(se => se.Path)
               .Select(path => _pathToValues[path])
               .Select(values => values[index]));
      }

      private void RemoveRow(int index)
      {
         ValidateIndex(index);

         foreach(KeyValuePair<string, IList> pe in _pathToValues)
         {
            pe.Value.RemoveAt(index);
         }

         _rowCount -= 1;
      }

      private void AddRow(Row row)
      {
         ValidateRow(row);

         for(int i = 0; i < _schema.Length; i++)
         {
            SchemaElement se = _schema[i];
            IList values = GetValues(se, true);
            values.Add(row[i]);
         }

         _rowCount += 1;
      }

      private IList GetValues(SchemaElement schema, bool createIfMissing)
      {
         if (!_pathToValues.TryGetValue(schema.Path, out IList values))
         {
            if (createIfMissing)
            {
               values = TypeFactory.Create(schema.ElementType, schema.IsNullable, schema.IsRepeated);
               _pathToValues[schema.Path] = values;
            }
            else
            {
               throw new ArgumentException($"column '{schema.Name}' does not exist by path '{schema.Parent}'", nameof(schema));
            }
         }

         return values;
      }

      private void ValidateIndex(int index)
      {
         if(index < 0 || index >= _rowCount)
         {
            throw new IndexOutOfRangeException($"row index {index} is not within allowed range [0; {_rowCount})");
         }
      }

      private void ValidateRow(Row row)
      {
         if (row == null) throw new ArgumentNullException(nameof(row));

         int rl = row.Length;

         if (rl != _schema.Length)
            throw new ArgumentException($"the row has {rl} values but schema expects {_schema.Length}", nameof(row));

         //todo: type validation
      }

      #endregion
   }
}
