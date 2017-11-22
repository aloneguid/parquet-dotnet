using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.File;

namespace Parquet.Data
{
   /// <summary>
   /// Represents a data set
   /// </summary>
   public partial class DataSet
   {
      private readonly Schema _schema;
      private readonly Dictionary<string, IList> _columns;
      private int _rowCount;
      private readonly DataSetMetadata _metadata = new DataSetMetadata();

      internal Thrift.FileMetaData Thrift { get; set; }

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(Schema schema)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));

         _columns = new Dictionary<string, IList>();
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(IEnumerable<Field> schema) : this(new Schema(schema))
      {
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(params Field[] schema) : this(new Schema(schema))
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
         _columns = pathToValues;
         _rowCount = _columns.Count == 0 ? 0 : pathToValues.Min(pv => pv.Value.Count);
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
      public IList GetColumn(DataField schemaElement, int offset = 0, int count = -1)
      {
         return GetColumn(schemaElement.Path, offset, count);
      }

      internal IList GetColumn(string path, int offset, int count)
      {
         if (!_columns.TryGetValue(path, out IList values))
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
      public IReadOnlyCollection<T> GetColumn<T>(DataField schemaElement)
      {
         return (List<T>)GetColumn(schemaElement);
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

         return RowExtractor.Extract(_schema.Fields, index, _columns);
      }

      private void RemoveRow(int index)
      {
         ValidateIndex(index);

         foreach(KeyValuePair<string, IList> pe in _columns)
         {
            pe.Value.RemoveAt(index);
         }

         _rowCount -= 1;
      }

      private void AddRow(Row row)
      {
         RowAppender.Append(_columns, _schema.Fields, row);

         _rowCount += 1;
      }

      private void ValidateIndex(int index)
      {
         if(index < 0 || index >= _rowCount)
         {
            throw new IndexOutOfRangeException($"row index {index} is not within allowed range [0; {_rowCount})");
         }
      }


      #endregion

      /// <summary>
      /// Displays some DataSet rows
      /// </summary>
      /// <returns></returns>
      public override string ToString()
      {
         var sb = new StringBuilder();
         int count = Math.Min(Count, 10);

         sb.AppendFormat("first {0} rows:", count);
         sb.AppendLine();

         for(int i = 0; i < count; i++)
         {
            Row row = CreateRow(i);
            sb.AppendFormat("{0,5}: ");
            sb.AppendLine(row.ToString());
         }

         return sb.ToString();
      }
   }
}
