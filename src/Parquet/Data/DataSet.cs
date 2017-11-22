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
      private readonly Dictionary<string, IList> _pathToValues;
      private int _rowCount;
      private readonly DataSetMetadata _metadata = new DataSetMetadata();
      private readonly DataSetValidator _validator;

      internal Thrift.FileMetaData Thrift { get; set; }

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(Schema schema)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));

         _pathToValues = new Dictionary<string, IList>();

         _validator = new DataSetValidator(this);
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
         _pathToValues = pathToValues;
         _rowCount = _pathToValues.Count == 0 ? 0 : pathToValues.Min(pv => pv.Value.Count);
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
         if (!_pathToValues.TryGetValue(path, out IList values))
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

         return CreateRow(_schema.Elements, index, _pathToValues);
      }

      private Row CreateRow(IEnumerable<Field> schema, int index, Dictionary<string, IList> pathToValues)
      {
         return new Row(schema.Select(se => CreateElement(se, index, pathToValues)));
      }

      private object CreateElement(Field se, int index, Dictionary<string, IList> pathToValues)
      {
         if(se.SchemaType == SchemaType.Map)
         {
            return ((MapField)se).CreateCellValue(pathToValues, index);
         }
         else if(se.SchemaType == SchemaType.Structure || se.SchemaType == SchemaType.List)
         {
            throw new NotSupportedException($"{se.SchemaType} is not (yet) supported");
         }
         else
         {
            IList values = pathToValues[se.Name];
            return values[index];
         }
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
         AddRow(row, _schema.Elements, true);
      }

      private void AddRow(Row row, IReadOnlyList<Field> schema, bool updateCount)
      {
         _validator.ValidateRow(row);

         for(int i = 0; i < schema.Count; i++)
         {
            Field se = schema[i];
            object value = row[i];

            if(se.SchemaType == SchemaType.Map)
            {
               ((MapField)se).AddElement(this, value as IDictionary);
            }
            else if(se.SchemaType == SchemaType.Structure || se.SchemaType == SchemaType.List)
            {
               throw new NotSupportedException($"{se.SchemaType} is not (yet) supported");
            }
            else
            {
               IList values = GetValues((DataField)se, true);

               values.Add(value);
            }
         }

         if (updateCount)
         {
            _rowCount += 1;
         }
      }

      internal IList GetValues(DataField field, bool createIfMissing, bool isNested = false)
      {
         if(field.Path == null) throw new ArgumentNullException(nameof(field.Path));

         if (!_pathToValues.TryGetValue(field.Path, out IList values))
         {
            if (createIfMissing)
            {
               IDataTypeHandler handler = DataTypeFactory.Match(field);

               values = isNested
                  ? (IList)(new List<IEnumerable>())
                  : (IList)(handler.CreateEmptyList(field.HasNulls, field.IsArray, 0));

               _pathToValues[field.Path] = values;
            }
            else
            {
               throw new ArgumentException($"column does not exist by path '{field.Path}'", nameof(field));
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
