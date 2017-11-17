using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

      internal Thrift.FileMetaData Thrift { get; set; }

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
         throw new NotImplementedException();
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
      public IReadOnlyCollection<T> GetColumn<T>(SchemaElement schemaElement)
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

      private Row CreateRow(IEnumerable<SchemaElement> schema, int index, Dictionary<string, IList> pathToValues)
      {
         return new Row(schema.Select(se => CreateElement(se, index, pathToValues)));
      }

      private object CreateElement(SchemaElement se, int index, Dictionary<string, IList> pathToValues)
      {
         if(se.DataType == DataType.Dictionary || se.DataType == DataType.Structure || se.DataType == DataType.List)
         {
            throw new NotSupportedException($"{se.DataType} is not (yet) supported");
         }
         else
         {
            IList values = pathToValues[se.Name];
            return values[index];
         }
      }

      /*private Dictionary<string, IList> CreateElementPathToValue(
         SchemaElement root, int index,
         Dictionary<string, IList> pathToValues,
         out int count)
      {
         var elementPathToValues = new Dictionary<string, IList>();
         count = int.MaxValue;

         foreach(SchemaElement child in root.Children)
         {
            string key = child.Path;
            IList value = pathToValues[key][index] as IList;
            elementPathToValues[key] = value;
            if (value.Count < count) count = value.Count;
         }

         return elementPathToValues;
      }*/

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

      private void AddRow(Row row, IList<SchemaElement> schema, bool updateCount)
      {
         ValidateRow(row);

         for(int i = 0; i < schema.Count; i++)
         {
            SchemaElement se = schema[i];
            object value = row[i];

            if(se.DataType == DataType.Dictionary || se.DataType == DataType.Structure || se.DataType == DataType.List)
            {
               throw new NotSupportedException($"{se.DataType} is not (yet) supported");
            }
            else
            {
               IList values = GetValues(se, true);

               values.Add(value);
            }
         }

         if (updateCount)
         {
            _rowCount += 1;
         }
      }

      private IList GetValues(SchemaElement schema, bool createIfMissing)
      {
         if (!_pathToValues.TryGetValue(schema.Name, out IList values))
         {
            if (createIfMissing)
            {
               IDataTypeHandler handler = DataTypeFactory.Match(schema.DataType);

               values = handler.CreateEmptyList(schema.HasNulls, 0);

               _pathToValues[schema.Name] = values;
            }
            else
            {
               throw new ArgumentException($"column '{schema.Name}' does not exist by path '{schema.Name}'", nameof(schema));
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

         ValidateRow(row.RawValues, _schema.Elements);
      }

      private void ValidateRow(object[] values, IList<SchemaElement> schema)
      {
         if (values.Length != schema.Count)
            throw new ArgumentException($"the row has {values.Length} values but schema expects {schema.Count}", nameof(values));

         for(int i = 0; i < values.Length; i++)
         {
            object value = values[i];
            SchemaElement se = schema[i];

            if(value == null)
            {
               if (!se.HasNulls)
                  throw new ArgumentException($"element is null but column '{se.Name}' does not accept nulls");
            }
            else
            {
               Type vt = value.GetType();
               if (value.GetType() != se.ClrType)
               {
                  if (TrySmartConvert(vt, se.ClrType, value, out object convertedValue))
                  {
                     values[i] = convertedValue;
                  }
                  else
                  {
                     throw new ArgumentException($"expected '{se.ClrType}' but found '{vt}' in column '{se.Name}'");
                  }

               }
            }

         }
      }

      private static bool TrySmartConvert(Type passedType, Type requiredType, object value, out object convertedValue)
      {
         if (passedType == typeof(DateTime) && requiredType == typeof(DateTimeOffset))
         {
            convertedValue = new DateTimeOffset((DateTime)value);
            return true;
         }

         convertedValue = null;
         return false;
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
