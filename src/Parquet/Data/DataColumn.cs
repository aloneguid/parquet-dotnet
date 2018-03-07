using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data
{
   /// <summary>
   /// The primary low-level structure to hold data for a parqut column.
   /// Handles internal data composition/decomposition to enrich with custom data Parquet format requires.
   /// </summary>
   internal class DataColumn
   {
      private readonly DataField _field;
      private readonly IList _definedData;            // data that is defined i.e. doesn't ever have nulls
      private readonly List<int> _definitionLevels;   // not utilised at all when field is not nullable
      private int _undefinedCount;

      public DataColumn(DataField field)
      {
         _field = field ?? throw new ArgumentNullException(nameof(field));

         IDataTypeHandler handler = DataTypeFactory.Match(field.DataType);
         _definedData = handler.CreateEmptyList(false, false, 0);       // always a plain list, always non-nullable when possible
         _definitionLevels = field.HasNulls ? new List<int>() : null;   // do not create an instance when not required
      }

      internal DataColumn(DataField field, IList definedData, List<int> definitionLevels) : this(field)
      {
         _definedData.AddOneByOne(definedData);

         if (_definitionLevels != null)
         {
            _definitionLevels.AddRange(definitionLevels);
            _undefinedCount = _definitionLevels.Count(l => l == 0);
         }
      }

      public DataColumn(DataField field, IEnumerable data) : this(field)
      {
         if(!data.GetType().TryExtractEnumerableType(out Type baseType))
         {
            throw new ArgumentException($"the collection is not a generic one", nameof(data));
         }

         if (baseType != field.ClrType) throw new ArgumentException($"expected {_field.ClrType} but passed a collection of {baseType}");

         AddRange(data);
      }

      public bool HasRepetitions => false;

      public bool HasDefinitions => _field.HasNulls;

      //todo: think of a better way
      public IList DefinedData => _definedData;

      public List<int> DefinitionLevels => _definitionLevels;

      public int TotalCount => _definedData.Count + _undefinedCount;

      // todo: boxing is happening here, must be killed or MSIL-generated
      public void Add(object item)
      {
         if (_field.HasNulls)
         {
            if (item == null)
            {
               _definitionLevels.Add(0);
               _undefinedCount += 1;
               return;
            }

            _definitionLevels.Add(1);
         }

         _definedData.Add(item);
      }

      private void AddRange(IEnumerable data)
      {
         foreach(object item in data)
         {
            Add(item);
         }
      }
   }
}
