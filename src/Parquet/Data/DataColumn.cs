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
      private readonly List<int> _repetitionLevels;
      private int _undefinedCount;
      private int _currentRepetitionLevel = 0;
      private int _touchedRepetitionLevel = 0;

      public DataColumn(DataField field)
      {
         _field = field ?? throw new ArgumentNullException(nameof(field));

         IDataTypeHandler handler = DataTypeFactory.Match(field.DataType);
         _definedData = handler.CreateEmptyList(false, false, 0);       // always a plain list, always non-nullable when possible
         _definitionLevels = new List<int>();

         HasRepetitions = field.IsArray;
         _repetitionLevels = HasRepetitions ? new List<int>() : null;
      }

      internal DataColumn(DataField field,
         Array definedData,
         int[] definitionLevels, int maxDefinitionLevel,
         int[] repetitionLevels, int maxRepetitionLevel,
         Array dictionary) : this(field)
      {
         if (dictionary != null) throw new NotSupportedException("dictionaries not yet supported in V3");
         if (repetitionLevels != null) throw new NotSupportedException("repetitions not yet supported in V3");

         Data = definedData;

         if(definitionLevels != null)
         {
            Data = UnpackDefinitions(field, definedData, definitionLevels, maxDefinitionLevel);
         }

         _definedData.AddOneByOne(definedData);
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

      public Array Data { get; private set; }

      public DataField Field => _field;

      public bool HasRepetitions { get; private set; }

      //todo: think of a better way
      public IList DefinedData => _definedData;

      public List<int> DefinitionLevels => _definitionLevels;

      public List<int> RepetitionLevels => _repetitionLevels;

      public int TotalCount => _definedData.Count + _undefinedCount;

      private static Array UnpackDefinitions(DataField df, Array src, int[] definitonLevels, int maxDefinitionLevel)
      {
         Array result = Array.CreateInstance(df.ClrNullableIfHasNullsType, definitonLevels.Length);

         int isrc = 0;
         for(int i = 0; i < definitonLevels.Length; i++)
         {
            int level = definitonLevels[i];
            if(level == maxDefinitionLevel)
            {
               result.SetValue(src.GetValue(isrc++), i);
            }
         }

         return result;
      }

      public void IncrementLevel()
      {
         _currentRepetitionLevel += 1;
      }

      public void DecrementLevel()
      {
         _currentRepetitionLevel -= 1;
         _touchedRepetitionLevel = _currentRepetitionLevel;
      }

      // todo: boxing is happening here, must be killed or MSIL-generated
      public void Add(object item)
      {
         if (item == null)
         {
            _definitionLevels.Add(0);
            _undefinedCount += 1;
            return;
         }

         _definitionLevels.Add(1);

         _definedData.Add(item);

         if (HasRepetitions)
         {
            _repetitionLevels.Add(_touchedRepetitionLevel);
            _touchedRepetitionLevel = _currentRepetitionLevel;
         }
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
