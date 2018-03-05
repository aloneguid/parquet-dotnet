using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet.Data
{
   /// <summary>
   /// The primary low-level structure to hold data for a parqut column.
   /// Handles internal data composition/decomposition to enrich with custom data Parquet format requires.
   /// </summary>
   internal class DataColumn
   {
      private readonly DataField _field;
      private readonly IList _data;
      private readonly List<int> _definitionLevels = new List<int>();

      public DataColumn(DataField field)
      {
         _field = field ?? throw new ArgumentNullException(nameof(field));

         IDataTypeHandler handler = DataTypeFactory.Match(field.DataType);
         _data = handler.CreateEmptyList(field.HasNulls, field.IsArray, 0);
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
      public IList Data => _data;

      public List<int> DefinitionLevels => _definitionLevels;

      // todo: boxing is happening here, must be killed or MSIL-generated
      public void Add(object item)
      {
         if(item == null)
         {
            _definitionLevels.Add(0);
            return;
         }

         _definitionLevels.Add(1);
         _data.Add(item);
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
