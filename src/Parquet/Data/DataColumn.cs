using System;
using System.Collections;

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

      public DataColumn(DataField field, IEnumerable data)
      {
         _field = field ?? throw new ArgumentNullException(nameof(field));

         if(!data.GetType().TryExtractEnumerableType(out Type baseType))
         {
            throw new ArgumentException($"the collection is not a generic one", nameof(data));
         }

         if (baseType != field.ClrType) throw new ArgumentException($"expected {_field.ClrType} but passed a collection of {baseType}");
      }

      public bool HasRepetitions => false;

      public bool HasDefinitions => _field.HasNulls;
   }
}
