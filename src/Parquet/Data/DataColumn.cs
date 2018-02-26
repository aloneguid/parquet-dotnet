using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data
{
   /// <summary>
   /// Column to hold data for a Parquet file
   /// </summary>
   internal class DataColumn
   {
      private readonly DataField _field;
      private readonly IList _data;

      public DataColumn(DataField field, IList data)
      {
         _field = field ?? throw new ArgumentNullException(nameof(field));
         _data = data;
      }

      public bool HasRepetitions => false;

      public bool HasDefinitions => _field.HasNulls;
   }
}
