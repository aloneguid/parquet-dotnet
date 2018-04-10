using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.Serialization.Values
{
   /// <summary>
   /// Responsible for mapping data columns to CLR objects and vice versa
   /// </summary>
   interface IColumnClrMapper
   {
      IReadOnlyCollection<DataColumn> ExtractDataColumns(IReadOnlyCollection<DataField> dataFields, IEnumerable classInstances);

      IReadOnlyCollection<T> CreateClassInstances<T>(IReadOnlyCollection<DataColumn> columns) where T : new();
   }
}
