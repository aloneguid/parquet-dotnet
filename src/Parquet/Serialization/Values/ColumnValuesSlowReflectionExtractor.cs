using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Parquet.Data;

namespace Parquet.Serialization.Values
{
   /// <summary>
   /// Extremely slow reflection based column extractor used to validate the concept and simply compare performance.
   /// </summary>
   class ColumnValuesSlowReflectionExtractor : IColumnValuesExtractor
   {
      private readonly Type _classType;
      private readonly List<DataColumn> _columns;

      public ColumnValuesSlowReflectionExtractor(Type classType, List<DataColumn> columns)
      {
         _classType = classType;
         _columns = columns;
      }

      public void ExtractToList(IEnumerable classInstances)
      {
         foreach(object ci in classInstances)
         {
            Extract(ci);
         }
      }

      private void Extract(object classInstance)
      {
         foreach(DataColumn dc in _columns)
         {
            AddValue(classInstance, dc);
         }
      }

      private void AddValue(object classInstance, DataColumn dc)
      {
         //get the value
         object value = null;
         TypeInfo ti = _classType.GetTypeInfo();
         PropertyInfo pi = ti.GetDeclaredProperty(dc.Field.Name);
         if (pi != null)
         {
            value = pi.GetValue(classInstance);
         }

         //add value

         if (dc.Field.IsArray)
         {
            IEnumerable ienum = value as IEnumerable;
            if(ienum != null)
            {
               dc.IncrementLevel();
               foreach(object element in ienum)
               {
                  dc.Add(element);
               }
               dc.DecrementLevel();
            }
         }
         else
         {
            dc.Add(value);
         }
      }
   }
}
