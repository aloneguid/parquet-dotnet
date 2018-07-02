using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Parquet.Data;

namespace Parquet.Serialization.Values
{
   /// <summary>
   /// Extremely slow reflection based column extractor used to validate the concept and simply compare performance.
   /// </summary>
   class SlowReflectionColumnClrMapper : IColumnClrMapper
   {
      private readonly Type _classType;

      public SlowReflectionColumnClrMapper(Type classType)
      {
         _classType = classType;
      }

      public IReadOnlyCollection<DataColumn> ExtractDataColumns(IReadOnlyCollection<DataField> dataFields, IEnumerable classInstances)
      {
         List<DataColumn> result = dataFields.Select(df => new DataColumn(df, null)).ToList();

         foreach (object ci in classInstances)
         {
            Extract(result, ci);
         }

         return result;
      }

      private void Extract(IReadOnlyCollection<DataColumn> columns, object classInstance)
      {
         foreach(DataColumn dc in columns)
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
            throw new NotSupportedException();
            /*IEnumerable ienum = value as IEnumerable;
            if(ienum != null)
            {
               dc.IncrementLevel();
               foreach(object element in ienum)
               {
                  dc.Add(element);
               }
               dc.DecrementLevel();
            }*/
         }
         else
         {
            //dc.Add(value);
         }
      }

      public IReadOnlyCollection<T> CreateClassInstances<T>(IReadOnlyCollection<DataColumn> columns) where T : new()
      {
         var result = new List<T>();

         throw new NotImplementedException();
      }
   }
}
