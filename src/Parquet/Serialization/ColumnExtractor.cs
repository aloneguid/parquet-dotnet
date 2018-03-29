using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.Serialization.Values;

namespace Parquet.Serialization
{
   /// <summary>
   /// Extracts data from CLR structures
   /// </summary>
   internal class ColumnExtractor
   {
      /// <summary>
      /// Creates a new instnce of <see cref="ColumnExtractor"/>
      /// </summary>
      public ColumnExtractor()
      {
         
      }

      /// <summary>
      /// Extracts data columns from a collection of CLR class instances
      /// </summary>
      /// <typeparam name="TClass">Class type</typeparam>
      /// <param name="classInstances">Collection of class instances</param>
      /// <param name="schema">Schema to operate on</param>
      public List<DataColumn> ExtractColumns<TClass>(IEnumerable<TClass> classInstances, Schema schema)
      {
         List<DataField> dataFields = schema.GetDataFields();
         List<DataColumn> result = dataFields.Select(df => new DataColumn(df)).ToList();

         var valuesExtractor = new ColumnValuesSlowReflectionExtractor(typeof(TClass), result);
         valuesExtractor.ExtractToList(classInstances);

         return result;
      }
   }
}
