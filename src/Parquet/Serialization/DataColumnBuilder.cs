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
   class DataColumnBuilder
   {
      /// <summary>
      /// Creates a new instnce of <see cref="DataColumnBuilder"/>
      /// </summary>
      public DataColumnBuilder()
      {
         
      }

      /// <summary>
      /// Extracts data columns from a collection of CLR class instances
      /// </summary>
      /// <typeparam name="TClass">Class type</typeparam>
      /// <param name="classInstances">Collection of class instances</param>
      /// <param name="schema">Schema to operate on</param>
      public IReadOnlyCollection<DataColumn> BuildColumns<TClass>(IReadOnlyCollection<TClass> classInstances, Schema schema)
      {
         List<DataField> dataFields = schema.GetDataFields();

         var bridge = new ClrBridge(typeof(TClass));

         return dataFields
            .Select(df => bridge.BuildColumn(df, classInstances, classInstances.Count))
            .ToList();
      }
   }
}
