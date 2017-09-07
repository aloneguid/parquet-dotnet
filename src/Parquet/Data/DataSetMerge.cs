using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data
{
   /// <summary>
   /// A comparer used to check the equality of different parquet schemas
   /// </summary>
   public class DataSetMerge : IEqualityComparer<SchemaElement>
   {
      /// <summary>
      /// Used to merge and add columns to a dataset 
      /// </summary>
      /// <param name="dataSet1">A primary dataset for the merge</param>
      /// <param name="dataSet2">A second dataset to merge with the primary</param>
      public DataSet Merge(DataSet dataSet1, DataSet dataSet2)
      {
         // Get the new schema that we have to build fold the row copies into a new view
         List<SchemaElement> newSchema = dataSet1.Schema.Elements.Union(dataSet2.Schema.Elements, this).ToList();
         // Create a new dataset form the previous two
         var mergedDs = new DataSet(newSchema);
         TypeCheck(dataSet1, dataSet2);
         mergedDs = AddAdditionalMergedRows(mergedDs, dataSet1);

         return AddAdditionalMergedRows(mergedDs, dataSet2);
      }

      private DataSet AddAdditionalMergedRows(DataSet mergedDs, DataSet valuesDatasSet)
      {
         for (int i = 0; i < valuesDatasSet.RowCount; i++)
         {
            var schemaValues = mergedDs.Schema.Elements.ToDictionary(se => se.Name, se => (object) null);
            Row values = valuesDatasSet[i];
            for (int j = 0; j < valuesDatasSet.ColumnCount; j++)
            {
               if (schemaValues.Keys.Contains(valuesDatasSet.Schema.Elements[j].Name))
               {
                  schemaValues[valuesDatasSet.Schema.Elements[j].Name] = values[j];
               }
            }
            mergedDs.Add(schemaValues.Values.ToArray());
         }
         return mergedDs;
      }

      private void NullCheck(DataSet ds)
      {
         foreach (SchemaElement element in ds.Schema.Elements)
         {
            if (!element.IsNullable)
            {
               throw new ParquetException($"unable to merge schema, {element.Name} is not nullable");
            }
         }
      }

      private void TypeCheck(DataSet ds1, DataSet ds2)
      {
         foreach (SchemaElement schemaElement1 in ds1.Schema.Elements)
         {
            foreach (SchemaElement schemaElement2 in ds2.Schema.Elements)
            {
               if (schemaElement1.Name == schemaElement2.Name &&
                   schemaElement1.ElementType != schemaElement2.ElementType)
               {
                  throw new ParquetException(
                     $"unable to merge schema, {schemaElement1.Name} has different types in schemas");
               }
            }
         }
      }
      /// <summary>
      /// Checks to see whether two schema elements are equal or not
      /// </summary>
      /// <param name="x">The first SchemaElement</param>
      /// <param name="y">The second SchemaElement</param>
      /// <returns>Equality check</returns>
      public bool Equals(SchemaElement x, SchemaElement y)
      {
         return x.Equals(y);
      }
      /// <summary>
      /// Gets a unique has for the comparer 
      /// </summary>
      /// <param name="obj">The SchemaElement to check</param>
      /// <returns>An int value for uniqueness based on name and type</returns>
      public int GetHashCode(SchemaElement obj)
      {
         return ASCIIEncoding.ASCII.GetBytes(String.Concat(obj.Name, obj.ElementType.Name)).Sum(item => (int)item);
      }
   }
}
