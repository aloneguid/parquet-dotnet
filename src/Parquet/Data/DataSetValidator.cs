using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data
{
   class DataSetValidator
   {
      private readonly Schema _schema;
      private readonly DataSet _ds;

      public DataSetValidator(DataSet ds)
      {
         _schema = ds.Schema;
         _ds = ds;
      }

      public void ValidateRow(Row row)
      {
         if (row == null) throw new ArgumentNullException(nameof(row));

         ValidateRow(row.RawValues, _schema.Elements);
      }

      private void ValidateRow(object[] values, IReadOnlyList<Field> schema)
      {
         if (values.Length != schema.Count)
            throw new ArgumentException($"the row has {values.Length} values but schema expects {schema.Count}", nameof(values));

         for (int i = 0; i < values.Length; i++)
         {
            object value = values[i];
            Field se = schema[i];

            switch(se.SchemaType)
            {
               case SchemaType.PrimitiveType:
                  values[i] = ValidatePrimitive((DataField)se, value);
                  break;
            }


         }
      }

      private object ValidatePrimitive(DataField df, object value)
      {
         if (value == null)
         {
            if (!df.HasNulls)
               throw new ArgumentException($"element is null but column '{df.Name}' does not accept nulls");
         }
         else
         {
            Type vt = value.GetType();
            if (value.GetType() != df.ClrType)
            {
               if (TrySmartConvert(vt, df.ClrType, value, out object convertedValue))
               {
                  return convertedValue;
               }
               else
               {
                  throw new ArgumentException($"expected '{df.ClrType}' but found '{vt}' in column '{df.Name}'");
               }

            }
         }

         return value;
      }

      private static bool TrySmartConvert(Type passedType, Type requiredType, object value, out object convertedValue)
      {
         if (passedType == typeof(DateTime) && requiredType == typeof(DateTimeOffset))
         {
            convertedValue = new DateTimeOffset((DateTime)value);
            return true;
         }

         convertedValue = null;
         return false;
      }

   }
}
