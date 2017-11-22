using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet.Data
{
   static class DataSetValidator
   {
      public static void ValidateRow(Row row, IReadOnlyList<Field> schema)
      {
         if (row == null) throw new ArgumentNullException(nameof(row));

         ValidateRow(row.RawValues, schema);
      }

      private static void ValidateRow(object[] values, IReadOnlyList<Field> schema)
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

               //todo: validate the rest of the schemas
            }


         }
      }

      private static object ValidatePrimitive(DataField df, object value)
      {
         if (value == null)
         {
            if (!df.HasNulls)
               throw new ArgumentException($"element is null but column '{df.Name}' does not accept nulls");
         }
         else
         {
            Type vt = value.GetType();
            Type et = df.ClrType;
            if (vt.IsNullable()) vt = vt.GetNonNullable();

            if(vt.TryExtractEnumerableType(out Type enumElementType))
            {
               if(!df.IsArray)
               {
                  throw new ArgumentException($"element is an array but non-array type ({vt}) is passed in column '{df.Name}''");
               }

               vt = enumElementType;
            }

            if (vt != et)
            {
               if (TrySmartConvertPrimitive(vt, et, value, out object convertedValue))
               {
                  return convertedValue;
               }
               else
               {
                  throw new ArgumentException($"expected '{df.ClrType}' but found '{vt}' in column '{df.Name}'");
               }

            }

            if (df.IsArray)
            {
               return df.CreateGenericList((IEnumerable)value);
            }

         }

         return value;
      }

      private static bool TrySmartConvertPrimitive(Type passedType, Type requiredType, object value, out object convertedValue)
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
