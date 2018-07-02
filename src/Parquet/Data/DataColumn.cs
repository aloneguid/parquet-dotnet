using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data
{
   /// <summary>
   /// The primary low-level structure to hold data for a parqut column.
   /// Handles internal data composition/decomposition to enrich with custom data Parquet format requires.
   /// </summary>
   internal class DataColumn
   {
      public DataColumn(DataField field)
      {
         Field = field ?? throw new ArgumentNullException(nameof(field));

         IDataTypeHandler handler = DataTypeFactory.Match(field.DataType);
         HasRepetitions = field.IsArray;
      }

      public DataColumn(DataField field, Array data) : this(field)
      {
         Data = data ?? throw new ArgumentNullException(nameof(data));
      }

      internal DataColumn(DataField field,
         Array definedData,
         int[] definitionLevels, int maxDefinitionLevel,
         int[] repetitionLevels, int maxRepetitionLevel,
         Array dictionary) : this(field)
      {
         if (dictionary != null) throw new NotSupportedException("dictionaries not yet supported in V3");

         /*
          1. dictionary
          2. definitions
          3. repetitions
          */

         Data = definedData;

         if(definitionLevels != null)
         {
            Data = UnpackDefinitions(field, definedData, definitionLevels, maxDefinitionLevel);
         }

         if (repetitionLevels != null) throw new NotSupportedException("repetitions not yet supported in V3");
      }

      public Array Data { get; private set; }

      public int[] RepetitionLevels { get; private set; }

      public DataField Field { get; private set; }

      public bool HasRepetitions { get; private set; }

      private static Array UnpackDefinitions(DataField df, Array src, int[] definitonLevels, int maxDefinitionLevel)
      {
         Array result = Array.CreateInstance(df.ClrNullableIfHasNullsType, definitonLevels.Length);

         int isrc = 0;
         for(int i = 0; i < definitonLevels.Length; i++)
         {
            int level = definitonLevels[i];
            if(level == maxDefinitionLevel)
            {
               result.SetValue(src.GetValue(isrc++), i);
            }
         }

         return result;
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="maxDefinitionLevel"></param>
      /// <param name="definitionLevels">Rented array where length can be greater than needed. You have to release it after use.</param>
      /// <param name="definitionLevelCount"></param>
      /// <returns></returns>
      internal Array PackDefinitions(int maxDefinitionLevel, out int[] definitionLevels, out int definitionLevelCount)
      {
         if (!Field.HasNulls)
         {
            definitionLevels = null;
            definitionLevelCount = 0;
            return Data;
         }

         //get count of nulls
         int nullCount = 0;
         for(int i = 0; i < Data.Length; i++)
         {
            bool isNull = (Data.GetValue(i) == null);
            if (isNull) nullCount += 1;
         }

         //pack
         Array result = Array.CreateInstance(Field.ClrType, Data.Length - nullCount);
         int ir = 0;
         //definitionLevels = new int[Data.Length];
         definitionLevels = ArrayPool<int>.Shared.Rent(Data.Length);
         definitionLevelCount = Data.Length;
         for(int i = 0; i < Data.Length; i++)
         {
            object value = Data.GetValue(i);
            if(value == null)
            {
               definitionLevels[i] = 0;
            }
            else
            {
               definitionLevels[i] = maxDefinitionLevel;
               result.SetValue(value, ir++);
            }
         }
         return result;
      }
   }
}
