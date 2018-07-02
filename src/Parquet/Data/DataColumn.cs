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
   public class DataColumn
   {
      private DataColumn(DataField field)
      {
         Field = field ?? throw new ArgumentNullException(nameof(field));

         IDataTypeHandler handler = DataTypeFactory.Match(field.DataType);
         HasRepetitions = field.IsArray;
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="field"></param>
      /// <param name="data"></param>
      /// <param name="repetitionLevels"></param>
      public DataColumn(DataField field, Array data, int[] repetitionLevels = null) : this(field)
      {
         Data = data ?? throw new ArgumentNullException(nameof(data));

         RepetitionLevels = repetitionLevels;
      }

      internal DataColumn(DataField field,
         Array definedData,
         int[] definitionLevels, int maxDefinitionLevel,
         int[] repetitionLevels, int maxRepetitionLevel,
         Array dictionary,
         int[] dictionaryIndexes) : this(field)
      {
         Data = definedData;

         // 1. Dictionary merge
         if (dictionary != null)
         {
            Data = MergeDictionary(dictionary, dictionaryIndexes);
         }

         // 2. Apply definitions
         if(definitionLevels != null)
         {
            Data = UnpackDefinitions(field, Data, definitionLevels, maxDefinitionLevel);
         }

         // 3. Apply repetitions
         RepetitionLevels = repetitionLevels;
      }

      /// <summary>
      /// Column data
      /// </summary>
      public Array Data { get; private set; }

      /// <summary>
      /// Repetition levels if any.
      /// </summary>
      public int[] RepetitionLevels { get; private set; }

      /// <summary>
      /// Data field
      /// </summary>
      public DataField Field { get; private set; }

      /// <summary>
      /// 
      /// </summary>
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

      internal Array PackDefinitions(int maxDefinitionLevel, out int[] pooledDefinitionLevels, out int definitionLevelCount)
      {
         pooledDefinitionLevels = ArrayPool<int>.Shared.Rent(Data.Length);
         definitionLevelCount = Data.Length;

         if (!Field.HasNulls)
         {
            for(int i = 0; i < Data.Length; i++)
            {
               pooledDefinitionLevels[i] = maxDefinitionLevel;
            }

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
         for(int i = 0; i < Data.Length; i++)
         {
            object value = Data.GetValue(i);
            if(value == null)
            {
               pooledDefinitionLevels[i] = 0;
            }
            else
            {
               pooledDefinitionLevels[i] = maxDefinitionLevel;
               result.SetValue(value, ir++);
            }
         }
         return result;
      }

      private Array MergeDictionary(Array dictionary, int[] indexes)
      {
         Array result = Array.CreateInstance(Field.ClrType, indexes.Length);

         for(int i = 0; i < indexes.Length; i++)
         {
            int index = indexes[i];
            object value = dictionary.GetValue(index);
            result.SetValue(value, i);
         }

         return result;
      }
   }
}
