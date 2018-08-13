using System;
using System.Buffers;

namespace Parquet.Data
{
   /// <summary>
   /// The primary low-level structure to hold data for a parqut column.
   /// Handles internal data composition/decomposition to enrich with custom data Parquet format requires.
   /// </summary>
   public class DataColumn
   {
      private readonly IDataTypeHandler _dataTypeHandler;

      private DataColumn(DataField field)
      {
         Field = field ?? throw new ArgumentNullException(nameof(field));

         _dataTypeHandler = DataTypeFactory.Match(field.DataType);
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
            Data = _dataTypeHandler.MergeDictionary(dictionary, dictionaryIndexes);
         }

         // 2. Apply definitions
         if(definitionLevels != null)
         {
            Data = _dataTypeHandler.UnpackDefinitions(Data, definitionLevels, maxDefinitionLevel);
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

      internal Array PackDefinitions(int maxDefinitionLevel, out int[] pooledDefinitionLevels, out int definitionLevelCount)
      {
         pooledDefinitionLevels = ArrayPool<int>.Shared.Rent(Data.Length);
         definitionLevelCount = Data.Length;

         bool isNullable = Field.ClrType.IsNullable() || Data.GetType().GetElementType().IsNullable();

         if (!Field.HasNulls || !isNullable)
         {
            SetPooledDefinitionLevels(maxDefinitionLevel, pooledDefinitionLevels);
            return Data;
         }

         return _dataTypeHandler.PackDefinitions(Data, maxDefinitionLevel, out pooledDefinitionLevels, out definitionLevelCount);
      }

      void SetPooledDefinitionLevels(int maxDefinitionLevel, int[] pooledDefinitionLevels)
      {
         for (int i = 0; i < Data.Length; i++)
         {
            pooledDefinitionLevels[i] = maxDefinitionLevel;
         }
      }
   }
}
