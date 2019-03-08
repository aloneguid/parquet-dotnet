using System;
using System.Buffers;
using System.Linq;

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

         // 1. Apply definitions
         if (definitionLevels != null)
         {
            bool[] hasValueFlags;
            Data = _dataTypeHandler.UnpackDefinitions(Data, definitionLevels, maxDefinitionLevel, out hasValueFlags);
            HasValueFlags = hasValueFlags;
         }

         // 2. Apply repetitions
         RepetitionLevels = repetitionLevels;
      }

      /// <summary>
      /// Column data where definition levels are already applied
      /// </summary>
      public Array Data { get; private set; }

      /// <summary>
      /// Repetition levels if any.
      /// </summary>
      public int[] RepetitionLevels { get; private set; }

      internal bool[] HasValueFlags { get; set; }

      /// <summary>
      /// Data field
      /// </summary>
      public DataField Field { get; private set; }

      /// <summary>
      /// When true, this field has repetitions. It doesn't mean that it's an array though. This property simply checks that
      /// repetition levels are present on this column.
      /// </summary>
      public bool HasRepetitions => RepetitionLevels != null;

      internal Array PackDefinitions(int maxDefinitionLevel, out int[] pooledDefinitionLevels, out int definitionLevelCount, out int nullCount)
      {
         pooledDefinitionLevels = ArrayPool<int>.Shared.Rent(Data.Length);
         definitionLevelCount = Data.Length;

         bool isNullable = Field.ClrType.IsNullable() || Data.GetType().GetElementType().IsNullable();

         if (!Field.HasNulls || !isNullable)
         {
            SetPooledDefinitionLevels(maxDefinitionLevel, pooledDefinitionLevels);
            nullCount = 0; //definitely no nulls here
            return Data;
         }

         return _dataTypeHandler.PackDefinitions(Data, maxDefinitionLevel, out pooledDefinitionLevels, out definitionLevelCount, out nullCount);
      }

      void SetPooledDefinitionLevels(int maxDefinitionLevel, int[] pooledDefinitionLevels)
      {
         for (int i = 0; i < Data.Length; i++)
         {
            pooledDefinitionLevels[i] = maxDefinitionLevel;
         }
      }

      internal long CalculateRowCount()
      {
         if(Field.MaxRepetitionLevel > 0)
         {
            return RepetitionLevels.Count(rl => rl == 0);
         }

         return Data.Length;
      }

      /// <summary>
      /// pretty print
      /// </summary>
      public override string ToString()
      {
         return Field.ToString();
      }
   }
}
