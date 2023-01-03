using System;
using System.Buffers;
using System.Collections;
using System.Linq;
using Parquet.Schema;

namespace Parquet.Data {
    /// <summary>
    /// The primary low-level structure to hold data for a parquet column.
    /// Handles internal data composition/decomposition to enrich with custom data Parquet format requires.
    /// </summary>
    public class DataColumn {
        private readonly IDataTypeHandler _dataTypeHandler;

        private readonly int _offset;
        private readonly int _count = -1;

        private DataColumn(DataField field) {
            Field = field ?? throw new ArgumentNullException(nameof(field));

            _dataTypeHandler = DataTypeFactory.Match(field.DataType);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="field"></param>
        /// <param name="data"></param>
        /// <param name="repetitionLevels"></param>
        public DataColumn(DataField field, Array data, int[] repetitionLevels = null) : this(field, data, 0, -1, repetitionLevels) {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="field"></param>
        /// <param name="data"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <param name="repetitionLevels"></param>
        public DataColumn(DataField field, Array data, int offset, int count, int[] repetitionLevels = null) : this(field) {
            Data = data ?? throw new ArgumentNullException(nameof(data));
            _offset = offset;
            _count = count;

            RepetitionLevels = repetitionLevels;
        }

        internal DataColumn(DataField field,
           Array definedData,
           int[] definitionLevels, int maxDefinitionLevel,
           int[] repetitionLevels, int maxRepetitionLevel,
           Array dictionary,
           int[] dictionaryIndexes) : this(field) {
            Data = definedData;

            // 1. Apply definitions
            if(definitionLevels != null) {
                Data = _dataTypeHandler.UnpackDefinitions(Data, definitionLevels, maxDefinitionLevel);
            }

            // 2. Apply repetitions
            RepetitionLevels = repetitionLevels;
        }

        /// <summary>
        /// Column data where definition levels are already applied
        /// </summary>
        public Array Data { get; private set; }

        /// <summary>
        /// Offset of the array
        /// </summary>
        public int Offset => _offset;

        /// <summary>
        /// Length of the array
        /// </summary>
        public int Count => _count > -1 ? _count : Data.Length;

        /// <summary>
        /// Repetition levels if any.
        /// </summary>
        public int[] RepetitionLevels { get; private set; }

        /// <summary>
        /// Data field
        /// </summary>
        public DataField Field { get; set; }

        /// <summary>
        /// When true, this field has repetitions. It doesn't mean that it's an array though. This property simply checks that
        /// repetition levels are present on this column.
        /// </summary>
        public bool HasRepetitions => RepetitionLevels != null;

        /// <summary>
        /// Basic statistics for this data column (populated on read)
        /// </summary>
        public DataColumnStatistics Statistics { get; internal set; } = new DataColumnStatistics(0, 0, null, null);

        internal ArrayView PackDefinitions(int maxDefinitionLevel, out int[] pooledDefinitionLevels, out int definitionLevelCount, out int nullCount) {
            pooledDefinitionLevels = ArrayPool<int>.Shared.Rent(Count);
            definitionLevelCount = Count;

            bool isNullable = Field.ClrType.IsNullable() || Data.GetType().GetElementType().IsNullable();

            if(!Field.HasNulls || !isNullable) {
                SetPooledDefinitionLevels(maxDefinitionLevel, pooledDefinitionLevels);  // all defined, all ones
                nullCount = 0; //definitely no nulls here
                return new ArrayView(Data, Offset, Count);
            }

            return _dataTypeHandler.PackDefinitions(Data, Offset, Count, maxDefinitionLevel, out pooledDefinitionLevels, out definitionLevelCount, out nullCount);
        }

        void SetPooledDefinitionLevels(int maxDefinitionLevel, int[] pooledDefinitionLevels) {
            for(int i = 0; i < Count; i++) {
                pooledDefinitionLevels[i] = maxDefinitionLevel;
            }
        }

        internal long CalculateRowCount() {
            if(Field.MaxRepetitionLevel > 0) {
                return RepetitionLevels.Count(rl => rl == 0);
            }

            return Count;
        }

        /// <summary>
        /// pretty print
        /// </summary>
        public override string ToString() {
            return Field.ToString();
        }
    }
}