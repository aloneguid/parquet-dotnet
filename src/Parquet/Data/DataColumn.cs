using System;
using System.Linq;
using Parquet.Extensions;
using Parquet.Schema;

namespace Parquet.Data {
    /// <summary>
    /// The primary low-level structure to hold data for a parquet column.
    /// Handles internal data composition/decomposition to enrich with custom data Parquet format requires.
    /// </summary>
    public class DataColumn {
        private readonly int _offset;
        private readonly int _count = -1;

        private DataColumn(DataField field) {
            Field = field ?? throw new ArgumentNullException(nameof(field));
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
           int[] repetitionLevels, int maxRepetitionLevel) : this(field) {
            Data = definedData;

            // 1. Apply definitions
            if(definitionLevels != null) {
                Data = field.UnpackDefinitions(Data, definitionLevels, maxDefinitionLevel);
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
        /// Count of elements not including nulls, if statistics contains null count.
        /// </summary>
        public int CountWithoutNulls => (int)(Count - Statistics.NullCount);

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

        internal int CalculateNullCount() {
            return Data.CalculateNullCountFast(Offset, Count);
        }

        internal void PackDefinitions(Span<int> definitions,
            Array data, int dataOffset, int dataCount,
            Array packedData,
            int maxDefinitionLevel) {

            for(int i = dataOffset, y = 0, ir = 0; i < (dataOffset + dataCount); i++, y++) {
                object value = data.GetValue(i);

                if(value == null) {
                    definitions[y] = 0;
                }
                else {
                    definitions[y] = maxDefinitionLevel;
                    packedData.SetValue(value, ir++);
                }
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