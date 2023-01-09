using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data {

    abstract class BasicDataTypeHandler<TSystemType> : IDataTypeHandler, IComparer<TSystemType>, IEqualityComparer<TSystemType> {
        private readonly Thrift.Type _thriftType;
        private readonly Thrift.ConvertedType? _convertedType;
        private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

        public BasicDataTypeHandler(DataType dataType, Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null) {
            DataType = dataType;
            _thriftType = thriftType;
            _convertedType = convertedType;
        }

        public DataType DataType { get; private set; }

        public SchemaType SchemaType => SchemaType.Data;

        public Type ClrType => typeof(TSystemType);

        public virtual bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return
               tse.__isset.type && _thriftType == tse.Type &&
               (_convertedType == null || (tse.__isset.converted_type && tse.Converted_type == _convertedType.Value));
        }

        public virtual Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount) {
            Thrift.SchemaElement tse = schema[index++];

            bool hasNulls = (tse.Repetition_type != Thrift.FieldRepetitionType.REQUIRED);
            bool isArray = (tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED);

            Field simple = CreateSimple(tse, hasNulls, isArray);
            ownedChildCount = 0;
            return simple;
        }

        protected virtual DataField CreateSimple(Thrift.SchemaElement tse, bool hasNulls, bool isArray) {
            return new DataField(tse.Name, DataType, hasNulls, isArray);
        }

        public virtual int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset) {
            return Read(tse, reader, (TSystemType[])dest, offset);
        }

        public virtual object Read(BinaryReader reader, Thrift.SchemaElement tse, int length) {
            return ReadSingle(reader, tse, length);
        }

        protected abstract TSystemType ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length);

        private int Read(Thrift.SchemaElement tse, BinaryReader reader, TSystemType[] dest, int offset) {
            int totalLength = (int)reader.BaseStream.Length;
            int idx = offset;
            Stream s = reader.BaseStream;

            while(s.Position < totalLength && idx < dest.Length) {
                TSystemType element = ReadSingle(reader, tse, -1);  //potential performance hit on calling a method
                dest[idx++] = element;
            }

            return idx - offset;
        }

        public virtual void Write(Thrift.SchemaElement tse, BinaryWriter writer, ArrayView values, DataColumnStatistics statistics) {
            foreach(TSystemType one in values.GetValuesAndReturnArray(statistics, this, this)) {
                WriteOne(writer, one);
            }
        }

        public virtual void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            DataField sef = (DataField)se;
            var tse = new Thrift.SchemaElement(se.Name);
            tse.Type = _thriftType;
            if(_convertedType != null)
                tse.Converted_type = _convertedType.Value;

            bool isList = container.Count > 1 && container[container.Count - 2].Converted_type == Thrift.ConvertedType.LIST;

            tse.Repetition_type = sef.IsArray && !isList
               ? Thrift.FieldRepetitionType.REPEATED
               : (sef.HasNulls ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED);
            container.Add(tse);
            parent.Num_children += 1;
        }

        public virtual Array MergeDictionary(Array untypedDictionary, int[] indexes, Array data, int offset, int length) {
            TSystemType[] dictionary = (TSystemType[])untypedDictionary;
            TSystemType[] result = (TSystemType[])data;

            for(int i = 0; i < length; i++) {
                int index = indexes[i];
                if(index < dictionary.Length) {
                    // may not be true when value is null
                    TSystemType value = dictionary[index];
                    result[offset + i] = value;
                }
            }

            return result;
        }

        public abstract Array GetArray(int minCount, bool rent, bool isNullable);

        public abstract ArrayView PackDefinitions(Array data, int offset, int count, int maxDefinitionLevel, out int[] definitions, out int definitionsLength, out int nullCount);

        public abstract Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel);

        protected ArrayView PackDefinitions<TNullable>(TNullable[] data, int offset, int count, int maxDefinitionLevel, out int[] definitionLevels, out int definitionsLength, out int nullCount)
           where TNullable : class {
            definitionLevels = IntPool.Rent(count);
            definitionsLength = count;

            nullCount = 0;
            WritableArrayView<TNullable> result = ArrayView.CreateWritable<TNullable>(count);
            int ir = 0;

            for(int i = offset; i < count; i++) {
                TNullable value = data[i];

                if(value == null) {
                    definitionLevels[i] = 0;
                    nullCount++;
                }
                else {
                    definitionLevels[i] = maxDefinitionLevel;
                    result[ir++] = value;
                }
            }

            return result;
        }

        protected T[] UnpackGenericDefinitions<T>(T[] src, int[] definitionLevels, int maxDefinitionLevel) {
            T[] result = (T[])GetArray(definitionLevels.Length, false, true);

            int isrc = 0;
            for(int i = 0; i < definitionLevels.Length; i++) {
                int level = definitionLevels[i];

                if(level == maxDefinitionLevel) {
                    result[i] = src[isrc++];
                }
            }

            return result;
        }


        #region [ Reader / Writer Helpers ]

        protected virtual void WriteOne(BinaryWriter writer, TSystemType value) {
            throw new NotSupportedException();
        }


        #endregion

        /// <summary>
        /// less than 0 - x &lt; y
        /// 0 - x == y
        /// greater than 0 - x &gt; y
        /// </summary>
        public abstract int Compare(TSystemType x, TSystemType y);

        public abstract bool Equals(TSystemType x, TSystemType y);

        public int GetHashCode(TSystemType x) => x.GetHashCode();

        public abstract object PlainDecode(Thrift.SchemaElement tse, byte[] encoded);
    }
}