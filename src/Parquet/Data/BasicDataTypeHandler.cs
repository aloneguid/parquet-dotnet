using System;
using System.Buffers;
using System.Collections.Generic;
using Parquet.Schema;

namespace Parquet.Data {

    abstract class BasicDataTypeHandler<TSystemType> : IDataTypeHandler {
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
    }
}