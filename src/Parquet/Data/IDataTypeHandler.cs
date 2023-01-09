﻿using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data {
    /// <summary>
    /// Various operations related to a parquet data type. This interface will be eventually deprecated in favor of low-level optimisations.
    /// </summary>
    interface IDataTypeHandler {
        /// <summary>
        /// Called by the library to determine if this data handler can be used in current schema position
        /// </summary>
        /// <param name="tse"></param>
        /// <param name="formatOptions"></param>
        /// <returns></returns>
        bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions);

        Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount);

        void CreateThrift(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container);

        DataType DataType { get; }

        SchemaType SchemaType { get; }

        Type ClrType { get; }

        int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset);

        /// <summary>
        /// Reads a single value from the current stream position. This shouldn't be used normally, however it's useful when decoding parquet stats.
        /// </summary>
        /// <param name="reader">Incoming data</param>
        /// <param name="tse">Schema element</param>
        /// <param name="length">Number of bytes to read (type specific). Pass -1 to read the length from incoming stream if you don't know how long the buffer is.</param>
        object Read(BinaryReader reader, Thrift.SchemaElement tse, int length);

        void Write(Thrift.SchemaElement tse, BinaryWriter writer, ArrayView values, DataColumnStatistics statistics);

        /// <summary>
        /// Creates or rents a native array
        /// </summary>
        /// <param name="minCount">Minimum element count. Realistically there could be more elements than you've asked for only when arrays are rented.</param>
        /// <param name="rent">Rent or create</param>
        /// <param name="isNullable">Nullable elements or not</param>
        /// <returns></returns>
        Array GetArray(int minCount, bool rent, bool isNullable);

        Array MergeDictionary(Array dictionary, int[] indexes, Array data, int offset, int length);

        ArrayView PackDefinitions(Array data, int offset, int count, int maxDefinitionLevel, out int[] definitions, out int definitionsLength, out int nullCount);

        Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel);

        object PlainDecode(Thrift.SchemaElement tse, byte[] encoded);
    }
}