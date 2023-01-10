using System;
using System.Collections.Generic;
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
    }
}