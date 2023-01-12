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

        Array MergeDictionary(Array dictionary, int[] indexes, Array data, int offset, int length);
    }
}