using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   /// <summary>
   /// Prototype: data type interface
   /// </summary>
   interface IDataType
   {
      bool IsMatch(Thrift.SchemaElement tse);

      SchemaElement Parse(SchemaElement root, ICollection<Thrift.SchemaElement> schema, ref int index);

      void Build(SchemaElement element, IList<Thrift.SchemaElement> schema);

      IList CreateList(bool nullable, int capacity);

      int? BitWidth { get; }
   }
}