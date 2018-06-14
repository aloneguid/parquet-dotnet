using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Parquet.Data.Concrete
{
   class StructureDataTypeHandler : IDataTypeHandler
   {
      public DataType DataType => DataType.Unspecified;

      public SchemaType SchemaType => SchemaType.Struct;

      public Type ClrType => null;

      public IList CreateEmptyList(bool isNullable, bool isArray, int capacity)
      {
         throw new NotSupportedException("structures cannot have row values");
      }

      public Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount)
      {
         Thrift.SchemaElement container = schema[index++];

         ownedChildCount = container.Num_children; //make then owned to receive in .Assign()
         return StructField.CreateWithNoElements(container.Name);
      }

      public void CreateThrift(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         StructField structField = (StructField)field;

         Thrift.SchemaElement tseStruct = new Thrift.SchemaElement(field.Name)
         {
            Repetition_type = Thrift.FieldRepetitionType.OPTIONAL,
         };
         container.Add(tseStruct);
         parent.Num_children += 1;

         foreach(Field cf in structField.Fields)
         {
            IDataTypeHandler handler = DataTypeFactory.Match(cf);
            handler.CreateThrift(cf, tseStruct, container);
         }
      }

      public bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            tse.Num_children > 0;
      }

      public IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         throw new NotSupportedException();
      }

      public int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset, ParquetOptions formatOptions)
      {
         throw new NotSupportedException();
      }

      public void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         throw new NotSupportedException();
      }
   }
}
