using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class ListDataTypeHandler : NonDataDataTypeHandler
   {
      public override SchemaType SchemaType => SchemaType.List;

      public override void CreateThrift(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         ListField listField = (ListField)field;

         parent.Num_children += 1;

         //add list container
         var root = new Thrift.SchemaElement(field.Name)
         {
            Converted_type = Thrift.ConvertedType.LIST,
            Repetition_type = Thrift.FieldRepetitionType.OPTIONAL,
            Num_children = 1  //field container below
         };
         container.Add(root);

         //add field container
         var list = new Thrift.SchemaElement(listField.ContainerName)
         {
            Repetition_type = Thrift.FieldRepetitionType.REPEATED
         };
         container.Add(list);

         //add the list item as well
         IDataTypeHandler fieldHandler = DataTypeFactory.Match(listField.Item);
         fieldHandler.CreateThrift(listField.Item, list, container);
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.LIST;
      }
   }
}