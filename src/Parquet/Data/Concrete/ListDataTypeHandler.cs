using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Parquet.Data.Concrete
{
   class ListDataTypeHandler : NonDataDataTypeHandler
   {
      public override SchemaType SchemaType => SchemaType.List;

      public override Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount)
      {
         Thrift.SchemaElement tseList = schema[index];

         ListField listField = ListField.CreateWithNoItem(tseList.Name);

         //https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules

         Thrift.SchemaElement tseRepeated = schema[index + 1];

         //Rule 1. If the repeated field is not a group, then its type is the element type and elements are required.
         //not implemented

         //Rule 2. If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
         //not implemented

         //Rule 3. f the repeated field is a group with one field and is named either array or uses
         //the LIST-annotated group's name with _tuple appended then the repeated type is the element
         //type and elements are required.

         // "group with one field and is named either array":
         if(tseList.Num_children == 1 && tseRepeated.Name == "array")
         {
            listField.Path = tseList.Name;
            index += 1; //only skip this element
            ownedChildCount = 1;
            return listField;
         }

         //as we are skipping elements set path hint
         listField.Path = $"{tseList.Name}{Schema.PathSeparator}{schema[index + 1].Name}";
         index += 2;          //skip this element and child container
         ownedChildCount = 1; //we should get this element assigned back
         return listField;
      }

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
         var list = new Thrift.SchemaElement("list")
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