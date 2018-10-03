using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class MapDataTypeHandler : NonDataDataTypeHandler
   {
      public override SchemaType SchemaType => SchemaType.Map;

      public override Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount)
      {
         Thrift.SchemaElement tseRoot = schema[index];

         //next element is a container
         Thrift.SchemaElement tseContainer = schema[++index];

         if (tseContainer.Num_children != 2)
         {
            throw new IndexOutOfRangeException($"dictionary container must have exactly 2 children but {tseContainer.Num_children} found");
         }

         //followed by a key and a value, but we declared them as owned

         var map = new MapField(tseRoot.Name);
         map.Path = tseRoot.Name + Schema.PathSeparator + tseContainer.Name;

         index += 1;
         ownedChildCount = 2;
         return map;
      }

      public override void CreateThrift(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         parent.Num_children += 1;

         //add the root container where map begins
         var root = new Thrift.SchemaElement(field.Name)
         {
            Converted_type = Thrift.ConvertedType.MAP,
            Num_children = 1,
            Repetition_type = Thrift.FieldRepetitionType.OPTIONAL
         };
         container.Add(root);

         //key-value is a container for column of keys and column of values
         var keyValue = new Thrift.SchemaElement(MapField.ContainerName)
         {
            Num_children = 0, //is assigned by children
            Repetition_type = Thrift.FieldRepetitionType.REPEATED
         };
         container.Add(keyValue);

         //now add the key and value separately
         MapField mapField = field as MapField;
         IDataTypeHandler keyHandler = DataTypeFactory.Match(mapField.Key);
         IDataTypeHandler valueHandler = DataTypeFactory.Match(mapField.Value);

         keyHandler.CreateThrift(mapField.Key, keyValue, container);
         Thrift.SchemaElement tseKey = container[container.Count - 1];
         valueHandler.CreateThrift(mapField.Value, keyValue, container);
         Thrift.SchemaElement tseValue = container[container.Count - 1];

         //fixups for weirdness in RLs
         if (tseKey.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
            tseKey.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
         if (tseValue.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
            tseValue.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
      }

      public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            tse.__isset.converted_type &&
            (tse.Converted_type == Thrift.ConvertedType.MAP || tse.Converted_type == Thrift.ConvertedType.MAP_KEY_VALUE);
      }
   }
}