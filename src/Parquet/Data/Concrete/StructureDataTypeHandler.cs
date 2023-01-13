using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class StructureDataTypeHandler : NonDataDataTypeHandler {
        public override SchemaType SchemaType => SchemaType.Struct;

        public override void CreateThrift(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            StructField structField = (StructField)field;

            Thrift.SchemaElement tseStruct = new Thrift.SchemaElement(field.Name) {
                Repetition_type = Thrift.FieldRepetitionType.OPTIONAL,
            };
            container.Add(tseStruct);
            parent.Num_children += 1;

            foreach(Field cf in structField.Fields) {
                IDataTypeHandler handler = DataTypeFactory.Match(cf);
                handler.CreateThrift(cf, tseStruct, container);
            }
        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return
               tse.Num_children > 0;
        }
    }
}