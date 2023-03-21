using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Parquet.Schema;

namespace Parquet.Rows {
    /// <summary>
    /// Everything is rows!!! Not dealing with dictionaries etc. seems like a brilliant idea!!!
    /// </summary>
    internal static class RowValidator {
        public static void Validate(Row row, IReadOnlyList<Field> fields) {
            for(int i = 0; i < fields.Count; i++) {
                Field field = fields[i];
                object? value = row[i];
                Type? vt = value == null ? null : value.GetType();

                switch(field.SchemaType) {
                    case SchemaType.Data:
                        ValidatePrimitive((DataField)field, value);
                        break;
                    case SchemaType.Map:
                        ValidateMap((MapField)field, value);
                        break;
                    case SchemaType.Struct:
                        if(value is Row row1) {
                            Validate(row1, ((StructField)field).Fields);
                        } else if (value != null) {
                            throw new InvalidOperationException("expected Row");
                        }
                        break;
                    case SchemaType.List:
                        ValidateList((ListField)field, value);
                        break;
                    default:
                        throw new NotImplementedException(field.SchemaType.ToString());
                }
            }
        }

        private static void ValidateMap(MapField mf, object? value) {
            if(value == null || !value.GetType().TryExtractIEnumerableType(out Type? elementType))
                throw new ArgumentException($"map must be a collection, but found {value?.GetType()}");

            if(elementType != typeof(Row))
                throw new ArgumentException($"map element must be a collection of rows, but found a collection of {elementType}");

            foreach(Row? row in (IEnumerable)value) {
                if(row == null)
                    continue;
                Validate(row, new[] { mf.Key, mf.Value });
            }
        }

        private static void ValidateList(ListField lf, object? value) {
            Type? elementType = null;
            bool isEnumerable = value?.GetType().TryExtractIEnumerableType(out elementType) ?? false;

            //value must be an enumeration of items
            if(!isEnumerable)
                throw new ArgumentException($"simple list must be a collection, but found {value?.GetType()}");

            if(lf.Item.SchemaType == SchemaType.Data) {
                var df = (DataField)lf.Item;

                //value is a list of items

                if(value is IEnumerable ie) {
                    foreach(object? element in ie)
                        ValidatePrimitive(df, element);
                }
            }
            else if(elementType != typeof(Row))
                throw new ArgumentException($"expected a collection of {typeof(Row)} but found a collection of {elementType}");
        }

        private static void ValidatePrimitive(DataField df, object? value) {
            if(value == null) {
                if(!df.IsNullable)
                    throw new ArgumentException($"element is null but column '{df.Name}' does not accept nulls");
            }
            else {
                Type vt = value.GetType();
                Type st = df.ClrType;

                if(vt.IsNullable())
                    vt = vt.GetNonNullable();

                if(df.IsArray) {
                    if(!vt.IsArray)
                        throw new ArgumentException($"expected array but found {vt}");

                    if(vt.GetElementType() != st)
                        throw new ArgumentException($"expected array element type {st} but found {vt.GetElementType()}");
                }
                else if(vt != st)
                    throw new ArgumentException($"expected {st} but found {vt}");
            }
        }
    }
}