using System;
using System.Collections;
using System.Collections.Generic;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.Templates;
using Avalonia.Layout;
using Parquet.Schema;

namespace Parquet.Floor.Views.Templates {

    /// <summary>
    /// Implements pretty much all of the logic for rendering a parquet data cell, including complex types.
    /// </summary>
    class DataViewCellTemplate : IDataTemplate {

        private const string DataCellClassName = "data-cell";
        private const string DataCellNullClassName = "data-cell-null";
        private const string DataCellKeyClassName = "data-cell-key";

        private readonly Field _field;

        public DataViewCellTemplate(Field field) {
            _field = field;
        }

        public Control? Build(object? param) {
            return Build(param, _field, 0);
        }

        /// <summary>
        /// Creates a new <see cref="TextBlock"/> with the text "null" and the class "data-cell-null".
        /// Needs to be a new instance per cell, as it can only be added to one parent.
        /// </summary>
        /// <returns></returns>
        private static TextBlock CreateNullTextBlock() {
            var r = new TextBlock {
                Text = "null"
            };
            r.Classes.Add(DataCellClassName);
            r.Classes.Add(DataCellNullClassName);
            return r;
        }

        private static TextBlock CreateTextBlock(string value, string? extraClassName = null) {
            var r = new TextBlock {
                Text = value
            };
            r.Classes.Add(DataCellClassName);
            if(extraClassName != null)
                r.Classes.Add(extraClassName);
            return r;
        }

        private static HorizontalAlignment GetAlignment(Field f) => 
            f is DataField df && (df.ClrType == typeof(string) || df.ClrType == typeof(byte[]))
                ? HorizontalAlignment.Left
                : HorizontalAlignment.Right;

        public static Control BuildValue(object? value, Field f, int depth, string? extraClassName = null, bool forceData = false) {
            if(value == null)
                return CreateNullTextBlock();

            if(forceData || f.SchemaType == SchemaType.Data) {
                TextBlock tb = CreateTextBlock(value.ToString()!, extraClassName);
                tb.HorizontalAlignment = GetAlignment(f);
                return tb;
            } else if(f.SchemaType == SchemaType.Struct) {
                var sp = new StackPanel {
                    Orientation = Orientation.Vertical
                };

                var structField = (StructField)f;
                if(value is IDictionary<string, object> valueDictionary)
                    foreach(Field field in structField.Fields) {

                        var vsp = new StackPanel {
                            Orientation = Orientation.Horizontal
                        };
                        sp.Children.Add(vsp);

                        vsp.Children.Add(BuildValue(field.Name, field, depth, DataCellKeyClassName, forceData = true));
                        //vsp.Children.Add(CreateTextBlock(": "));

                        valueDictionary.TryGetValue(field.Name, out object? fieldValue);
                        vsp.Children.Add(BuildValue(fieldValue, field, depth + 1));
                    }

                return sp;
            } else if(f.SchemaType == SchemaType.Map) {
                var sp = new StackPanel {
                    Orientation = Orientation.Vertical
                };

                var mapField = (MapField)f;

                if(value is IDictionary valueDictionary)
                    foreach(DictionaryEntry entry in valueDictionary) {

                        var vsp = new StackPanel {
                            Orientation = Orientation.Horizontal
                        };
                        sp.Children.Add(vsp);

                        vsp.Children.Add(BuildValue(entry.Key, mapField.Key, depth, DataCellKeyClassName));
                        //vsp.Children.Add(CreateTextBlock(": "));
                        vsp.Children.Add(BuildValue(entry.Value, mapField.Value, depth + 1));
                    }

                return sp;
            } else if(f.SchemaType == SchemaType.List) {
                var sp = new StackPanel {
                    Orientation = Orientation.Vertical
                };
                var listField = (ListField)f;
                if(value is IEnumerable valueList)
                    foreach(object? entry in valueList)
                        sp.Children.Add(BuildValue(entry, listField.Item, depth + 1));
                return sp;
            }

            return new Control();
        }

        public static Control Build(object? param, Field f, int depth) {

            object? value = null;

            if(param is Dictionary<string, object> row)
                if(row.TryGetValue(f.Name, out object? mapValue))
                    if(mapValue != null)
                        value = mapValue;

            return BuildValue(value, f, depth);
        }

        public bool Match(object? data) => true;
    }
}