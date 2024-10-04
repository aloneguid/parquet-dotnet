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

        private static Control CreatePrimitiveValueControl(Field f, object value, string? extraClassName = null) {

            if(f is DataField df) {
                if(df.ClrType == typeof(bool) && value is bool bValue) {
                    var r = new CheckBox {
                        IsChecked = bValue
                    };
                    r.Classes.Add(DataCellClassName);
                    r.IsEnabled = false;
                    return r;
                }
            }

            TextBlock tb = CreateTextBlock(value.ToString()!, extraClassName);
            tb.HorizontalAlignment = GetAlignment(f);
            return tb;
        }

        private static HorizontalAlignment GetAlignment(Field f) => 
            f is DataField df && (df.ClrType == typeof(string) || df.ClrType == typeof(byte[]))
                ? HorizontalAlignment.Left
                : HorizontalAlignment.Right;

        public static Control BuildValue(object? value, Field f, int depth, string? extraClassName = null, bool forceData = false) {
            if(value == null)
                return CreateNullTextBlock();

            if(forceData || f.SchemaType == SchemaType.Data) {
                if(f is DataField df && df.IsArray) {
                    var sp = new StackPanel {
                        Orientation = Orientation.Horizontal
                    };
                    foreach(object? entry in (IEnumerable)value) {
                        TextBlock ctrl = entry == null
                            ? CreateNullTextBlock()
                            : CreateTextBlock(entry.ToString()!, extraClassName);
                        var border = new Border {
                            BorderThickness = new Thickness(1),
                            BorderBrush = Avalonia.Media.Brushes.Black,
                            Margin = new Thickness(2),
                            CornerRadius = new CornerRadius(2)
                        };
                        border.Child = ctrl;
                        sp.Children.Add(border);
                    }
                    return sp;
                }

                return CreatePrimitiveValueControl(f, value, extraClassName);
            } else if(f.SchemaType == SchemaType.Struct) {
                //var sp = new StackPanel { Orientation = Orientation.Vertical };
                var hdr = new Expander { IsExpanded = false };
                hdr.Classes.Add("data-cell-struct");
                //hdr.Content = sp;

                var structField = (StructField)f;
                if(value is IDictionary<string, object> valueDictionary) {

                    // create a simple grid control
                    var grid = new Grid();
                    hdr.Content = grid;
                    grid.ColumnDefinitions.Add(new ColumnDefinition(GridLength.Auto));  // key
                    grid.ColumnDefinitions.Add(new ColumnDefinition(GridLength.Auto));  // value
                    int iRow = 0;

                    foreach(Field field in structField.Fields) {
                        Control keyControl = BuildValue(field.Name, field, depth, DataCellKeyClassName, forceData = true);
                        valueDictionary.TryGetValue(field.Name, out object? fieldValue);
                        Control valueControl = BuildValue(fieldValue, field, depth + 1);

                        grid.RowDefinitions.Add(new RowDefinition(GridLength.Auto));
                        grid.Children.Add(keyControl);
                        grid.Children.Add(valueControl);
                        Grid.SetColumn(keyControl, 0);
                        Grid.SetColumn(valueControl, 1);
                        Grid.SetRow(keyControl, iRow);
                        Grid.SetRow(valueControl, iRow);
                        iRow++;
                    }
                }

                return hdr;
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