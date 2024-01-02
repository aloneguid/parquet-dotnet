using System;
using System.Collections;
using System.Collections.Generic;
using Avalonia.Controls;
using Avalonia.Controls.Templates;
using Avalonia.Layout;
using Parquet.Schema;

namespace Parquet.Floor.Views {

    /// <summary>
    /// Implements pretty much all of the logic for rendering a parquet data cell, including complex types.
    /// </summary>
    internal class DataViewCellTemplate : IDataTemplate {

        private const string DataCellClassName = "data-cell";
        private const string DataCellNullClassName = "data-cell-null";
        private const string DataCellKeyClassName = "data-cell-key";

        private readonly Field _field;

        public DataViewCellTemplate(Field field) {
            _field = field;
        }

        public Control? Build(object? param) {
            return Build(param, _field);
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
            if(extraClassName != null) {
                r.Classes.Add(extraClassName);
            }
            return r;
        }

        public static Control BuildValue(object? value, Field f, string? extraClassName = null) {
            if(value == null) {
                return CreateNullTextBlock();
            }

            if(f.SchemaType == SchemaType.Data) {
                return CreateTextBlock(value.ToString()!, extraClassName);
            } else if(f.SchemaType == SchemaType.Map) {
                var sp = new StackPanel {
                    Orientation = Orientation.Vertical
                };

                var mapField = (MapField)f;

                if(value is IDictionary valueDictionary) {
                    foreach(DictionaryEntry entry in valueDictionary) {

                        var vsp = new StackPanel {
                            Orientation = Orientation.Horizontal
                        };
                        sp.Children.Add(vsp);

                        vsp.Children.Add(BuildValue(entry.Key, mapField.Key, DataCellKeyClassName));
                        vsp.Children.Add(CreateTextBlock(": "));
                        vsp.Children.Add(BuildValue(entry.Value, mapField.Value));
                    }
                }

                return sp;
            }

            return new Control();
        }

        public static Control Build(object? param, Field f) {

            object? value = null;

            if(param is Dictionary<string, object> row) {
                if(row.TryGetValue(f.Name, out object? mapValue)) {
                    if(mapValue != null) {
                        value = mapValue;
                    }
                }
            }

            return BuildValue(value, f);


            /*if(f.SchemaType == SchemaType.Map) {
                var sp = new StackPanel {
                    Orientation = Orientation.Vertical
                };

                var mapField = (MapField)f;

                if(param is Dictionary<string, object> row) {
                    if(row.TryGetValue(f.Name, out object? value)) {

                        if(value is IDictionary valueDictionary) {
                            foreach(DictionaryEntry entry in valueDictionary) {

                                var vsp = new StackPanel {
                                    Orientation = Orientation.Horizontal
                                };
                                sp.Children.Add(vsp);

                                vsp.Children.Add(Build(entry.Key, mapField.Key));
                                vsp.Children.Add(new TextBlock { Text = " -> "});
                            }
                        }


                    }
                }


                return sp;
            }*/
        }

        public bool Match(object? data) => true;
    }
}
