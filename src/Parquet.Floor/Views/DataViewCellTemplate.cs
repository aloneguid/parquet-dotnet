using System;
using System.Collections;
using System.Collections.Generic;
using Avalonia.Controls;
using Avalonia.Controls.Templates;
using Avalonia.Layout;
using Parquet.Schema;

namespace Parquet.Floor.Views {
    internal class DataViewCellTemplate : IDataTemplate {
        private readonly Field _field;

        public DataViewCellTemplate(Field field) {
            _field = field;
        }

        public Control? Build(object? param) {
            return Build(param, _field);
        }

        public static Control BuildValue(object? value, Field f) {
            if(value == null) {
                var tx = new TextBlock() {
                    Text = "null"
                };
                tx.Classes.Add("data-cell");
                tx.Classes.Add("data-cell-null");
                return tx;
            }

            if(f.SchemaType == SchemaType.Data) {
                var tx = new TextBlock() {
                    Text = value?.ToString()
                };
                tx.Classes.Add("data-cell");
                return tx;
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

                        vsp.Children.Add(BuildValue(entry.Key, mapField.Key));
                        vsp.Children.Add(new TextBlock { Text = " -> " });
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
