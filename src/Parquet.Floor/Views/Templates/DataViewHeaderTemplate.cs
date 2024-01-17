using System;
using Parquet.Schema;
using Avalonia.Controls;
using Avalonia.Controls.Templates;
using Avalonia.Layout;
using Avalonia;
using Parquet.File.Values.Primitives;

namespace Parquet.Floor.Views.Templates {
    class DataViewHeaderTemplate : IDataTemplate {
        private readonly Field _field;

        public DataViewHeaderTemplate(Field field) {
            _field = field;
        }

        private static bool IsNumeric(Type t) =>
            t == typeof(short) || t == typeof(ushort) ||
            t == typeof(int) || t == typeof(uint) || 
            t == typeof(long) || t == typeof(ulong) ||
            t == typeof(float) ||
            t == typeof(decimal) ||
            t == typeof(double);

        private static bool IsByteArray(Type t) =>
            t == typeof(byte[]);

        private static bool IsString(Type t) => t == typeof(string);

        private Control CreateIcon() {

            string? name = null;

            switch(_field.SchemaType) {
                case SchemaType.Data:
                    if(_field is DataField df) {
                        if(IsNumeric(df.ClrType)) {
                            name = "hashtag";
                        } else if(IsString(df.ClrType)) {
                            name = "comment";
                        } else if(IsByteArray(df.ClrType)) {
                            name = "chart-simple";
                        } else if(df.ClrType == typeof(bool)) {
                            name = "square-check";
                        } else if(df.ClrType == typeof(DateTime)) {
                            name = "calendar";
                        } else if(df.ClrType == typeof(TimeSpan)) {
                            name = "clock";
                        } else if(df.ClrType == typeof(Interval)) {
                            name = "hourglass";
                        }
                    }
                    break;
                case SchemaType.Struct:
                    name = "folder-tree";
                    break;
                case SchemaType.Map:
                    name = "map";
                    break;
                case SchemaType.List:
                    name = "list";
                    break;
            }


            if(name == null)
                return new Control();

            return new Projektanker.Icons.Avalonia.Icon {
                Value = $"fa-solid fa-{name}",
                FontSize = 12,
                Margin = new Thickness(6, 0, 0, 6)
            };

        }

        public Control? Build(object? param) {

            var r = new StackPanel {
                Orientation = Orientation.Horizontal,
                HorizontalAlignment = HorizontalAlignment.Stretch
            };
            r.Children.Add(new TextBlock {
                Text = _field.Name
            });
            r.Children.Add(CreateIcon());

            //r.SetValue(ToolTip.TipProperty, "...");
            return r;
        }
        public bool Match(object? data) => true;
    }
}
