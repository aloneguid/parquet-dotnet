using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Schema;
using Avalonia.Controls;
using Avalonia.Controls.Templates;
using Avalonia.Layout;
using Avalonia;
using Parquet.Floor.ViewModels;
using Avalonia.Platform;
using Avalonia.Media.Imaging;

namespace Parquet.Floor.Views.Templates {
    class DataViewHeaderTemplate : IDataTemplate {
        private readonly Field _field;

        public DataViewHeaderTemplate(Field field) {
            _field = field;
        }

        private static bool IsNumeric(Type t) =>
            t == typeof(short) || t == typeof(int) || t == typeof(long) || t == typeof(decimal);

        private static bool IsByteArray(Type t) =>
            t == typeof(byte[]);

        private static bool IsString(Type t) => t == typeof(string);

        private Control CreateIcon() {

            string? name = null;

            switch(_field.SchemaType) {
                case SchemaType.Data:
                    if(_field is DataField df) {
                        if(IsNumeric(df.ClrType)) {
                            name = "number";
                        } else if(IsString(df.ClrType)) {
                            name = "string";
                        } else if(IsByteArray(df.ClrType)) {
                            name = "bytearray";
                        }
                    }
                    break;
                case SchemaType.Struct:
                    name = "struct";
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

            var image = new Image {
                Source = new Bitmap(AssetLoader.Open(new Uri($"avares://floor/Assets/icons/col/{name}.png")))
            };
            image.Classes.Add("dt-icon");
            return image;

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
