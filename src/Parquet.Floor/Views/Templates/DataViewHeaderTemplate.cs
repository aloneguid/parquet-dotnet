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

namespace Parquet.Floor.Views.Templates {
    class DataViewHeaderTemplate : IDataTemplate {
        private readonly Field _field;

        public DataViewHeaderTemplate(Field field) {
            _field = field;
        }

        private Control CreateIcon() {
            string? v = null;
            switch(_field.SchemaType) {
                case SchemaType.Data:
                    if(_field is DataField df) {
                        if(df.ClrType == typeof(short) || df.ClrType == typeof(int) || df.ClrType == typeof(long) || df.ClrType == typeof(decimal)) {
                            v = "list-ol";
                        } else if(df.ClrType == typeof(string)) {
                            v = "s";
                        }
                    }
                    break;
            }
            if(v == null) {
                v = "fa-solid fa-database";
            } else {
                v = "fa-solid fa-" + v;
            }

            return new Projektanker.Icons.Avalonia.Icon {
                Value = v,
                Margin = new Thickness(5, 0, 0, 0),
                FontSize = 12
            };
        }

        public Control? Build(object? param) {

            var r = new StackPanel {
                Orientation = Orientation.Horizontal
            };
            r.Children.Add(new TextBlock {
                Text = _field.Name
            });
            //r.Children.Add(CreateIcon());

            //r.SetValue(ToolTip.TipProperty, "...");
            return r;
        }
        public bool Match(object? data) => true;
    }
}
