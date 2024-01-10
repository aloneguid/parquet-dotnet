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

        private Control CreateIcon() {

            // Assuming you have a resource named 'myImage.png' in your resources
            var image = new Image {
                Source = new Bitmap(AssetLoader.Open(new Uri("avares://floor/Assets/icons/col/string.png")))
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
