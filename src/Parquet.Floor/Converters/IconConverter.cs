using System;
using System.Globalization;
using System.IO;
using Avalonia.Data.Converters;
using Avalonia.Media.Imaging;
using Avalonia.Platform;
using Parquet.Floor.ViewModels;

namespace Parquet.Floor.Converters {
    public class IconConverter : IValueConverter {

        private readonly Bitmap _folderIcon;
        private readonly Bitmap _fileIcon;
        private readonly Bitmap _parquetIcon;

        public IconConverter() {
            using Stream fsFolder = AssetLoader.Open(new Uri("avares://floor/Assets/folder.png"));
            _folderIcon = new Bitmap(fsFolder);

            using Stream fsFile = AssetLoader.Open(new Uri("avares://floor/Assets/file.png"));
            _fileIcon = new Bitmap(fsFile);

            using Stream fsParquet = AssetLoader.Open(new Uri("avares://floor/Assets/icon.ico"));
            _parquetIcon = new Bitmap(fsParquet);
        }

        public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) {
            //if(value is FileNode n) {
            //    if(n.Entry.Path.IsFolder) {
            //        return _folderIcon;
            //    } else if(n.Entry.Path.IsFile) {
            //        if(n.Entry.Path.Full.EndsWith(".parquet")) {
            //            return _parquetIcon;
            //        } else {
            //            return _fileIcon;
            //        }
            //    }
            //}

            return null;
        }

        public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) => throw new NotImplementedException();
    }
}
