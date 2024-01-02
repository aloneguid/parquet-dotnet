using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Avalonia.Controls;
using Avalonia.Controls.Templates;
using Avalonia.Data;
using Avalonia.Markup.Xaml.Templates;
using Avalonia.Threading;
using Parquet.Floor.ViewModels;
using Parquet.Schema;

namespace Parquet.Floor.Views {
    public partial class DataView : UserControl {
        public DataView() {
            InitializeComponent();


        }

        protected override void OnDataContextChanged(EventArgs e) {
            base.OnDataContextChanged(e);

            ViewModel.PropertyChanged += ViewModel_PropertyChanged;
        }

        private void ViewModel_PropertyChanged(object? sender, PropertyChangedEventArgs e) {
            Dispatcher.UIThread.Invoke(() => {

                if(ViewModel == null || ViewModel.Schema == null)
                    return;

                if(e.PropertyName == nameof(DataViewModel.Schema)) {
                    // build columns

                    grid.Columns.Clear();

                    foreach(Field f in ViewModel.Schema.Fields) {
                        grid.Columns.Add(new DataGridTemplateColumn {
                            Header = f.Name,
                            CellTemplate = new DataViewCellTemplate(f)
                        });
                    }
                } else if(e.PropertyName == nameof(DataViewModel.Data)) {
                    // build rows

                    grid.ItemsSource = ViewModel.Data;
                }
            });
        }

        public DataViewModel ViewModel => (DataViewModel)DataContext!;
    }
}
