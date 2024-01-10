using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Controls.Models.TreeDataGrid;
using CommunityToolkit.Mvvm.ComponentModel;
using Parquet.Schema;

namespace Parquet.Floor.ViewModels; 

public partial class SchemaViewModel : ViewModelBase {
    [ObservableProperty]
    private ParquetSchema? _schema;

    private ObservableCollection<FieldModel>? _fields;

    [ObservableProperty]
    private HierarchicalTreeDataGridSource<FieldModel>? _fieldsTreeSource;

    public SchemaViewModel() {
#if DEBUG
        if(Design.IsDesignMode) {
            InitSchema(DesignData.Schema);
        }
#endif
    }

    public void InitSchema(ParquetSchema? schema) {
        Schema = schema;

        if(Schema == null) {
            _fields = new ObservableCollection<FieldModel>();
        } else {
            _fields = new ObservableCollection<FieldModel>(Schema.Fields.Select(f => new FieldModel(f)));
        }

        FieldsTreeSource = new HierarchicalTreeDataGridSource<FieldModel>(_fields) {
            Columns = {
                new HierarchicalExpanderColumn<FieldModel>(
                    new TextColumn<FieldModel, string>("Name", x => x.Name),
                    x => x.Children, isExpandedSelector: x => x.IsExpanded),
                //new TextColumn<FieldModel, string>("Num children", x => x.NumChildren),
                new TextColumn<FieldModel, string>("Type", x => x.Type),
                new TextColumn<FieldModel, string>("Converted type", x => x.ConvertedType),
                new TextColumn<FieldModel, string>("Logical type", x => x.LogicalType),
                new TextColumn<FieldModel, string>("Type length", x => x.TypeLength),
                new TextColumn<FieldModel, string>("Repetition type", x => x.RepetitionType),
                new TextColumn<FieldModel, string>("Scale", x => x.Scale),
                new TextColumn<FieldModel, string>("Precision", x => x.Precision),
                new TextColumn<FieldModel, string>("Field ID", x => x.FieldId),
                new TextColumn<FieldModel, string>("DL", x => x.DefinitionLevel),
                new TextColumn<FieldModel, string>("RL", x => x.RepetitionLevel)
            }
        };

    }
}
