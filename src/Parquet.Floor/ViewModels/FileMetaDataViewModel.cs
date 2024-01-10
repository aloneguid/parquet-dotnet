using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Controls.Models.TreeDataGrid;
using CommunityToolkit.Mvvm.ComponentModel;
using Parquet.Meta;

namespace Parquet.Floor.ViewModels {

    public class PropertyModel {
        public PropertyModel(string name, string? value) {
            Name = name;
            Value = value;
        }

        public string Name { get; }

        public string? Value { get; }

        public ObservableCollection<PropertyModel>? Children { get; set; }
    }

    public partial class FileMetaDataViewModel : ViewModelBase {

        [ObservableProperty]
        private HierarchicalTreeDataGridSource<PropertyModel>? _metaTreeSource;

        [ObservableProperty]
        private ObservableCollection<PropertyModel>? _rootProps;

        // todo: public List<SchemaElement> Schema { get; set; } = new List<SchemaElement>();

        // todo: public List<RowGroup> RowGroups { get; set; } = new List<RowGroup>();

        public FileMetaDataViewModel() { 
            if(Design.IsDesignMode) {
                RootProps = new ObservableCollection<PropertyModel> {
                    new PropertyModel("Version", "1"),
                    new PropertyModel("Num rows", "1000"),
                    new PropertyModel("Created by", "Parquet.Net"),
                    new PropertyModel("Key/value metadata", "0") {
                        Children = new ObservableCollection<PropertyModel> {
                            new PropertyModel("Key", "Value"),
                            new PropertyModel("Key", "Value"),
                            new PropertyModel("Key", "Value"),
                        }
                    },
                    new PropertyModel("Column orders", "0"),
                    new PropertyModel("Encryption algorithm", "None"),
                    new PropertyModel("Footer signing key metadata", "0"),
                };

                CreateTreeSource();
            }
        }

        private void CreateTreeSource() {

            if(RootProps == null)
                return;

            MetaTreeSource = new HierarchicalTreeDataGridSource<PropertyModel>(RootProps) {
                Columns = {
                        new HierarchicalExpanderColumn<PropertyModel>(
                                                       new TextColumn<PropertyModel, string>("Name", x => x.Name),
                                                                                  x => x.Children),
                        new TextColumn<PropertyModel, string>("Value", x => x.Value)
                    }
            };
        }

        public FileMetaDataViewModel(FileMetaData? fm) {
            RootProps = new ObservableCollection<PropertyModel> {
                new PropertyModel(nameof(fm.Version), fm?.Version.ToString()),
                new PropertyModel(nameof(fm.NumRows), fm?.NumRows.ToString()),
                new PropertyModel(nameof(fm.CreatedBy), fm?.CreatedBy),
                new PropertyModel(nameof(fm.KeyValueMetadata), fm?.KeyValueMetadata?.Count.ToString()) {
                    Children = fm?.KeyValueMetadata == null
                    ? null
                    : new ObservableCollection<PropertyModel>(
                        fm!.KeyValueMetadata!.Select(kv => new PropertyModel(kv.Key, kv.Value)))
                },
                new PropertyModel(nameof(fm.Schema), fm?.Schema?.Count.ToString()) {
                    Children = fm?.Schema == null
                    ? null
                    : MakeSchema(fm)
                },
                new PropertyModel(nameof(fm.RowGroups), fm?.RowGroups?.Count.ToString()) {
                    Children = fm?.RowGroups == null
                    ? null
                    : MakeRowGroups(fm)
                },
            };

            CreateTreeSource();
        }

        private ObservableCollection<PropertyModel>? MakeSchema(FileMetaData? fm) {
            if(fm?.Schema == null)
                return null;

            var r = new ObservableCollection<PropertyModel>();

            for(int i = 0; i < fm.Schema.Count; i++) {
                SchemaElement f = fm.Schema[i];

                r.Add(new PropertyModel($"[{i}]", f.Name) {
                    Children = Shrink(new ObservableCollection<PropertyModel> {
                        new PropertyModel(nameof(f.Name), f.Name),
                        new PropertyModel(nameof(f.Type), f.Type?.ToString()),
                        new PropertyModel(nameof(f.TypeLength), f.TypeLength?.ToString()),
                        new PropertyModel(nameof(f.ConvertedType), f.ConvertedType?.ToString()),
                        new PropertyModel(nameof(f.LogicalType), null) { Children = MakeLogicalType(f.LogicalType)},
                        new PropertyModel(nameof(f.RepetitionType), f.RepetitionType?.ToString()),
                        new PropertyModel(nameof(f.Scale), f.Scale?.ToString()),
                        new PropertyModel(nameof(f.Precision), f.Precision?.ToString()),
                        new PropertyModel(nameof(f.FieldId), f.FieldId?.ToString()),
                        new PropertyModel(nameof(f.NumChildren), f.NumChildren?.ToString()),
                    })
                });
            }

            return r;
        }

        private ObservableCollection<PropertyModel>? MakeLogicalType(LogicalType? lt) {
            if(lt == null)
                return null;

            if(lt.STRING != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.STRING), "")
                };

            if(lt.MAP != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.MAP), "")
                };

            if(lt.LIST != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.LIST), "")
                };

            if(lt.ENUM != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.ENUM), "")
                };

            if(lt.DECIMAL != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.DECIMAL), "") {
                        Children = new ObservableCollection<PropertyModel> {
                            new PropertyModel(nameof(lt.DECIMAL.Scale), lt.DECIMAL.Scale.ToString()),
                            new PropertyModel(nameof(lt.DECIMAL.Precision), lt.DECIMAL.Precision.ToString()),
                        }
                    }
                };

            if(lt.DATE != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.DATE), "")
                };

            if(lt.TIME != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.TIME), "") {
                        Children = new ObservableCollection<PropertyModel> {
                            new PropertyModel(nameof(lt.TIME.IsAdjustedToUTC), lt.TIME.IsAdjustedToUTC.ToString()),
                            new PropertyModel(nameof(lt.TIME.Unit), "") {
                                Children = new ObservableCollection<PropertyModel> {
                                    new PropertyModel(nameof(lt.TIME.Unit.MILLIS), lt.TIME.Unit.MILLIS == null ? null : ""),
                                    new PropertyModel(nameof(lt.TIME.Unit.MICROS), lt.TIME.Unit.MICROS == null ? null : ""),
                                    new PropertyModel(nameof(lt.TIME.Unit.NANOS), lt.TIME.Unit.NANOS == null ? null : "")
                                }
                            }
                        }
                    }
                };

            if(lt.TIMESTAMP != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.TIMESTAMP), "") {
                        Children = new ObservableCollection<PropertyModel> {
                            new PropertyModel(nameof(lt.TIMESTAMP.IsAdjustedToUTC), lt.TIMESTAMP.IsAdjustedToUTC.ToString()),
                            new PropertyModel(nameof(lt.TIMESTAMP.Unit), "") {
                                Children = new ObservableCollection<PropertyModel> {
                                    new PropertyModel(nameof(lt.TIMESTAMP.Unit.MILLIS), lt.TIMESTAMP.Unit.MILLIS == null ? null : ""),
                                    new PropertyModel(nameof(lt.TIMESTAMP.Unit.MICROS), lt.TIMESTAMP.Unit.MICROS == null ? null : ""),
                                    new PropertyModel(nameof(lt.TIMESTAMP.Unit.NANOS), lt.TIMESTAMP.Unit.NANOS == null ? null : "")
                                }
                            }
                        }
                    }
                };

            if(lt.INTEGER != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.INTEGER), "") {
                        Children = new ObservableCollection<PropertyModel> {
                            new PropertyModel(nameof(lt.INTEGER.IsSigned), lt.INTEGER.IsSigned.ToString()),
                            new PropertyModel(nameof(lt.INTEGER.BitWidth), lt.INTEGER.BitWidth.ToString()),
                        }
                    }
                };

            if(lt.UNKNOWN != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.UNKNOWN), "")
                };

            if(lt.JSON != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.JSON), "")
                };

            if(lt.BSON != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.BSON), "")
                };

            if(lt.UUID != null)
                return new ObservableCollection<PropertyModel> {
                    new PropertyModel(nameof(lt.UUID), "")
                };

            return null;
        }

        private ObservableCollection<PropertyModel>? MakeRowGroups(FileMetaData? fm) {
            if(fm?.RowGroups == null)
                return null;

            var r = new ObservableCollection<PropertyModel>();

            for(int i = 0; i < fm.RowGroups.Count; i++) {
                RowGroup rg = fm.RowGroups[i];

                r.Add(new PropertyModel($"[{i}]", "") {
                    Children = Shrink(new ObservableCollection<PropertyModel> {
                        new PropertyModel(nameof(rg.TotalByteSize), rg.TotalByteSize.ToString()),
                        new PropertyModel(nameof(rg.NumRows), rg.NumRows.ToString()),
                        new PropertyModel(nameof(rg.FileOffset), rg.FileOffset?.ToString()),
                        new PropertyModel(nameof(rg.TotalCompressedSize), rg.TotalCompressedSize?.ToString()),
                        new PropertyModel(nameof(rg.Ordinal), rg.Ordinal?.ToString()),
                        new PropertyModel(nameof(rg.Columns), rg.Columns?.Count.ToString()) {
                            Children = rg.Columns == null
                            ? null
                            : MakeColumns(rg)
                        }
                        // sorting columns
                    })
                });
            }

            return r;
        }

        private ObservableCollection<PropertyModel>? MakeColumns(RowGroup rg) {
            if(rg.Columns == null)
                return null;

            var r = new ObservableCollection<PropertyModel>();

            for(int i = 0; i < rg.Columns.Count; i++) {
                ColumnChunk cc = rg.Columns[i];

                var root = new PropertyModel($"[{i}]", "");
                root.Children = new ObservableCollection<PropertyModel>();
                root.Children.Add(new PropertyModel(nameof(cc.FilePath), cc.FilePath));
                root.Children.Add(new PropertyModel(nameof(cc.FileOffset), cc.FileOffset.ToString()));
                root.Children.Add(new PropertyModel(nameof(cc.OffsetIndexOffset), cc.OffsetIndexOffset?.ToString()));
                root.Children.Add(new PropertyModel(nameof(cc.OffsetIndexLength), cc.OffsetIndexLength?.ToString()));
                root.Children.Add(new PropertyModel(nameof(cc.ColumnIndexOffset), cc.ColumnIndexOffset?.ToString()));
                root.Children.Add(new PropertyModel(nameof(cc.ColumnIndexLength), cc.ColumnIndexLength?.ToString()));

                if(cc.MetaData != null) {
                    var metadata = new PropertyModel(nameof(cc.MetaData), "");
                    metadata.Children = new ObservableCollection<PropertyModel>();
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.Type), cc.MetaData.Type.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.Encodings), string.Join(';', cc.MetaData.Encodings.Select(e => e.ToString()))));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.PathInSchema), string.Join('.', cc.MetaData.PathInSchema)));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.Codec), cc.MetaData.Codec.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.NumValues), cc.MetaData.NumValues.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.TotalUncompressedSize), cc.MetaData.TotalUncompressedSize.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.TotalCompressedSize), cc.MetaData.TotalCompressedSize.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.KeyValueMetadata), cc.MetaData.KeyValueMetadata?.Count.ToString()) {
                        Children = cc.MetaData.KeyValueMetadata == null
                        ? null
                        : new ObservableCollection<PropertyModel>(cc.MetaData.KeyValueMetadata.Select(kv => new PropertyModel(kv.Key, kv.Value)))
                    });
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.DataPageOffset), cc.MetaData.DataPageOffset.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.IndexPageOffset), cc.MetaData.IndexPageOffset?.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.DictionaryPageOffset), cc.MetaData.DictionaryPageOffset?.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.Statistics), cc.MetaData.Statistics == null ? null : "") {
                        Children = cc.MetaData.Statistics == null
                        ? null
                        : Shrink(new ObservableCollection<PropertyModel> {
                            new PropertyModel(nameof(cc.MetaData.Statistics.NullCount), cc.MetaData.Statistics!.NullCount?.ToString()),
                            new PropertyModel(nameof(cc.MetaData.Statistics.DistinctCount), cc.MetaData.Statistics.DistinctCount?.ToString()),
                            new PropertyModel(nameof(cc.MetaData.Statistics.Min), cc.MetaData.Statistics.Min?.ToString()),
                            new PropertyModel(nameof(cc.MetaData.Statistics.Max), cc.MetaData.Statistics.Max?.ToString()),
                        })
                    });
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.EncodingStats), cc.MetaData.EncodingStats == null ? null : "") {
                        Children = MakeEncodingStats(cc.MetaData.EncodingStats)
                    });
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.BloomFilterOffset), cc.MetaData.BloomFilterOffset?.ToString()));
                    metadata.Children.Add(new PropertyModel(nameof(cc.MetaData.BloomFilterLength), cc.MetaData.BloomFilterLength?.ToString()));
                    metadata.Children = Shrink(metadata.Children);
                    root.Children.Add(metadata);
                }

                root.Children = Shrink(root.Children);

                r.Add(root);
            }

            return r;
        }

        private ObservableCollection<PropertyModel>? MakeEncodingStats(List<PageEncodingStats>? es) {
            if(es == null)
                return null;

            var r = new ObservableCollection<PropertyModel>();

            for(int i = 0; i < es.Count; i++) {
                PageEncodingStats s = es[i];

                r.Add(new PropertyModel($"[{i}]", s.ToString()) {
                    Children = Shrink(new ObservableCollection<PropertyModel> {
                        new PropertyModel(nameof(s.PageType), s.PageType.ToString()),
                        new PropertyModel(nameof(s.Encoding), s.Encoding.ToString()),
                        new PropertyModel(nameof(s.Count), s.Count.ToString()),
                    })
                });
            }

            return r;
        }

        private ObservableCollection<PropertyModel> Shrink(ObservableCollection<PropertyModel> props) {
            return new ObservableCollection<PropertyModel>(props.Where(p => p.Value != null || p.Children != null));
        }
    }
}
