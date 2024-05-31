using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Controls.Models.TreeDataGrid;
using Avalonia.Controls.Selection;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Messaging;
using Parquet.Floor.Messages;
using Stowage;
using Stowage.Impl;

namespace Parquet.Floor.ViewModels {

    public class FileNode {
        private readonly IFileStorage _storage;
        private ObservableCollection<FileNode>? _subNodes;

        public IOEntry Entry { get; }

        public string NameDisplay => Entry.Name.Trim('/');

        public string SizeDisplay => Entry.Size?.ToFileSizeUiString() ?? string.Empty;

        public ObservableCollection<FileNode> SubNodes => _subNodes ??= LoadChildren();

        public FileNode(IOEntry entry, IFileStorage storage) {
            Entry = entry;
            _storage = storage;
        }

        private ObservableCollection<FileNode> LoadChildren() {

            IReadOnlyCollection<IOEntry> children = _storage.Ls(Entry.Path).Result;
            _subNodes = new ObservableCollection<FileNode>(children.Select(x => new FileNode(x, _storage)));
            return _subNodes;
        }
    } 

    public partial class FileExplorerViewModel : ViewModelBase {

        private readonly IFileStorage _storage;

        [ObservableProperty]
        private string _currentPath = IOPath.Root;

        [ObservableProperty]
        private FlatTreeDataGridSource<FileNode>? _filesTreeSource;


        [ObservableProperty]
        private ObservableCollection<FileNode> _files = new ObservableCollection<FileNode>();

        [ObservableProperty]
        private bool _canGoUp;


        public FileExplorerViewModel() {
            _storage = Stowage.Files.Of.EntireLocalDisk();
            CurrentPath = Settings.Instance.LastFileExplorerPath ?? IOPath.Root;
            BindFiles();

            ReloadCurrentPath().Forget();
        }

        private async Task ReloadCurrentPath() {
            CanGoUp = !IOPath.IsRoot(CurrentPath);
            IReadOnlyCollection<IOEntry> entries = await _storage.Ls(CurrentPath);
            entries = entries.OrderBy(r => r.Path.IsFolder ? 0 : 1).ToList();

            //await Dispatcher.UIThread.InvokeAsync(BindFiles);

            Files.Clear();
            foreach(IOEntry e in entries) {
                Files.Add(new FileNode(e, _storage));
            }
        }

        public void GoLevelUp() {
            string? parent = IOPath.GetParent(CurrentPath);
            if(parent == null)
                return;
            CurrentPath = parent;

            ReloadCurrentPath().Forget();
        }

        private void BindFiles() {
            // see sample here: https://github.com/AvaloniaUI/Avalonia.Controls.TreeDataGrid/blob/master/samples/TreeDataGridDemo/ViewModels/FilesPageViewModel.cs
            // for the view: https://github.com/AvaloniaUI/Avalonia.Controls.TreeDataGrid/blob/master/samples/TreeDataGridDemo/MainWindow.axaml

            FilesTreeSource = new FlatTreeDataGridSource<FileNode>(Files) {
                Columns = {
                    new TemplateColumn<FileNode>("Name", "FileNameCell", width: GridLength.Auto),
                    new TextColumn<FileNode, string>("Size", x => x.SizeDisplay, GridLength.Star,
                    new TextColumnOptions<FileNode> { TextAlignment = Avalonia.Media.TextAlignment.Right }),
                }
            };

            FilesTreeSource.RowSelection!.SelectionChanged += SelectionChanged;
        }

        private void SelectionChanged(object? sender, TreeSelectionModelSelectionChangedEventArgs<FileNode> e) {
            FileNode? node = e.SelectedItems.FirstOrDefault();
            if(node == null)
                return;

            if(node.Entry.Path.IsFolder) {
                CurrentPath = node.Entry.Path;
                ReloadCurrentPath().Forget();
            } else {
                string filePath = ((ILocalDiskFileStorage)_storage).ToNativeLocalPath(node.Entry.Path);
                // send a message to open file
                WeakReferenceMessenger.Default.Send(new FileOpenMessage(filePath));
            }
        }
    }
}
