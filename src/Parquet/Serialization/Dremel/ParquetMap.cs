using System;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Serialization.Dremel;

class ParquetMapKV<TKey, TValue> where TKey : notnull {
    public ParquetMap<TKey, TValue>? parent;
    private TKey? _key;
    private TValue? _value;

    public TKey? Key {
        get => _key;
        set {
            _key = value;
            if(parent != null) {
                // unfortunately assigning default value must be done here, otherwise we can miss situation where key exists but value is null, resulting in data loss
                parent[_key!] = default!;
            }
        }
    }

    public TValue? Value {
        get => _value;
        set {
            _value = value;

            if(parent != null) {
                parent[Key!] = _value!;
                parent.valuesAdded++;

                if(parent.keysAdded == parent.valuesAdded) {
                    // we are done building the map, can release the list to save memory
                    parent.list = null;
                }
            }
        }
    }

}

/// <summary>
/// Dictionary that allows for the values and keys to be set independently. Parquet MAP is physically stored as LIST of structs
/// with key and value fields. .NET dictionary does not 100% map to this structure so this class is used as an intermediary.
/// </summary>
class ParquetMap<TKey, TValue> : 
    Dictionary<TKey, TValue>, IList<ParquetMapKV<TKey, TValue>>
    where TKey : notnull {

    public int keysAdded;
    public int valuesAdded;

    // this can be set to null when we are done with building the map to save memory
    public List<ParquetMapKV<TKey, TValue>>? list = new();

    #region [ IList Overrides ]

    public ParquetMapKV<TKey, TValue> this[int index] {
        get => list![index];
        set => list![index] = value;
    }

    public bool IsReadOnly => false;

    public void Add(ParquetMapKV<TKey, TValue> item) {
        item.parent = this;
        list?.Add(item);
        keysAdded++;
    }

    public bool Contains(ParquetMapKV<TKey, TValue> item) => 
        list?.Contains(item) ?? false;
    public void CopyTo(ParquetMapKV<TKey, TValue>[] array, int arrayIndex) => 
        list?.CopyTo(array, arrayIndex);
    public int IndexOf(ParquetMapKV<TKey, TValue> item) => 
        list?.IndexOf(item) ?? -1;
    public void Insert(int index, ParquetMapKV<TKey, TValue> item) => 
        list?.Insert(index, item);
    public bool Remove(ParquetMapKV<TKey, TValue> item) => 
        list?.Remove(item) ?? false;
    public void RemoveAt(int index) =>
        list?.RemoveAt(index);
    IEnumerator<ParquetMapKV<TKey, TValue>> IEnumerable<ParquetMapKV<TKey, TValue>>.GetEnumerator() => 
        list?.GetEnumerator() ?? Enumerable.Empty<ParquetMapKV<TKey, TValue>>().GetEnumerator();

    #endregion

    public new int Count => list?.Count ?? base.Count;
}