using System.Collections.Generic;

namespace Parquet.Serialization.Dremel {
    
    /// <summary>
    /// This is a dictionary that allows for the values and keys to be set independently.
    /// It is used by the record assembler, because internally keys and values collections are stored separately inside
    /// the parquet file, therefore there is no other way to re-assemble them into a native data structure.
    /// </summary>
    class ParquetDictionary<TKey, TValue> : Dictionary<TKey, TValue>, IList<ParquetDictionary<TKey, TValue>.ParquetDictionaryElement>
        where TKey : notnull {

        private readonly List<ParquetDictionaryElement> _list = new();

        #region [ IList Overrides ]
        public ParquetDictionary<TKey, TValue>.ParquetDictionaryElement this[int index] {
            get => _list[index];
            set => _list[index] = value;
        }

        public bool IsReadOnly => false;

        public void Add(ParquetDictionary<TKey, TValue>.ParquetDictionaryElement item) {
            item.Parent = this;
            _list.Add(item);
        }

        public bool Contains(ParquetDictionary<TKey, TValue>.ParquetDictionaryElement item) => 
            _list.Contains(item);
        public void CopyTo(ParquetDictionary<TKey, TValue>.ParquetDictionaryElement[] array, int arrayIndex) => 
            _list.CopyTo(array, arrayIndex);
        public int IndexOf(ParquetDictionary<TKey, TValue>.ParquetDictionaryElement item) => 
            _list.IndexOf(item);
        public void Insert(int index, ParquetDictionary<TKey, TValue>.ParquetDictionaryElement item) => 
            _list.Insert(index, item);
        public bool Remove(ParquetDictionary<TKey, TValue>.ParquetDictionaryElement item) => 
            _list.Remove(item);
        public void RemoveAt(int index) =>
            _list.RemoveAt(index);
        IEnumerator<ParquetDictionary<TKey, TValue>.ParquetDictionaryElement> IEnumerable<ParquetDictionary<TKey, TValue>.ParquetDictionaryElement>.GetEnumerator() => 
            _list.GetEnumerator();

        #endregion

        public new int Count => _list.Count;

        public class ParquetDictionaryElement {

            private TValue? _value;

            public ParquetDictionary<TKey, TValue>? Parent;

            public TKey? Key { get; set; }

            public TValue? Value {
                get => _value;
                set {
                    _value = value;

                    if(Parent != null && Key != null) {
                        ((Dictionary<TKey, TValue>)Parent)[Key] = value!;
                    }
                }
            }
        }

        
    }
}
