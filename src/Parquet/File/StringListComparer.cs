using System.Collections.Generic;

namespace Parquet.File {
    /// <summary>
    /// Wraps a list of strings and provides equality 
    /// </summary>
    class StringListComparer {
        private readonly List<string> _key;

        public StringListComparer(List<string> key) {
            _key = key;
        }

        public override bool Equals(object? obj) {
            return obj is StringListComparer s && Equals(s);
        }

        bool Equals(StringListComparer other) {
            if(ReferenceEquals(_key, other._key)) {
                return true;
            }
            if(_key == null || other._key == null) {
                return false;
            }

            if(_key.Count != other._key.Count) {
                return false;
            }

            for(int i = 0; i < _key.Count; i++) {
                string a = _key[i];
                string b = other._key[i];
                if(string.CompareOrdinal(a, b) != 0)
                    return false;
            }

            return true;
        }

        public override int GetHashCode() {
            int hashCode = 0;
            // ReSharper disable once LoopCanBeConvertedToQuery
            // ReSharper disable once ForCanBeConvertedToForeach
            for(int index = 0; index < _key.Count; index++) {
                string key = _key[index];
                hashCode = (key.GetHashCode() * 397) ^ hashCode;
            }
            return hashCode;
        }
    }
}