using System;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Schema {

    /// <summary>
    /// Represents path in schema. Path is a dot-separated string, however path parts can also contain dots!
    /// Never use strings to represent path, prefer this class.
    /// </summary>
    public sealed class FieldPath : IEquatable<FieldPath> {

        private readonly List<string> _parts;

        /// <summary>
        /// Construct path single part, which becomes the first part in the result path.
        /// </summary>
        /// <param name="firstPart"></param>
        public FieldPath(string firstPart) {
            _parts = new List<string> { firstPart ?? throw new ArgumentNullException(nameof(firstPart)) };
        }

        /// <summary>
        /// Constructs path from parts (safe)
        /// </summary>
        public FieldPath(IEnumerable<string> parts) {
            _parts = parts
                .Where(i => !string.IsNullOrEmpty(i))
                .ToList();
        }

        /// <summary>
        /// Constructs path from parts (safe)
        /// </summary>
        /// <param name="parts"></param>
        public FieldPath(params string[] parts) : this((IEnumerable<string>)parts) {

        }

        /// <summary>
        /// Append an element to path
        /// </summary>
        /// <param name="value">Can be null or empty (ignored).</param>
        public void Append(string value) {
            if(string.IsNullOrEmpty(value))
                return;

            _parts.Add(value);
        }

        /// <summary>
        /// Convert to list
        /// </summary>
        /// <returns></returns>
        public List<string> ToList() => new List<string>(_parts);

        /// <summary>
        /// Returns first part of the path, or null if path is empty
        /// </summary>
        public string? FirstPart => _parts.Count == 0 ? null : _parts[0];

        /// <summary>
        /// Gets part by index
        /// </summary>
        public string this[int i] => _parts[i];

        /// <summary>
        /// Number of elements in path
        /// </summary>
        public int Length => _parts.Count;

        /// <summary>
        /// Compares string path
        /// </summary>
        public bool Equals(FieldPath? other) {
            if(ReferenceEquals(this, other))
                return true;

            if(other?._parts.Count != _parts.Count)
                return false;

            for(int i = 0; i < _parts.Count; i++) {
                if(_parts[i] != other._parts[i]) return false;
            }

            return true;
        }

        /// <inheritdoc/>
        public override int GetHashCode() {
            int hash = 19;
            unchecked {
                foreach(string part in _parts) {
                    hash = (hash * 31) + part.GetHashCode();
                }
            }
            return hash;
        }

        /// <inheritdoc/>
        public override bool Equals(object? obj) {
            if(obj is not FieldPath fp) return false;

            return Equals(fp);
        }

        /// <summary>
        /// String repr
        /// </summary>
        public override string ToString() => string.Join("/", _parts);

        /// <summary>
        /// Combines two paths safely
        /// </summary>
        public static FieldPath operator +(FieldPath? left, FieldPath? right) {
            var parts = new List<string>();
            if(left != null)
                parts.AddRange(left._parts);
            if(right != null)
                parts.AddRange(right._parts);
            return new FieldPath(parts);
        }
    }
}
