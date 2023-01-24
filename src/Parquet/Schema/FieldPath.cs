using System;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Schema {

    /// <summary>
    /// Represents path in schema. Path is a dot-separated string, however path parts can also contain dots!
    /// Never use strings to represent path, prefer this class.
    /// </summary>
    public sealed class FieldPath : IEquatable<FieldPath> {

        /// <summary>
        /// Path separator
        /// </summary>
        public const string Separator = ".";

        private readonly List<string> _parts;
        private string _str;

        /// <summary>
        /// Construct path from raw string (unsafe!)
        /// </summary>
        /// <param name="path"></param>
        public FieldPath(string path) {
            _str = path ?? throw new ArgumentNullException(nameof(path));
#if NETSTANDARD2_0
            _parts = path.Split(new[] { Separator[0] }, StringSplitOptions.RemoveEmptyEntries).ToList();
#else
            _parts = path.Split(Separator, StringSplitOptions.RemoveEmptyEntries).ToList();
#endif
        }

        /// <summary>
        /// Constructs path from parts (safe)
        /// </summary>
        public FieldPath(IEnumerable<string> parts) {
            _parts = parts
                .Where(i => !string.IsNullOrEmpty(i))
                .ToList();
            _str = string.Join(Separator, _parts);

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
            _str = string.Join(Separator, _parts);
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
        /// Number of elements in path
        /// </summary>
        public int Length => _parts.Count;

        /// <summary>
        /// Compares string path
        /// </summary>
        public bool Equals(FieldPath? other) {
            if(ReferenceEquals(this, other))
                return true;

            return _str.Equals(other?._str);

        }

        /// <summary>
        /// Hash code of string path
        /// </summary>
        public override int GetHashCode() => _str.GetHashCode();

        /// <summary>
        /// String repr
        /// </summary>
        public override string ToString() => _str;

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

        /// <summary>
        /// String repr
        /// </summary>
        public static implicit operator string(FieldPath p) => p._str;

        /// <summary>
        /// Unsafe path constructor
        /// </summary>
        public static implicit operator FieldPath(string s) => new FieldPath(s);
    }
}
