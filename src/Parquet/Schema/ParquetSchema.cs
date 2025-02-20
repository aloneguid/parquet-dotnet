using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Schema {
    /// <summary>
    /// Represents dataset schema
    /// </summary>
    public class ParquetSchema : IEquatable<ParquetSchema> {
        /// <summary>
        /// Symbol used to separate path parts in schema element path
        /// </summary>
        public const string PathSeparator = ".";

        /// <summary>
        /// Character used to separate path parts in schema element path
        /// </summary>
        public const char PathSeparatorChar = '.';

        private readonly List<Field> _fields;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetSchema"/> class from schema elements.
        /// </summary>
        /// <param name="fields">The elements.</param>
        public ParquetSchema(IEnumerable<Field> fields) : this(fields.ToList()) {
            if(fields == null) {
                throw new ArgumentNullException(nameof(fields));
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetSchema"/> class.
        /// </summary>
        /// <param name="fields">The elements.</param>
        public ParquetSchema(params Field[] fields) : this(fields.ToList()) {
            if(fields == null) {
                throw new ArgumentNullException(nameof(fields));
            }
        }

        private ParquetSchema(List<Field> fields) {
            if(fields.Count == 0) {
                throw new ArgumentException("at least one field is required", nameof(fields));
            }

            _fields = fields;

            //set levels now, after schema is constructed
            PropagateLevels();
        }

        internal void PropagateLevels() {
            foreach(Field field in _fields) {
                field.PropagateLevels(0, 0);
            }
        }

        /// <summary>
        /// Gets the schema elements
        /// </summary>
        public IReadOnlyList<Field> Fields => _fields;

        /// <summary>
        /// Get schema element by index
        /// </summary>
        /// <param name="i">Index of schema element</param>
        /// <returns>Schema element</returns>
        public Field this[int i] {
            get { return _fields[i]; }
        }

        /// <summary>
        /// Gets a flat list of all data fields in this schema. Traverses schema tree in order to do that.
        /// </summary>
        /// <returns></returns>
        public DataField[] GetDataFields() {
            var result = new List<DataField>();

            void analyse(Field f) {
                switch(f.SchemaType) {
                    case SchemaType.Data:
                        result?.Add((DataField)f);
                        break;
                    case SchemaType.List:
                        analyse(((ListField)f).Item);
                        break;
                    case SchemaType.Map:
                        MapField mf = (MapField)f;
                        analyse(mf.Key);
                        analyse(mf.Value);
                        break;
                    case SchemaType.Struct:
                        StructField sf = (StructField)f;
                        traverse(sf.Fields);
                        break;
                }
            }

            void traverse(IEnumerable<Field> fields) {
                foreach(Field f in fields) {
                    analyse(f);
                }
            }

            traverse(Fields);

            return result.ToArray();
        }

        /// <summary>
        /// Gets data fields in this schema, including from nested types. Equivalent to <see cref="GetDataFields"/>
        /// </summary>
        public DataField[] DataFields => GetDataFields();

        /// <summary>
        /// Finds a data field by its path. If not found, throws <see cref="ArgumentException"/> exception.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public DataField FindDataField(FieldPath path) {
            DataField? df = DataFields.FirstOrDefault(f => f.Path.Equals(path));
            if(df == null) {
                throw new ArgumentException($"data field '{path}' not found", nameof(path));
            }
            return df;
        }

        /// <summary>
        /// Finds a data field by its path. If not found, throws <see cref="ArgumentException"/> exception.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public DataField FindDataField(string path) {
            return FindDataField(new FieldPath(path));
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
        /// </returns>
        public bool Equals(ParquetSchema? other) {
            if(ReferenceEquals(null, other))
                return false;
            if(ReferenceEquals(this, other))
                return true;

            DataField[] dataFields = DataFields;
            DataField[] otherDataFields = other.DataFields;
            if(dataFields.Length != otherDataFields.Length)
                return false;

            for(int i = 0; i < dataFields.Length; i++) {
                if(!dataFields[i].Equals(otherDataFields[i]))
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Compares this schema to <paramref name="other"/> and produces a human readable message describing the differences.
        /// </summary>
        public string GetNotEqualsMessage(ParquetSchema other, string thisName, string otherName) {
            DataField[] dataFields = DataFields;
            DataField[] otherDataFields = other.DataFields;

            if(dataFields.Length != otherDataFields.Length) {
                return $"different number of elements ({dataFields.Length} != {otherDataFields.Length})";
            }

            var sb = new StringBuilder();
            for(int i = 0; i < dataFields.Length; i++) {
                if(!dataFields[i].Equals(otherDataFields[i])) {
                    if(sb.Length != 0) {
                        sb.Append(", ");
                    }

                    sb.Append("[");
                    sb.Append(thisName);
                    sb.Append(": ");
                    sb.Append(dataFields[i]);
                    sb.Append("] != [");
                    sb.Append(otherName);
                    sb.Append(": ");
                    sb.Append(otherDataFields[i]);
                    sb.Append("]");
                }
            }
            if(sb.Length > 0)
                return sb.ToString();

            return "not sure!";
        }

        #region [ Schema augmentation ]

        /// <summary>
        /// Augments this schema with any information that can be taken from an external schema, to make it more complete.
        /// It's internal for now due to the fact that it's not clear whether it should be exposed and what are the full side effects.
        /// </summary>
        /// <param name="schema"></param>
        internal void Augment(ParquetSchema schema) {
            Augment(Fields, schema.Fields, true);
        }

        internal static void Augment(IEnumerable<Field> target, IEnumerable<Field> extras, bool fixCase) {
            foreach(Field tf in target) {
                Field? xf = extras.FirstOrDefault(f => fixCase
                    ? string.Equals(f.Name, tf.Name, StringComparison.OrdinalIgnoreCase)
                    : f.Name == tf.Name);
                if(xf == null)
                    continue;

                Augment(tf.Children, xf.Children, fixCase);

                if(fixCase) {
                    tf.Rename(xf.Name);
                }
            }
        }

        #endregion

        /// <summary>
        /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object? obj) {
            if(ReferenceEquals(null, obj))
                return false;
            if(ReferenceEquals(this, obj))
                return true;
            if(obj.GetType() != GetType())
                return false;

            return Equals((ParquetSchema)obj);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode() {
            return DataFields.Aggregate(1, (current, se) => current * se.GetHashCode());
        }

        /// <summary>
        /// </summary>
        public override string ToString() {
            var sb = new StringBuilder();

            foreach(Field f in Fields) {
                sb.AppendLine(f.ToString());
            }

            return sb.ToString();
        }
    }
}