using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using Parquet.Meta;

namespace Parquet.Schema {

    /// <summary>
    /// Element of dataset's schema
    /// </summary>
    public abstract class Field {
        /// <summary>
        /// Type of schema in this field
        /// </summary>
        public SchemaType SchemaType { get; }

        /// <summary>
        /// Column name
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets Parquet column path. For non-nested columns always equals to column <see cref="Name"/> otherwise contains
        /// a dot (.) separated path to the column within Parquet file. Note that this is a physical path which depends on field
        /// schema and you shouldn't build any reasonable business logic based on it.
        /// </summary>
        public FieldPath Path { get; internal set; }

        /// <summary>
        /// Original nullability.
        /// </summary>
        public virtual bool IsNullable { get; internal set; } = false;

        internal List<string> GetNaturalChildPath(List<string> path) {
            if(SchemaType == SchemaType.List) {
                // element.list.element.child
                return path.Skip(3).ToList();
            }

            if(SchemaType == SchemaType.Map) {
                // element.key_value.key|value
                return path.Skip(2).ToList();
            }

            // element.child
            return path.Skip(1).ToList();
        }

        /// <summary>
        /// Max repetition level
        /// </summary>
        public int MaxRepetitionLevel { get; protected set; }

        /// <summary>
        /// Max definition level
        /// </summary>
        public int MaxDefinitionLevel { get; protected set; }

        /// <summary>
        /// Used internally for serialisation
        /// </summary>
        internal string? ClrPropName { get; set; }

        /// <summary>
        /// Low-level schema element corresponding to this high-level schema element.
        /// Only set when reading files.
        /// </summary>
        public SchemaElement? SchemaElement { get; internal set; }

        internal virtual FieldPath? PathPrefix { set { } }

        internal int? Order { get; set; }

        /// <summary>
        /// Constructs a field with only requiremd parameters
        /// </summary>
        /// <param name="name">Field name, required</param>
        /// <param name="schemaType">Type of schema of this field</param>
        protected Field(string name, SchemaType schemaType) {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            SchemaType = schemaType;
            Path = new FieldPath(name);
        }

        #region [ Internal Helpers ]

        /// <summary>
        /// Called by schema when field hierarchy is constructed, so that fields can calculate levels as this is
        /// done in reverse order of construction and needs to be done after schema data is ready
        /// </summary>
        internal abstract void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel);

        internal virtual void Assign(Field field) {
            //only used by some schema fields internally to help construct a field hierarchy
        }

        /// <summary>
        /// Get child fields, which only makes sense for complex types
        /// </summary>
        internal virtual Field[] Children { get; } = Array.Empty<Field>();

        internal virtual Field[] NaturalChildren {
            get {
                if(SchemaType == SchemaType.List) {
                    return Children[0].Children;
                }

                return Children;
            }
        }

        internal virtual bool IsAtomic => false;

        /// <summary>
        /// Rename this field. Renaming should also fix up the path in complex nested schemas.
        /// </summary>
        /// <param name="newName"></param>
        internal virtual void Rename(string newName) {
            Name = newName;
            Path = new FieldPath(newName);
        }

        internal bool Equals(SchemaElement tse) {
            if(ReferenceEquals(tse, null))
                return false;

            return tse.Name == Name;
        }

        #endregion

        /// <inheritdoc/>
        public override string ToString() => $"{Path} ({SchemaType}, RL: {MaxRepetitionLevel}, DL: {MaxDefinitionLevel})";

        /// <summary>
        /// Basic equality check
        /// </summary>
        public override bool Equals(object? obj) {

            if(obj is not Field other) return false;

            return SchemaType == other.SchemaType && Name == other.Name && Path.Equals(other.Path);
        }

        /// <summary>
        /// GetHashCode
        /// </summary>
        public override int GetHashCode() => Path.ToString().GetHashCode();
    }
}