using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Data;

namespace Parquet.Schema {
    /// <summary>
    /// Represents a structure i.e. a container for other fields.
    /// </summary>
    public class StructField : Field, IEquatable<StructField> {
        private readonly List<Field> _fields = new List<Field>();

        /// <summary>
        /// Creates a new structure field 
        /// </summary>
        /// <param name="name">Structure name</param>
        /// <param name="elements">List of elements</param>
        public StructField(string name, params Field[] elements) : this(name) {
            if(elements == null || elements.Length == 0)                 throw new ArgumentException($"structure '{name}' requires at least one element");

            //path for structures has no weirdnes, yay!

            foreach(Field field in elements)                 _fields.Add(field);

            Path = name;
            PathPrefix = null;
        }

        internal override FieldPath PathPrefix {
            set {
                Path = value + Name;

                foreach(Field field in _fields)                     field.PathPrefix = Path;
            }
        }

        internal override void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel) {
            //struct is a container, it doesn't have any levels

            foreach(Field f in Fields)                 f.PropagateLevels(parentRepetitionLevel, parentDefinitionLevel);
        }

        private StructField(string name) : base(name, SchemaType.Struct) {

        }

        internal static StructField CreateWithNoElements(string name) {
            return new StructField(name);
        }

        /// <summary>
        /// Elements of this structure
        /// </summary>
        public IReadOnlyList<Field> Fields => _fields;

        internal override void Assign(Field se) {
            _fields.Add(se);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(StructField other) {
            bool ok = base.Equals(other) && Fields.Count == other.Fields.Count;

            if(ok) {
                for(int i = 0; i < Fields.Count; i++) {
                    if(!Fields[i].Equals(other.Fields[i])) {
                        ok = false;
                        break;
                    }
                }
            }

            return ok;
        }
    }
}