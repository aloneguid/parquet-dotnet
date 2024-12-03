using System;
using System.Collections;
using System.Collections.Generic;
using Parquet.Meta;
using Type = System.Type;

namespace Parquet.Schema {
    /// <summary>
    /// Implements a dictionary field
    /// </summary>
    public class MapField : Field {
        internal const string ContainerName = "key_value";
        internal const string KeyName = "key";
        internal const string ValueName = "value";

        private bool _keyAssigned = false;
        private bool _valueAssigned = false;

        /// <summary>
        /// Data field used as a key
        /// </summary>
        public Field Key { get; private set; }

        /// <summary>
        /// Data field used as a value
        /// </summary>
        public Field Value { get; private set; }

        /// <summary>
        /// Declares a map field
        /// </summary>
        public MapField(string name, Field keyField, Field valueField)
           : base(name, SchemaType.Map) {

            if(keyField is DataField keyDataField) {
                if(keyDataField.IsNullable) {
                    throw new ArgumentException($"map's key cannot be nullable", nameof(keyField));
                }
            }

            Key = keyField;
            Value = valueField;
            _keyAssigned = _valueAssigned = true;

            Path = new FieldPath(name, ContainerName);
            Key.PathPrefix = Path;
            Value.PathPrefix = Path;
            IsNullable = true;
        }

        internal MapField(string name)
           : base(name, SchemaType.Map) {
            Key = Value = new DataField<int>("invalid");
            IsNullable = true;
        }

        internal override void Assign(Field se) {
            if(!_keyAssigned) {
                Key = se;
                _keyAssigned = true;
            } else if(!_valueAssigned) {
                Value = se;
                _valueAssigned = true;
            } else {
                throw new InvalidOperationException($"'{Name}' already has key and value assigned");
            }
        }

        internal override FieldPath? PathPrefix {
            set {
                Path = value + new FieldPath(Name, ContainerName);
                Key.PathPrefix = Path;
                Value.PathPrefix = Path;
            }
        }

        internal override void Rename(string newName) {
            base.Rename(newName);
            PathPrefix = null;
        }

        internal override Field[] Children => new Field[] { Key, Value };

        internal SchemaElement ? GroupSchemaElement { get; set; } = null;

        internal override void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel) {

            MaxDefinitionLevel = parentDefinitionLevel;
            MaxRepetitionLevel = parentRepetitionLevel + 1; // because map is actually a list of key-values

            if(IsNullable) {
                MaxDefinitionLevel++;
            }

            if(GroupSchemaElement == null || GroupSchemaElement.RepetitionType != FieldRepetitionType.REQUIRED) {
                MaxDefinitionLevel++;
            }

            //push to children
            Key.PropagateLevels(MaxRepetitionLevel, MaxDefinitionLevel);
            Value.PropagateLevels(MaxRepetitionLevel, MaxDefinitionLevel);
        }

        /// <summary>
        /// Creates an empty dictionary to keep values for this map field. Only works when both key and value are <see cref="DataField"/>
        /// </summary>
        /// <returns></returns>
        internal IDictionary CreateSimpleDictionary() {
            Type genericType = typeof(Dictionary<,>);
            Type concreteType = genericType.MakeGenericType(
               ((DataField)Key).ClrNullableIfHasNullsType,
               ((DataField)Value).ClrNullableIfHasNullsType);

            return (IDictionary)Activator.CreateInstance(concreteType)!;
        }

        /// <inheritdoc/>
        public override bool Equals(object? obj) {
            if(obj is not MapField other)
                return false;

            return base.Equals(other) && 
                (Key?.Equals(other.Key) ?? true) && 
                (Value?.Equals(other.Value) ?? true);
        }

        /// <inheritdoc/>
        public override int GetHashCode() => base.GetHashCode();

    }
}
