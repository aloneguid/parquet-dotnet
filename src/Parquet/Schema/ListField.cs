using System;

namespace Parquet.Schema {
    /// <summary>
    /// Represents a list of items. The list can contain either a normal data field or a complex structure.
    /// If you need to get a list of primitive data fields it's more efficient to use arrays.
    /// </summary>
    public class ListField : Field {

        private bool _itemAssigned = false;

        /// <summary>
        /// Default container name for a list
        /// </summary>
        public const string DefaultContainerName = "list";

        internal string ContainerName { get; set; }

        /// <summary>
        /// Item contained within this list
        /// </summary>
        public Field Item { get; internal set; }

        /// <summary>
        /// Creates a new instance of <see cref="ListField"/>
        /// </summary>
        /// <param name="name">Field name</param>
        /// <param name="item">Field representing list element</param>
        /// <param name="containerName">Container name</param>
        public ListField(string name, Field item, string containerName = DefaultContainerName) : this(name) {
            Item = item ?? throw new ArgumentNullException(nameof(item));
            _itemAssigned = true;
            ContainerName = containerName;
            PathPrefix = null;
        }

        /// <summary>
        /// Creates a new instance of <see cref="ListField"/>
        /// </summary>
        /// <param name="name">Field name</param>
        /// <param name="dataType">Native Parquet type</param>
        /// <param name="hasNulls">When true, the field accepts null values. Note that nullable values take slightly more disk space and computing comparing to non-nullable, but are more common.</param>
        /// <param name="propertyName">When set, uses this property to get the list's data.  When not set, uses the property that matches the name parameter.</param>
        /// <param name="containerName">Container name</param>
        /// <param name="elementName">Element name</param>
        [Obsolete]
        public ListField(string name, DataType dataType, bool hasNulls = true, string? propertyName = null, string containerName = "list", string? elementName = null) : this(name) {
            Item = new DataField(elementName ?? name, dataType, hasNulls, false, propertyName ?? name);
            _itemAssigned = true;
            ContainerName = containerName;
            PathPrefix = null;
        }

        /// <summary>
        /// Creates a new instance of <see cref="ListField"/>
        /// </summary>
        /// <param name="name">Field name</param>
        /// <param name="itemDataType">Type of the item in the list</param>
        /// <param name="propertyName">When set, uses this property to get the list's data.  When not set, uses the property that matches the name parameter.</param>
        /// <param name="containerName">Container name</param>
        /// <param name="elementName">Element name</param>
        public ListField(string name,
            Type itemDataType,
            string? propertyName = null,
            string containerName = "list",
            string? elementName = null) : this(name) {
            Item = new DataField(elementName ?? name, itemDataType, null, null, propertyName ?? name);
            _itemAssigned = true;
            ContainerName = containerName;
            PathPrefix = null;
        }

        private ListField(string name) : base(name, SchemaType.List) {
            ContainerName = "list";
            Item = new DataField<int>("invalid");
        }

        internal override FieldPath? PathPrefix {
            set {
                Path = value + Name + ContainerName;
                Item.PathPrefix = Path;
            }
        }

        internal override Field[] Children => new Field[] { Item };

        internal override void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel) {
            int rl = parentRepetitionLevel;
            int dl = parentDefinitionLevel;

            //"container" is optional, therefore +1 to DL
            dl += 1;

            //"list" is repeated, both get +1
            rl += 1;
            dl += 1;

            MaxRepetitionLevel = rl;
            MaxDefinitionLevel = dl;

            //push to child item
            Item.PropagateLevels(rl, dl);
        }

        internal static ListField CreateWithNoItem(string name) {
            return new ListField(name);
        }

        internal override void Assign(Field field) {
            if(_itemAssigned)
                throw new InvalidOperationException($"item was already assigned to this list ({Name}), somethin is terribly wrong because a list can only have one item.");

            Item = field ?? throw new ArgumentNullException(nameof(field));
            _itemAssigned = true;
        }

        /// <inheritdoc/>
        public override bool Equals(object? obj) {
            if(obj is not ListField other) return false;

            return base.Equals(obj) && (Item?.Equals(other.Item) ?? true);
        }

        /// <inheritdoc/>
        public override int GetHashCode() => base.GetHashCode();
    }
}
