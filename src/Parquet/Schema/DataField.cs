using System;
using Parquet.Data;
using Parquet.File;

namespace Parquet.Schema {
    /// <summary>
    /// Field containing actual data, unlike fields containing metadata.
    /// </summary>
    public class DataField : Field {
        /// <summary>
        /// Parquet data type of this element
        /// </summary>
        public DataType DataType { get; }

        /// <summary>
        /// When true, this element is allowed to have nulls. Bad naming, probably should be something like IsNullable.
        /// </summary>
        public bool IsNullable { get; }

        /// <summary>
        /// When true, this element is allowed to have nulls. Bad naming, probably should be something like IsNullable.
        /// </summary>
        [Obsolete("Use IsNullable instead.")]
        public bool HasNulls => IsNullable;

        /// <summary>
        /// When true, the value is an array rather than a single value.
        /// </summary>
        public bool IsArray { get; }

        /// <summary>
        /// CLR type of this column. For nullable columns this type is not nullable.
        /// </summary>
        public Type ClrType { get; private set; }

        /// <summary>
        /// Unsupported, use at your own risk!
        /// </summary>
        public Type ClrNullableIfHasNullsType { get; set; }

        /// <summary>
        /// Creates a new instance of <see cref="DataField"/> by name and CLR type.
        /// </summary>
        /// <param name="name">Field name</param>
        /// <param name="clrType">CLR type of this field. The type is internally discovered and expanded into appropriate Parquet flags.</param>
        public DataField(string name, Type clrType)
           : this(name,
                Discover(clrType).dataType,
                Discover(clrType).hasNulls,
                Discover(clrType).isArray,
                null) {
            //todo: calls to Discover() can be killed by making a constructor method
        }

        /// <summary>
        /// Creates a new instance of <see cref="DataField"/> by specifying all the required attributes.
        /// </summary>
        /// <param name="name">Field name.</param>
        /// <param name="dataType">Native Parquet type</param>
        /// <param name="hasNulls">When true, the field accepts null values. Note that nullable values take slightly more disk space and computing comparing to non-nullable, but are more common.</param>
        /// <param name="isArray">When true, each value of this field can have multiple values, similar to array in C#.</param>
        /// <param name="propertyName">When set, uses this property to get the field's data.  When not set, uses the property that matches the name parameter.</param>
        public DataField(string name, DataType dataType, bool hasNulls = true, bool isArray = false, string propertyName = null) :
            base(name, SchemaType.Data) {

            DataType = dataType;
            IsNullable = hasNulls;
            IsArray = isArray;
            ClrPropName = propertyName ?? name;

            MaxRepetitionLevel = isArray ? 1 : 0;

            ClrType = SchemaEncoder.FindSystemType(dataType);
            if(ClrType != null)
                ClrNullableIfHasNullsType = hasNulls ? ClrType.GetNullable() : ClrType;
        }

        internal override FieldPath PathPrefix {
            set {
                Path = value + new FieldPath(Name);
            }
        }

        /// <summary>
        /// see <see cref="ThriftFooter.GetLevels(Thrift.ColumnChunk, out int, out int)"/>
        /// </summary>
        internal override void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel) {
            MaxRepetitionLevel = parentRepetitionLevel + (IsArray ? 1 : 0);
            MaxDefinitionLevel = parentDefinitionLevel + (IsNullable ? 1 : 0);
        }

        /// <summary>
        /// Creates non-nullable uninitialised array to hold this data type.
        /// </summary>
        /// <param name="length">Exact array size</param>
        /// <returns></returns>
        internal Array CreateArray(int length) {
            return Array.CreateInstance(ClrType, length);
        }

        internal Array CreateNullableArray(int length) {
            return Array.CreateInstance(ClrNullableIfHasNullsType, length);
        }

        internal Array UnpackDefinitions(Array definedData, int[] definitionLevels, int maxDefinitionLevel) {
            Array result = CreateNullableArray(definitionLevels.Length);

            int isrc = 0;
            for(int i = 0; i < definitionLevels.Length; i++) {
                int level = definitionLevels[i];

                if(level == maxDefinitionLevel) {
                    result.SetValue(definedData.GetValue(isrc++), i);
                }
            }
            return result;
        }

        /// <inheritdoc/>
        public override string ToString() => $"{Path} ({DataType})";

        /// <summary>
        /// Basic equality check
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj) {
            if(obj is not DataField other)
                return false;

            return base.Equals(obj) &&
                DataType == other.DataType &&
                IsNullable == other.IsNullable &&
                IsArray == other.IsArray;
        }

        /// <summary>
        /// Basic GetHashCode
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode() => base.GetHashCode();

        #region [ Type Resolution ]

        private struct CInfo {
            public DataType dataType;
            public Type baseType;
            public bool isArray;
            public bool hasNulls;
        }

        private static CInfo Discover(Type t) {
            Type baseType = t;
            bool isArray = false;
            bool hasNulls = false;

            //throw a useful hint
            if(t.TryExtractDictionaryType(out Type dKey, out Type dValue)) {
                throw new ArgumentException($"cannot declare a dictionary this way, please use {nameof(MapField)}.");
            }

            if(t.TryExtractEnumerableType(out Type enumItemType)) {
                baseType = enumItemType;
                isArray = true;
            }

            if(baseType.IsNullable()) {
                baseType = baseType.GetNonNullable();
                hasNulls = true;
            }

            DataType? dtn = SchemaEncoder.FindDataType(baseType);
            if(dtn == null)
                throw new NotSupportedException($"type '{baseType}' is not supported");
            DataType dataType = dtn.Value;

            return new CInfo {
                dataType = dataType,
                baseType = baseType,
                isArray = isArray,
                hasNulls = hasNulls
            };
        }

        #endregion
    }
}