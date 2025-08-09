using System;
using Parquet.Encodings;
using Parquet.Extensions;
using Parquet.File;

namespace Parquet.Schema {
    /// <summary>
    /// Field containing actual data, unlike fields containing metadata.
    /// </summary>
    public class DataField : Field, ICloneable {

        private bool _isNullable;
        private bool _isArray;

        /// <summary>
        /// When true, this element is allowed to have nulls. Bad naming, probably should be something like IsNullable.
        /// Changes <see cref="ClrNullableIfHasNullsType"/> property accordingly.
        /// </summary>
        public override bool IsNullable {
            get => _isNullable; internal set {
                _isNullable = value;
                ClrNullableIfHasNullsType = value ? ClrType.GetNullable() : ClrType;
            }
        }

        /// <summary>
        /// When true, this element is allowed to have nulls. Bad naming, probably should be something like IsNullable.
        /// </summary>
        [Obsolete("Use IsNullable instead.")]
        public bool HasNulls => IsNullable;

        /// <summary>
        /// When true, the value is an array rather than a single value.
        /// </summary>
        public bool IsArray {
            get => _isArray; internal set {
                _isArray = value;
                MaxRepetitionLevel = value ? 1 : 0;
            }
        }

        /// <summary>
        /// CLR type of this column. For nullable columns this type is not nullable.
        /// </summary>
        public Type ClrType { get; private set; }

        /// <summary>
        /// Unsupported, use at your own risk!
        /// </summary>
        public Type ClrNullableIfHasNullsType { get; set; } = typeof(void);

        /// <summary>
        /// Creates a new instance of <see cref="DataField"/> by name and CLR type.
        /// </summary>
        /// <param name="name">Field name</param>
        /// <param name="clrType">CLR type of this field. The type is internally discovered and expanded into appropriate Parquet flags.</param>
        /// <param name="isNullable">When set, will override <see cref="IsNullable"/> attribute regardless whether passed type was nullable or not.</param>
        /// <param name="isArray">When set, will override <see cref="IsArray"/> attribute regardless whether passed type was an array or not.</param>
        /// <param name="propertyName">When set, uses this property to get the field's data.  When not set, uses the property that matches the name parameter.</param>
        public DataField(string name, Type clrType, bool? isNullable = null, bool? isArray = null, string? propertyName = null)
           : base(name, SchemaType.Data) {

            Discover(clrType, out Type baseType, out bool discIsArray, out bool discIsNullable);
            ClrType = baseType;
            if(!SchemaEncoder.IsSupported(ClrType)) {
                if(baseType == typeof(DateTimeOffset)) {
                    throw new NotSupportedException($"{nameof(DateTimeOffset)} support was dropped due to numerous ambiguity issues, please use {nameof(DateTime)} from now on.");
                } else {
                    throw new NotSupportedException($"type {clrType} is not supported");
                }
            }

            IsNullable = isNullable ?? discIsNullable;
            IsArray = isArray ?? discIsArray;
            ClrPropName = propertyName ?? name;
            MaxRepetitionLevel = IsArray ? 1 : 0;
        }

        internal override FieldPath? PathPrefix {
            set => Path = value + new FieldPath(Name);
        }

        internal bool IsAttachedToSchema { get; set; } = false;

        internal void EnsureAttachedToSchema(string argName) {
            if(IsAttachedToSchema)
                return;

            throw new ArgumentException(
                    $"Field [{this}] is not attached to any schema. You need to construct a schema passing in this field first.",
                    argName);
        }

        internal override void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel) {
            MaxRepetitionLevel = parentRepetitionLevel;
            if(IsArray)
                MaxRepetitionLevel++;

            MaxDefinitionLevel = parentDefinitionLevel;

            // can't be both array and nullable
            if(IsArray)
                MaxDefinitionLevel++;
            else if(IsNullable)
                MaxDefinitionLevel++;

            IsAttachedToSchema = true;
        }

        /// <summary>
        /// Creates non-nullable uninitialised array to hold this data type.
        /// </summary>
        /// <param name="length">Exact array size</param>
        /// <returns></returns>
        internal Array CreateArray(int length) => Array.CreateInstance(ClrType, length);

        internal Array UnpackDefinitions(Array definedData, Span<int> definitionLevels) {
            if(IsNullable) {
                Array result = Array.CreateInstance(ClrNullableIfHasNullsType, definitionLevels.Length);
                definedData.UnpackNullsFast(definitionLevels, MaxDefinitionLevel, result);
                return result;
            } else {
                return definedData;
            }
        }

        internal override bool IsAtomic => true;

        internal bool IsDeltaEncodable => DeltaBinaryPackedEncoder.IsSupported(ClrType);

        /// <inheritdoc/>
        public override string ToString() =>
            $"{Path} ({ClrType}{(_isNullable ? "?" : "")}{(_isArray ? "[]" : "")})";

        private Type BaseClrType {
            get {
#if NET6_0_OR_GREATER
                if(ClrType == typeof(DateOnly))
                    return typeof(DateTime);

                if(ClrType == typeof(TimeOnly))
                    return typeof(TimeSpan);
#endif

                return ClrType;
            }
        }

        /// <summary>
        /// Basic equality check
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object? obj) {
            if(obj is not DataField other)
                return false;

            return base.Equals(obj) &&
                BaseClrType == other.BaseClrType &&
                IsNullable == other.IsNullable &&
                IsArray == other.IsArray;
        }

        /// <inheritdoc/>
        public override int GetHashCode() => base.GetHashCode();

        #region [ Type Resolution ]

        private static void Discover(Type t, out Type baseType, out bool isArray, out bool isNullable) {
            baseType = t;
            isArray = false;
            isNullable = false;

            //throw a useful hint
            if(t.IsGenericIDictionary()) {
                throw new NotSupportedException($"cannot declare a dictionary this way, please use {nameof(MapField)}.");
            }

            if(t.TryExtractIEnumerableType(out Type? enumItemType)) {
                baseType = enumItemType!;
                isArray = true;
            }

            if(baseType.IsNullable()) {
                baseType = baseType.GetNonNullable();
                isNullable = true;
            }
        }

        /// <summary>
        /// Simple memberwise clone
        /// </summary>
        /// <returns></returns>
        public object Clone() {
            return MemberwiseClone();
        }

        #endregion
    }
}