#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
using System.Linq;
using System.Collections.Generic;
using Parquet.Meta.Proto;
namespace Parquet.Meta {
    /// <summary>
    /// Types supported by Parquet.  These types are intended to be used in combination with the encodings to control the on disk storage format. For example INT16 is not included as a type since a good encoding of INT32 would handle this.
    /// </summary>
    public enum Type {
        BOOLEAN = 0,

        INT32 = 1,

        INT64 = 2,

        INT96 = 3,

        FLOAT = 4,

        DOUBLE = 5,

        BYTE_ARRAY = 6,

        FIXED_LEN_BYTE_ARRAY = 7,

    }

    /// <summary>
    /// DEPRECATED: Common types used by frameworks(e.g. hive, pig) using parquet. ConvertedType is superseded by LogicalType.  This enum should not be extended.  See LogicalTypes.md for conversion between ConvertedType and LogicalType.
    /// </summary>
    public enum ConvertedType {
        /// <summary>
        /// A BYTE_ARRAY actually contains UTF8 encoded chars.
        /// </summary>
        UTF8 = 0,

        /// <summary>
        /// A map is converted as an optional field containing a repeated key/value pair.
        /// </summary>
        MAP = 1,

        /// <summary>
        /// A key/value pair is converted into a group of two fields.
        /// </summary>
        MAP_KEY_VALUE = 2,

        /// <summary>
        /// A list is converted into an optional field containing a repeated field for its values.
        /// </summary>
        LIST = 3,

        /// <summary>
        /// An enum is converted into a BYTE_ARRAY field.
        /// </summary>
        ENUM = 4,

        /// <summary>
        /// A decimal value.  This may be used to annotate BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY primitive types. The underlying byte array stores the unscaled value encoded as two&#39;s complement using big-endian byte order (the most significant byte is the zeroth element). The value of the decimal is the value * 10^{-scale}.  This must be accompanied by a (maximum) precision and a scale in the SchemaElement. The precision specifies the number of digits in the decimal and the scale stores the location of the decimal point. For example 1.23 would have precision 3 (3 total digits) and scale 2 (the decimal point is 2 digits over).
        /// </summary>
        DECIMAL = 5,

        /// <summary>
        /// A Date  Stored as days since Unix epoch, encoded as the INT32 physical type.
        /// </summary>
        DATE = 6,

        /// <summary>
        /// A time  The total number of milliseconds since midnight.  The value is stored as an INT32 physical type.
        /// </summary>
        TIME_MILLIS = 7,

        /// <summary>
        /// A time.  The total number of microseconds since midnight.  The value is stored as an INT64 physical type.
        /// </summary>
        TIME_MICROS = 8,

        /// <summary>
        /// A date/time combination  Date and time recorded as milliseconds since the Unix epoch.  Recorded as a physical type of INT64.
        /// </summary>
        TIMESTAMP_MILLIS = 9,

        /// <summary>
        /// A date/time combination  Date and time recorded as microseconds since the Unix epoch.  The value is stored as an INT64 physical type.
        /// </summary>
        TIMESTAMP_MICROS = 10,

        /// <summary>
        /// An unsigned integer value.  The number describes the maximum number of meaningful data bits in the stored value. 8, 16 and 32 bit values are stored using the INT32 physical type.  64 bit values are stored using the INT64 physical type.
        /// </summary>
        UINT_8 = 11,

        UINT_16 = 12,

        UINT_32 = 13,

        UINT_64 = 14,

        /// <summary>
        /// A signed integer value.  The number describes the maximum number of meaningful data bits in the stored value. 8, 16 and 32 bit values are stored using the INT32 physical type.  64 bit values are stored using the INT64 physical type.
        /// </summary>
        INT_8 = 15,

        INT_16 = 16,

        INT_32 = 17,

        INT_64 = 18,

        /// <summary>
        /// An embedded JSON document  A JSON document embedded within a single UTF8 column.
        /// </summary>
        JSON = 19,

        /// <summary>
        /// An embedded BSON document  A BSON document embedded within a single BYTE_ARRAY column.
        /// </summary>
        BSON = 20,

        /// <summary>
        /// An interval of time  This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12 This data is composed of three separate little endian unsigned integers.  Each stores a component of a duration of time.  The first integer identifies the number of months associated with the duration, the second identifies the number of days associated with the duration and the third identifies the number of milliseconds associated with the provided duration.  This duration of time is independent of any particular timezone or date.
        /// </summary>
        INTERVAL = 21,

    }

    /// <summary>
    /// Representation of Schemas.
    /// </summary>
    public enum FieldRepetitionType {
        /// <summary>
        /// This field is required (can not be null) and each row has exactly 1 value.
        /// </summary>
        REQUIRED = 0,

        /// <summary>
        /// The field is optional (can be null) and each row has 0 or 1 values.
        /// </summary>
        OPTIONAL = 1,

        /// <summary>
        /// The field is repeated and can contain 0 or more values.
        /// </summary>
        REPEATED = 2,

    }

    /// <summary>
    /// Encodings supported by Parquet.  Not all encodings are valid for all types.  These enums are also used to specify the encoding of definition and repetition levels. See the accompanying doc for the details of the more complicated encodings.
    /// </summary>
    public enum Encoding {
        /// <summary>
        /// Default encoding. BOOLEAN - 1 bit per value. 0 is false; 1 is true. INT32 - 4 bytes per value.  Stored as little-endian. INT64 - 8 bytes per value.  Stored as little-endian. FLOAT - 4 bytes per value.  IEEE. Stored as little-endian. DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian. BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes. FIXED_LEN_BYTE_ARRAY - Just the bytes.
        /// </summary>
        PLAIN = 0,

        /// <summary>
        /// Deprecated: Dictionary encoding. The values in the dictionary are encoded in the plain type. in a data page use RLE_DICTIONARY instead. in a Dictionary page use PLAIN instead.
        /// </summary>
        PLAIN_DICTIONARY = 2,

        /// <summary>
        /// Group packed run length encoding. Usable for definition/repetition levels encoding and Booleans (on one bit: 0 is false; 1 is true.).
        /// </summary>
        RLE = 3,

        /// <summary>
        /// Bit packed encoding.  This can only be used if the data has a known max width.  Usable for definition/repetition levels encoding.
        /// </summary>
        BIT_PACKED = 4,

        /// <summary>
        /// Delta encoding for integers. This can be used for int columns and works best on sorted data.
        /// </summary>
        DELTA_BINARY_PACKED = 5,

        /// <summary>
        /// Encoding for byte arrays to separate the length values and the data. The lengths are encoded using DELTA_BINARY_PACKED.
        /// </summary>
        DELTA_LENGTH_BYTE_ARRAY = 6,

        /// <summary>
        /// Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED. Suffixes are stored as delta length byte arrays.
        /// </summary>
        DELTA_BYTE_ARRAY = 7,

        /// <summary>
        /// Dictionary encoding: the ids are encoded using the RLE encoding.
        /// </summary>
        RLE_DICTIONARY = 8,

        /// <summary>
        /// Encoding for fixed-width data (FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY). K byte-streams are created where K is the size in bytes of the data type. The individual bytes of a value are scattered to the corresponding stream and the streams are concatenated. This itself does not reduce the size of the data but can lead to better compression afterwards.  Added in 2.8 for FLOAT and DOUBLE. Support for INT32, INT64 and FIXED_LEN_BYTE_ARRAY added in 2.11.
        /// </summary>
        BYTE_STREAM_SPLIT = 9,

    }

    /// <summary>
    /// Supported compression algorithms.  Codecs added in format version X.Y can be read by readers based on X.Y and later. Codec support may vary between readers based on the format version and libraries available at runtime.  See Compression.md for a detailed specification of these algorithms.
    /// </summary>
    public enum CompressionCodec {
        UNCOMPRESSED = 0,

        SNAPPY = 1,

        GZIP = 2,

        LZO = 3,

        BROTLI = 4,

        LZ4 = 5,

        ZSTD = 6,

        LZ4_RAW = 7,

    }

    public enum PageType {
        DATA_PAGE = 0,

        INDEX_PAGE = 1,

        DICTIONARY_PAGE = 2,

        DATA_PAGE_V2 = 3,

    }

    /// <summary>
    /// Enum to annotate whether lists of min/max elements inside ColumnIndex are ordered and if so, in which direction.
    /// </summary>
    public enum BoundaryOrder {
        UNORDERED = 0,

        ASCENDING = 1,

        DESCENDING = 2,

    }

    /// <summary>
    /// A structure for capturing metadata for estimating the unencoded, uncompressed size of data written. This is useful for readers to estimate how much memory is needed to reconstruct data in their memory model and for fine grained filter pushdown on nested structures (the histograms contained in this structure can help determine the number of nulls at a particular nesting level and maximum length of lists).
    /// </summary>
    public class SizeStatistics {
        /// <summary>
        /// The number of physical bytes stored for BYTE_ARRAY data values assuming no encoding. This is exclusive of the bytes needed to store the length of each byte array. In other words, this field is equivalent to the `(size of PLAIN-ENCODING the byte array values) - (4 bytes * number of values written)`. To determine unencoded sizes of other types readers can use schema information multiplied by the number of non-null and null values. The number of null/non-null values can be inferred from the histograms below.  For example, if a column chunk is dictionary-encoded with dictionary [&quot;a&quot;, &quot;bc&quot;, &quot;cde&quot;], and a data page contains the indices [0, 0, 1, 2], then this value for that data page should be 7 (1 + 1 + 2 + 3).  This field should only be set for types that use BYTE_ARRAY as their physical type.
        /// </summary>
        public long? UnencodedByteArrayDataBytes { get; set; }

        /// <summary>
        /// When present, there is expected to be one element corresponding to each repetition (i.e. size=max repetition_level+1) where each element represents the number of times the repetition level was observed in the data.  This field may be omitted if max_repetition_level is 0 without loss of information.
        /// </summary>
        public List<long>? RepetitionLevelHistogram { get; set; }

        /// <summary>
        /// Same as repetition_level_histogram except for definition levels.  This field may be omitted if max_definition_level is 0 or 1 without loss of information.
        /// </summary>
        public List<long>? DefinitionLevelHistogram { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: UnencodedByteArrayDataBytes, i64
            if(UnencodedByteArrayDataBytes != null) {
                proto.WriteI64Field(1, UnencodedByteArrayDataBytes.Value);
            }
            // 2: RepetitionLevelHistogram, list
            if(RepetitionLevelHistogram != null) {
                proto.WriteListBegin(2, 6, RepetitionLevelHistogram.Count);
                foreach(long element in RepetitionLevelHistogram) {
                    proto.WriteI64Value(element);
                }
            }
            // 3: DefinitionLevelHistogram, list
            if(DefinitionLevelHistogram != null) {
                proto.WriteListBegin(3, 6, DefinitionLevelHistogram.Count);
                foreach(long element in DefinitionLevelHistogram) {
                    proto.WriteI64Value(element);
                }
            }

            proto.StructEnd();
        }

        internal static SizeStatistics Read(ThriftCompactProtocolReader proto) {
            var r = new SizeStatistics();
            proto.StructBegin();
            int elementCount = 0;
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // UnencodedByteArrayDataBytes, i64
                        r.UnencodedByteArrayDataBytes = proto.ReadI64();
                        break;
                    case 2: // RepetitionLevelHistogram, list
                        elementCount = proto.ReadListHeader(out _);
                        r.RepetitionLevelHistogram = new List<long>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.RepetitionLevelHistogram.Add(proto.ReadI64()); }
                        break;
                    case 3: // DefinitionLevelHistogram, list
                        elementCount = proto.ReadListHeader(out _);
                        r.DefinitionLevelHistogram = new List<long>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.DefinitionLevelHistogram.Add(proto.ReadI64()); }
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Statistics per row group and per page All fields are optional.
    /// </summary>
    public class Statistics {
        /// <summary>
        /// DEPRECATED: min and max value of the column. Use min_value and max_value.  Values are encoded using PLAIN encoding, except that variable-length byte arrays do not include a length prefix.  These fields encode min and max values determined by signed comparison only. New files should use the correct order for a column&#39;s logical type and store the values in the min_value and max_value fields.  To support older readers, these may be set when the column order is signed.
        /// </summary>
        public byte[]? Max { get; set; }

        public byte[]? Min { get; set; }

        /// <summary>
        /// Count of null values in the column.  Writers SHOULD always write this field even if it is zero (i.e. no null value) or the column is not nullable. Readers MUST distinguish between null_count not being present and null_count == 0. If null_count is not present, readers MUST NOT assume null_count == 0.
        /// </summary>
        public long? NullCount { get; set; }

        /// <summary>
        /// Count of distinct values occurring.
        /// </summary>
        public long? DistinctCount { get; set; }

        /// <summary>
        /// Lower and upper bound values for the column, determined by its ColumnOrder.  These may be the actual minimum and maximum values found on a page or column chunk, but can also be (more compact) values that do not exist on a page or column chunk. For example, instead of storing &quot;Blart Versenwald III&quot;, a writer may set min_value=&quot;B&quot;, max_value=&quot;C&quot;. Such more compact values must still be valid values within the column&#39;s logical type.  Values are encoded using PLAIN encoding, except that variable-length byte arrays do not include a length prefix.
        /// </summary>
        public byte[]? MaxValue { get; set; }

        public byte[]? MinValue { get; set; }

        /// <summary>
        /// If true, max_value is the actual maximum value for a column.
        /// </summary>
        public bool? IsMaxValueExact { get; set; }

        /// <summary>
        /// If true, min_value is the actual minimum value for a column.
        /// </summary>
        public bool? IsMinValueExact { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Max, binary
            if(Max != null) {
                proto.WriteBinaryField(1, Max);
            }
            // 2: Min, binary
            if(Min != null) {
                proto.WriteBinaryField(2, Min);
            }
            // 3: NullCount, i64
            if(NullCount != null) {
                proto.WriteI64Field(3, NullCount.Value);
            }
            // 4: DistinctCount, i64
            if(DistinctCount != null) {
                proto.WriteI64Field(4, DistinctCount.Value);
            }
            // 5: MaxValue, binary
            if(MaxValue != null) {
                proto.WriteBinaryField(5, MaxValue);
            }
            // 6: MinValue, binary
            if(MinValue != null) {
                proto.WriteBinaryField(6, MinValue);
            }
            // 7: IsMaxValueExact, bool
            if(IsMaxValueExact != null) {
                proto.WriteBoolField(7, IsMaxValueExact.Value);
            }
            // 8: IsMinValueExact, bool
            if(IsMinValueExact != null) {
                proto.WriteBoolField(8, IsMinValueExact.Value);
            }

            proto.StructEnd();
        }

        internal static Statistics Read(ThriftCompactProtocolReader proto) {
            var r = new Statistics();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Max, binary
                        r.Max = proto.ReadBinary();
                        break;
                    case 2: // Min, binary
                        r.Min = proto.ReadBinary();
                        break;
                    case 3: // NullCount, i64
                        r.NullCount = proto.ReadI64();
                        break;
                    case 4: // DistinctCount, i64
                        r.DistinctCount = proto.ReadI64();
                        break;
                    case 5: // MaxValue, binary
                        r.MaxValue = proto.ReadBinary();
                        break;
                    case 6: // MinValue, binary
                        r.MinValue = proto.ReadBinary();
                        break;
                    case 7: // IsMaxValueExact, bool
                        r.IsMaxValueExact = compactType == CompactType.BooleanTrue;
                        break;
                    case 8: // IsMinValueExact, bool
                        r.IsMinValueExact = compactType == CompactType.BooleanTrue;
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Empty structs to use as logical type annotations.
    /// </summary>
    public class StringType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static StringType Read(ThriftCompactProtocolReader proto) {
            var r = new StringType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class UUIDType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static UUIDType Read(ThriftCompactProtocolReader proto) {
            var r = new UUIDType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class MapType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static MapType Read(ThriftCompactProtocolReader proto) {
            var r = new MapType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class ListType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static ListType Read(ThriftCompactProtocolReader proto) {
            var r = new ListType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class EnumType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static EnumType Read(ThriftCompactProtocolReader proto) {
            var r = new EnumType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class DateType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static DateType Read(ThriftCompactProtocolReader proto) {
            var r = new DateType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class Float16Type {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static Float16Type Read(ThriftCompactProtocolReader proto) {
            var r = new Float16Type();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Logical type to annotate a column that is always null.  Sometimes when discovering the schema of existing data, values are always null and the physical type can&#39;t be determined. This annotation signals the case where the physical type was guessed from all null values.
    /// </summary>
    public class NullType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static NullType Read(ThriftCompactProtocolReader proto) {
            var r = new NullType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Decimal logical type annotation  Scale must be zero or a positive integer less than or equal to the precision. Precision must be a non-zero positive integer.  To maintain forward-compatibility in v1, implementations using this logical type must also set scale and precision on the annotated SchemaElement.  Allowed for physical types: INT32, INT64, FIXED_LEN_BYTE_ARRAY, and BYTE_ARRAY.
    /// </summary>
    public class DecimalType {
        public int Scale { get; set; }

        public int Precision { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Scale, i32
            proto.WriteI32Field(1, Scale);
            // 2: Precision, i32
            proto.WriteI32Field(2, Precision);

            proto.StructEnd();
        }

        internal static DecimalType Read(ThriftCompactProtocolReader proto) {
            var r = new DecimalType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Scale, i32
                        r.Scale = proto.ReadI32();
                        break;
                    case 2: // Precision, i32
                        r.Precision = proto.ReadI32();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Time units for logical types.
    /// </summary>
    public class MilliSeconds {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static MilliSeconds Read(ThriftCompactProtocolReader proto) {
            var r = new MilliSeconds();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class MicroSeconds {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static MicroSeconds Read(ThriftCompactProtocolReader proto) {
            var r = new MicroSeconds();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class NanoSeconds {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static NanoSeconds Read(ThriftCompactProtocolReader proto) {
            var r = new NanoSeconds();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class TimeUnit {
        public MilliSeconds? MILLIS { get; set; }

        public MicroSeconds? MICROS { get; set; }

        public NanoSeconds? NANOS { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: MILLIS, id
            if(MILLIS != null) {
                proto.BeginInlineStruct(1);
                MILLIS.Write(proto);
            }
            // 2: MICROS, id
            if(MICROS != null) {
                proto.BeginInlineStruct(2);
                MICROS.Write(proto);
            }
            // 3: NANOS, id
            if(NANOS != null) {
                proto.BeginInlineStruct(3);
                NANOS.Write(proto);
            }

            proto.StructEnd();
        }

        internal static TimeUnit Read(ThriftCompactProtocolReader proto) {
            var r = new TimeUnit();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // MILLIS, id
                        r.MILLIS = MilliSeconds.Read(proto);
                        break;
                    case 2: // MICROS, id
                        r.MICROS = MicroSeconds.Read(proto);
                        break;
                    case 3: // NANOS, id
                        r.NANOS = NanoSeconds.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Timestamp logical type annotation  Allowed for physical types: INT64.
    /// </summary>
    public class TimestampType {
        public bool IsAdjustedToUTC { get; set; }

        public TimeUnit Unit { get; set; } = new TimeUnit();


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: IsAdjustedToUTC, bool
            proto.WriteBoolField(1, IsAdjustedToUTC);
            // 2: Unit, id
            proto.BeginInlineStruct(2);
            Unit.Write(proto);

            proto.StructEnd();
        }

        internal static TimestampType Read(ThriftCompactProtocolReader proto) {
            var r = new TimestampType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // IsAdjustedToUTC, bool
                        r.IsAdjustedToUTC = compactType == CompactType.BooleanTrue;
                        break;
                    case 2: // Unit, id
                        r.Unit = TimeUnit.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Time logical type annotation  Allowed for physical types: INT32 (millis), INT64 (micros, nanos).
    /// </summary>
    public class TimeType {
        public bool IsAdjustedToUTC { get; set; }

        public TimeUnit Unit { get; set; } = new TimeUnit();


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: IsAdjustedToUTC, bool
            proto.WriteBoolField(1, IsAdjustedToUTC);
            // 2: Unit, id
            proto.BeginInlineStruct(2);
            Unit.Write(proto);

            proto.StructEnd();
        }

        internal static TimeType Read(ThriftCompactProtocolReader proto) {
            var r = new TimeType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // IsAdjustedToUTC, bool
                        r.IsAdjustedToUTC = compactType == CompactType.BooleanTrue;
                        break;
                    case 2: // Unit, id
                        r.Unit = TimeUnit.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Integer logical type annotation  bitWidth must be 8, 16, 32, or 64.  Allowed for physical types: INT32, INT64.
    /// </summary>
    public class IntType {
        public sbyte BitWidth { get; set; }

        public bool IsSigned { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: BitWidth, i8
            proto.WriteByteField(1, BitWidth);
            // 2: IsSigned, bool
            proto.WriteBoolField(2, IsSigned);

            proto.StructEnd();
        }

        internal static IntType Read(ThriftCompactProtocolReader proto) {
            var r = new IntType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // BitWidth, i8
                        r.BitWidth = proto.ReadByte();
                        break;
                    case 2: // IsSigned, bool
                        r.IsSigned = compactType == CompactType.BooleanTrue;
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Embedded JSON logical type annotation  Allowed for physical types: BYTE_ARRAY.
    /// </summary>
    public class JsonType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static JsonType Read(ThriftCompactProtocolReader proto) {
            var r = new JsonType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Embedded BSON logical type annotation  Allowed for physical types: BYTE_ARRAY.
    /// </summary>
    public class BsonType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static BsonType Read(ThriftCompactProtocolReader proto) {
            var r = new BsonType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Embedded Variant logical type annotation.
    /// </summary>
    public class VariantType {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static VariantType Read(ThriftCompactProtocolReader proto) {
            var r = new VariantType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// LogicalType annotations to replace ConvertedType.  To maintain compatibility, implementations using LogicalType for a SchemaElement must also set the corresponding ConvertedType (if any) from the following table.
    /// </summary>
    public class LogicalType {
        public StringType? STRING { get; set; }

        public MapType? MAP { get; set; }

        public ListType? LIST { get; set; }

        public EnumType? ENUM { get; set; }

        public DecimalType? DECIMAL { get; set; }

        public DateType? DATE { get; set; }

        public TimeType? TIME { get; set; }

        public TimestampType? TIMESTAMP { get; set; }

        public IntType? INTEGER { get; set; }

        public NullType? UNKNOWN { get; set; }

        public JsonType? JSON { get; set; }

        public BsonType? BSON { get; set; }

        public UUIDType? UUID { get; set; }

        public Float16Type? FLOAT16 { get; set; }

        public VariantType? VARIANT { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: STRING, id
            if(STRING != null) {
                proto.BeginInlineStruct(1);
                STRING.Write(proto);
            }
            // 2: MAP, id
            if(MAP != null) {
                proto.BeginInlineStruct(2);
                MAP.Write(proto);
            }
            // 3: LIST, id
            if(LIST != null) {
                proto.BeginInlineStruct(3);
                LIST.Write(proto);
            }
            // 4: ENUM, id
            if(ENUM != null) {
                proto.BeginInlineStruct(4);
                ENUM.Write(proto);
            }
            // 5: DECIMAL, id
            if(DECIMAL != null) {
                proto.BeginInlineStruct(5);
                DECIMAL.Write(proto);
            }
            // 6: DATE, id
            if(DATE != null) {
                proto.BeginInlineStruct(6);
                DATE.Write(proto);
            }
            // 7: TIME, id
            if(TIME != null) {
                proto.BeginInlineStruct(7);
                TIME.Write(proto);
            }
            // 8: TIMESTAMP, id
            if(TIMESTAMP != null) {
                proto.BeginInlineStruct(8);
                TIMESTAMP.Write(proto);
            }
            // 10: INTEGER, id
            if(INTEGER != null) {
                proto.BeginInlineStruct(10);
                INTEGER.Write(proto);
            }
            // 11: UNKNOWN, id
            if(UNKNOWN != null) {
                proto.BeginInlineStruct(11);
                UNKNOWN.Write(proto);
            }
            // 12: JSON, id
            if(JSON != null) {
                proto.BeginInlineStruct(12);
                JSON.Write(proto);
            }
            // 13: BSON, id
            if(BSON != null) {
                proto.BeginInlineStruct(13);
                BSON.Write(proto);
            }
            // 14: UUID, id
            if(UUID != null) {
                proto.BeginInlineStruct(14);
                UUID.Write(proto);
            }
            // 15: FLOAT16, id
            if(FLOAT16 != null) {
                proto.BeginInlineStruct(15);
                FLOAT16.Write(proto);
            }
            // 16: VARIANT, id
            if(VARIANT != null) {
                proto.BeginInlineStruct(16);
                VARIANT.Write(proto);
            }

            proto.StructEnd();
        }

        internal static LogicalType Read(ThriftCompactProtocolReader proto) {
            var r = new LogicalType();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // STRING, id
                        r.STRING = StringType.Read(proto);
                        break;
                    case 2: // MAP, id
                        r.MAP = MapType.Read(proto);
                        break;
                    case 3: // LIST, id
                        r.LIST = ListType.Read(proto);
                        break;
                    case 4: // ENUM, id
                        r.ENUM = EnumType.Read(proto);
                        break;
                    case 5: // DECIMAL, id
                        r.DECIMAL = DecimalType.Read(proto);
                        break;
                    case 6: // DATE, id
                        r.DATE = DateType.Read(proto);
                        break;
                    case 7: // TIME, id
                        r.TIME = TimeType.Read(proto);
                        break;
                    case 8: // TIMESTAMP, id
                        r.TIMESTAMP = TimestampType.Read(proto);
                        break;
                    case 10: // INTEGER, id
                        r.INTEGER = IntType.Read(proto);
                        break;
                    case 11: // UNKNOWN, id
                        r.UNKNOWN = NullType.Read(proto);
                        break;
                    case 12: // JSON, id
                        r.JSON = JsonType.Read(proto);
                        break;
                    case 13: // BSON, id
                        r.BSON = BsonType.Read(proto);
                        break;
                    case 14: // UUID, id
                        r.UUID = UUIDType.Read(proto);
                        break;
                    case 15: // FLOAT16, id
                        r.FLOAT16 = Float16Type.Read(proto);
                        break;
                    case 16: // VARIANT, id
                        r.VARIANT = VariantType.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Represents a element inside a schema definition.  - if it is a group (inner node) then type is undefined and num_children is defined  - if it is a primitive type (leaf) then type is defined and num_children is undefined the nodes are listed in depth first traversal order.
    /// </summary>
    public class SchemaElement {
        /// <summary>
        /// Data type for this field. Not set if the current element is a non-leaf node.
        /// </summary>
        public Type? Type { get; set; }

        /// <summary>
        /// If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values. Otherwise, if specified, this is the maximum bit length to store any of the values. (e.g. a low cardinality INT col could have this set to 3).  Note that this is in the schema, and therefore fixed for the entire file.
        /// </summary>
        public int? TypeLength { get; set; }

        /// <summary>
        /// Repetition of the field. The root of the schema does not have a repetition_type. All other nodes must have one.
        /// </summary>
        public FieldRepetitionType? RepetitionType { get; set; }

        /// <summary>
        /// Name of the field in the schema.
        /// </summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Nested fields.  Since thrift does not support nested fields, the nesting is flattened to a single list by a depth-first traversal. The children count is used to construct the nested relationship. This field is not set when the element is a primitive type.
        /// </summary>
        public int? NumChildren { get; set; }

        /// <summary>
        /// DEPRECATED: When the schema is the result of a conversion from another model. Used to record the original type to help with cross conversion.  This is superseded by logicalType.
        /// </summary>
        public ConvertedType? ConvertedType { get; set; }

        /// <summary>
        /// DEPRECATED: Used when this column contains decimal data. See the DECIMAL converted type for more details.  This is superseded by using the DecimalType annotation in logicalType.
        /// </summary>
        public int? Scale { get; set; }

        public int? Precision { get; set; }

        /// <summary>
        /// When the original schema supports field ids, this will save the original field id in the parquet schema.
        /// </summary>
        public int? FieldId { get; set; }

        /// <summary>
        /// The logical type of this SchemaElement  LogicalType replaces ConvertedType, but ConvertedType is still required for some logical types to ensure forward-compatibility in format v1.
        /// </summary>
        public LogicalType? LogicalType { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Type, id
            if(Type != null) {
                proto.WriteI32Field(1, (int)Type);
            }
            // 2: TypeLength, i32
            if(TypeLength != null) {
                proto.WriteI32Field(2, TypeLength.Value);
            }
            // 3: RepetitionType, id
            if(RepetitionType != null) {
                proto.WriteI32Field(3, (int)RepetitionType);
            }
            // 4: Name, string
            proto.WriteStringField(4, Name ?? string.Empty);
            // 5: NumChildren, i32
            if(NumChildren != null) {
                proto.WriteI32Field(5, NumChildren.Value);
            }
            // 6: ConvertedType, id
            if(ConvertedType != null) {
                proto.WriteI32Field(6, (int)ConvertedType);
            }
            // 7: Scale, i32
            if(Scale != null) {
                proto.WriteI32Field(7, Scale.Value);
            }
            // 8: Precision, i32
            if(Precision != null) {
                proto.WriteI32Field(8, Precision.Value);
            }
            // 9: FieldId, i32
            if(FieldId != null) {
                proto.WriteI32Field(9, FieldId.Value);
            }
            // 10: LogicalType, id
            if(LogicalType != null) {
                proto.BeginInlineStruct(10);
                LogicalType.Write(proto);
            }

            proto.StructEnd();
        }

        internal static SchemaElement Read(ThriftCompactProtocolReader proto) {
            var r = new SchemaElement();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Type, id
                        r.Type = (Type)proto.ReadI32();
                        break;
                    case 2: // TypeLength, i32
                        r.TypeLength = proto.ReadI32();
                        break;
                    case 3: // RepetitionType, id
                        r.RepetitionType = (FieldRepetitionType)proto.ReadI32();
                        break;
                    case 4: // Name, string
                        r.Name = proto.ReadString();
                        break;
                    case 5: // NumChildren, i32
                        r.NumChildren = proto.ReadI32();
                        break;
                    case 6: // ConvertedType, id
                        r.ConvertedType = (ConvertedType)proto.ReadI32();
                        break;
                    case 7: // Scale, i32
                        r.Scale = proto.ReadI32();
                        break;
                    case 8: // Precision, i32
                        r.Precision = proto.ReadI32();
                        break;
                    case 9: // FieldId, i32
                        r.FieldId = proto.ReadI32();
                        break;
                    case 10: // LogicalType, id
                        r.LogicalType = LogicalType.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Data page header.
    /// </summary>
    public class DataPageHeader {
        /// <summary>
        /// Number of values, including NULLs, in this data page.  If a OffsetIndex is present, a page must begin at a row boundary (repetition_level = 0). Otherwise, pages may begin within a row (repetition_level &gt; 0).
        /// </summary>
        public int NumValues { get; set; }

        /// <summary>
        /// Encoding used for this data page.
        /// </summary>
        public Encoding Encoding { get; set; } = new Encoding();

        /// <summary>
        /// Encoding used for definition levels.
        /// </summary>
        public Encoding DefinitionLevelEncoding { get; set; } = new Encoding();

        /// <summary>
        /// Encoding used for repetition levels.
        /// </summary>
        public Encoding RepetitionLevelEncoding { get; set; } = new Encoding();

        /// <summary>
        /// Optional statistics for the data in this page.
        /// </summary>
        public Statistics? Statistics { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: NumValues, i32
            proto.WriteI32Field(1, NumValues);
            // 2: Encoding, id
            proto.WriteI32Field(2, (int)Encoding);
            // 3: DefinitionLevelEncoding, id
            proto.WriteI32Field(3, (int)DefinitionLevelEncoding);
            // 4: RepetitionLevelEncoding, id
            proto.WriteI32Field(4, (int)RepetitionLevelEncoding);
            // 5: Statistics, id
            if(Statistics != null) {
                proto.BeginInlineStruct(5);
                Statistics.Write(proto);
            }

            proto.StructEnd();
        }

        internal static DataPageHeader Read(ThriftCompactProtocolReader proto) {
            var r = new DataPageHeader();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // NumValues, i32
                        r.NumValues = proto.ReadI32();
                        break;
                    case 2: // Encoding, id
                        r.Encoding = (Encoding)proto.ReadI32();
                        break;
                    case 3: // DefinitionLevelEncoding, id
                        r.DefinitionLevelEncoding = (Encoding)proto.ReadI32();
                        break;
                    case 4: // RepetitionLevelEncoding, id
                        r.RepetitionLevelEncoding = (Encoding)proto.ReadI32();
                        break;
                    case 5: // Statistics, id
                        r.Statistics = Statistics.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class IndexPageHeader {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static IndexPageHeader Read(ThriftCompactProtocolReader proto) {
            var r = new IndexPageHeader();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// The dictionary page must be placed at the first position of the column chunk if it is partly or completely dictionary encoded. At most one dictionary page can be placed in a column chunk.
    /// </summary>
    public class DictionaryPageHeader {
        /// <summary>
        /// Number of values in the dictionary.
        /// </summary>
        public int NumValues { get; set; }

        /// <summary>
        /// Encoding using this dictionary page.
        /// </summary>
        public Encoding Encoding { get; set; } = new Encoding();

        /// <summary>
        /// If true, the entries in the dictionary are sorted in ascending order.
        /// </summary>
        public bool? IsSorted { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: NumValues, i32
            proto.WriteI32Field(1, NumValues);
            // 2: Encoding, id
            proto.WriteI32Field(2, (int)Encoding);
            // 3: IsSorted, bool
            if(IsSorted != null) {
                proto.WriteBoolField(3, IsSorted.Value);
            }

            proto.StructEnd();
        }

        internal static DictionaryPageHeader Read(ThriftCompactProtocolReader proto) {
            var r = new DictionaryPageHeader();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // NumValues, i32
                        r.NumValues = proto.ReadI32();
                        break;
                    case 2: // Encoding, id
                        r.Encoding = (Encoding)proto.ReadI32();
                        break;
                    case 3: // IsSorted, bool
                        r.IsSorted = compactType == CompactType.BooleanTrue;
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// New page format allowing reading levels without decompressing the data Repetition and definition levels are uncompressed The remaining section containing the data is compressed if is_compressed is true.
    /// </summary>
    public class DataPageHeaderV2 {
        /// <summary>
        /// Number of values, including NULLs, in this data page.
        /// </summary>
        public int NumValues { get; set; }

        /// <summary>
        /// Number of NULL values, in this data page. Number of non-null = num_values - num_nulls which is also the number of values in the data section.
        /// </summary>
        public int NumNulls { get; set; }

        /// <summary>
        /// Number of rows in this data page. Every page must begin at a row boundary (repetition_level = 0): rows must **not** be split across page boundaries when using V2 data pages.
        /// </summary>
        public int NumRows { get; set; }

        /// <summary>
        /// Encoding used for data in this page.
        /// </summary>
        public Encoding Encoding { get; set; } = new Encoding();

        /// <summary>
        /// Length of the definition levels.
        /// </summary>
        public int DefinitionLevelsByteLength { get; set; }

        /// <summary>
        /// Length of the repetition levels.
        /// </summary>
        public int RepetitionLevelsByteLength { get; set; }

        /// <summary>
        /// Whether the values are compressed. Which means the section of the page between definition_levels_byte_length + repetition_levels_byte_length + 1 and compressed_page_size (included) is compressed with the compression_codec. If missing it is considered compressed.
        /// </summary>
        public bool? IsCompressed { get; set; }

        /// <summary>
        /// Optional statistics for the data in this page.
        /// </summary>
        public Statistics? Statistics { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: NumValues, i32
            proto.WriteI32Field(1, NumValues);
            // 2: NumNulls, i32
            proto.WriteI32Field(2, NumNulls);
            // 3: NumRows, i32
            proto.WriteI32Field(3, NumRows);
            // 4: Encoding, id
            proto.WriteI32Field(4, (int)Encoding);
            // 5: DefinitionLevelsByteLength, i32
            proto.WriteI32Field(5, DefinitionLevelsByteLength);
            // 6: RepetitionLevelsByteLength, i32
            proto.WriteI32Field(6, RepetitionLevelsByteLength);
            // 7: IsCompressed, bool
            if(IsCompressed != null) {
                proto.WriteBoolField(7, IsCompressed.Value);
            }
            // 8: Statistics, id
            if(Statistics != null) {
                proto.BeginInlineStruct(8);
                Statistics.Write(proto);
            }

            proto.StructEnd();
        }

        internal static DataPageHeaderV2 Read(ThriftCompactProtocolReader proto) {
            var r = new DataPageHeaderV2();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // NumValues, i32
                        r.NumValues = proto.ReadI32();
                        break;
                    case 2: // NumNulls, i32
                        r.NumNulls = proto.ReadI32();
                        break;
                    case 3: // NumRows, i32
                        r.NumRows = proto.ReadI32();
                        break;
                    case 4: // Encoding, id
                        r.Encoding = (Encoding)proto.ReadI32();
                        break;
                    case 5: // DefinitionLevelsByteLength, i32
                        r.DefinitionLevelsByteLength = proto.ReadI32();
                        break;
                    case 6: // RepetitionLevelsByteLength, i32
                        r.RepetitionLevelsByteLength = proto.ReadI32();
                        break;
                    case 7: // IsCompressed, bool
                        r.IsCompressed = compactType == CompactType.BooleanTrue;
                        break;
                    case 8: // Statistics, id
                        r.Statistics = Statistics.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Block-based algorithm type annotation.
    /// </summary>
    public class SplitBlockAlgorithm {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static SplitBlockAlgorithm Read(ThriftCompactProtocolReader proto) {
            var r = new SplitBlockAlgorithm();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// The algorithm used in Bloom filter.
    /// </summary>
    public class BloomFilterAlgorithm {
        /// <summary>
        /// Block-based Bloom filter.
        /// </summary>
        public SplitBlockAlgorithm? BLOCK { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: BLOCK, id
            if(BLOCK != null) {
                proto.BeginInlineStruct(1);
                BLOCK.Write(proto);
            }

            proto.StructEnd();
        }

        internal static BloomFilterAlgorithm Read(ThriftCompactProtocolReader proto) {
            var r = new BloomFilterAlgorithm();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // BLOCK, id
                        r.BLOCK = SplitBlockAlgorithm.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Hash strategy type annotation. xxHash is an extremely fast non-cryptographic hash algorithm. It uses 64 bits version of xxHash.
    /// </summary>
    public class XxHash {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static XxHash Read(ThriftCompactProtocolReader proto) {
            var r = new XxHash();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// The hash function used in Bloom filter. This function takes the hash of a column value using plain encoding.
    /// </summary>
    public class BloomFilterHash {
        /// <summary>
        /// XxHash Strategy.
        /// </summary>
        public XxHash? XXHASH { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: XXHASH, id
            if(XXHASH != null) {
                proto.BeginInlineStruct(1);
                XXHASH.Write(proto);
            }

            proto.StructEnd();
        }

        internal static BloomFilterHash Read(ThriftCompactProtocolReader proto) {
            var r = new BloomFilterHash();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // XXHASH, id
                        r.XXHASH = XxHash.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// The compression used in the Bloom filter.
    /// </summary>
    public class Uncompressed {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static Uncompressed Read(ThriftCompactProtocolReader proto) {
            var r = new Uncompressed();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class BloomFilterCompression {
        public Uncompressed? UNCOMPRESSED { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: UNCOMPRESSED, id
            if(UNCOMPRESSED != null) {
                proto.BeginInlineStruct(1);
                UNCOMPRESSED.Write(proto);
            }

            proto.StructEnd();
        }

        internal static BloomFilterCompression Read(ThriftCompactProtocolReader proto) {
            var r = new BloomFilterCompression();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // UNCOMPRESSED, id
                        r.UNCOMPRESSED = Uncompressed.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Bloom filter header is stored at beginning of Bloom filter data of each column and followed by its bitset.
    /// </summary>
    public class BloomFilterHeader {
        /// <summary>
        /// The size of bitset in bytes.
        /// </summary>
        public int NumBytes { get; set; }

        /// <summary>
        /// The algorithm for setting bits.
        /// </summary>
        public BloomFilterAlgorithm Algorithm { get; set; } = new BloomFilterAlgorithm();

        /// <summary>
        /// The hash function used for Bloom filter.
        /// </summary>
        public BloomFilterHash Hash { get; set; } = new BloomFilterHash();

        /// <summary>
        /// The compression used in the Bloom filter.
        /// </summary>
        public BloomFilterCompression Compression { get; set; } = new BloomFilterCompression();


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: NumBytes, i32
            proto.WriteI32Field(1, NumBytes);
            // 2: Algorithm, id
            proto.BeginInlineStruct(2);
            Algorithm.Write(proto);
            // 3: Hash, id
            proto.BeginInlineStruct(3);
            Hash.Write(proto);
            // 4: Compression, id
            proto.BeginInlineStruct(4);
            Compression.Write(proto);

            proto.StructEnd();
        }

        internal static BloomFilterHeader Read(ThriftCompactProtocolReader proto) {
            var r = new BloomFilterHeader();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // NumBytes, i32
                        r.NumBytes = proto.ReadI32();
                        break;
                    case 2: // Algorithm, id
                        r.Algorithm = BloomFilterAlgorithm.Read(proto);
                        break;
                    case 3: // Hash, id
                        r.Hash = BloomFilterHash.Read(proto);
                        break;
                    case 4: // Compression, id
                        r.Compression = BloomFilterCompression.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class PageHeader {
        /// <summary>
        /// The type of the page: indicates which of the *_header fields is set.
        /// </summary>
        public PageType Type { get; set; } = new PageType();

        /// <summary>
        /// Uncompressed page size in bytes (not including this header).
        /// </summary>
        public int UncompressedPageSize { get; set; }

        /// <summary>
        /// Compressed (and potentially encrypted) page size in bytes, not including this header.
        /// </summary>
        public int CompressedPageSize { get; set; }

        /// <summary>
        /// The 32-bit CRC checksum for the page, to be be calculated as follows:  - The standard CRC32 algorithm is used (with polynomial 0x04C11DB7,   the same as in e.g. GZip). - All page types can have a CRC (v1 and v2 data pages, dictionary pages,   etc.). - The CRC is computed on the serialization binary representation of the page   (as written to disk), excluding the page header. For example, for v1   data pages, the CRC is computed on the concatenation of repetition levels,   definition levels and column values (optionally compressed, optionally   encrypted). - The CRC computation therefore takes place after any compression   and encryption steps, if any.  If enabled, this allows for disabling checksumming in HDFS if only a few pages need to be read.
        /// </summary>
        public int? Crc { get; set; }

        public DataPageHeader? DataPageHeader { get; set; }

        public IndexPageHeader? IndexPageHeader { get; set; }

        public DictionaryPageHeader? DictionaryPageHeader { get; set; }

        public DataPageHeaderV2? DataPageHeaderV2 { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Type, id
            proto.WriteI32Field(1, (int)Type);
            // 2: UncompressedPageSize, i32
            proto.WriteI32Field(2, UncompressedPageSize);
            // 3: CompressedPageSize, i32
            proto.WriteI32Field(3, CompressedPageSize);
            // 4: Crc, i32
            if(Crc != null) {
                proto.WriteI32Field(4, Crc.Value);
            }
            // 5: DataPageHeader, id
            if(DataPageHeader != null) {
                proto.BeginInlineStruct(5);
                DataPageHeader.Write(proto);
            }
            // 6: IndexPageHeader, id
            if(IndexPageHeader != null) {
                proto.BeginInlineStruct(6);
                IndexPageHeader.Write(proto);
            }
            // 7: DictionaryPageHeader, id
            if(DictionaryPageHeader != null) {
                proto.BeginInlineStruct(7);
                DictionaryPageHeader.Write(proto);
            }
            // 8: DataPageHeaderV2, id
            if(DataPageHeaderV2 != null) {
                proto.BeginInlineStruct(8);
                DataPageHeaderV2.Write(proto);
            }

            proto.StructEnd();
        }

        internal static PageHeader Read(ThriftCompactProtocolReader proto) {
            var r = new PageHeader();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Type, id
                        r.Type = (PageType)proto.ReadI32();
                        break;
                    case 2: // UncompressedPageSize, i32
                        r.UncompressedPageSize = proto.ReadI32();
                        break;
                    case 3: // CompressedPageSize, i32
                        r.CompressedPageSize = proto.ReadI32();
                        break;
                    case 4: // Crc, i32
                        r.Crc = proto.ReadI32();
                        break;
                    case 5: // DataPageHeader, id
                        r.DataPageHeader = DataPageHeader.Read(proto);
                        break;
                    case 6: // IndexPageHeader, id
                        r.IndexPageHeader = IndexPageHeader.Read(proto);
                        break;
                    case 7: // DictionaryPageHeader, id
                        r.DictionaryPageHeader = DictionaryPageHeader.Read(proto);
                        break;
                    case 8: // DataPageHeaderV2, id
                        r.DataPageHeaderV2 = DataPageHeaderV2.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Wrapper struct to store key values.
    /// </summary>
    public class KeyValue {
        public string Key { get; set; } = string.Empty;

        public string? Value { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Key, string
            proto.WriteStringField(1, Key ?? string.Empty);
            // 2: Value, string
            if(Value != null) {
                proto.WriteStringField(2, Value);
            }

            proto.StructEnd();
        }

        internal static KeyValue Read(ThriftCompactProtocolReader proto) {
            var r = new KeyValue();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Key, string
                        r.Key = proto.ReadString();
                        break;
                    case 2: // Value, string
                        r.Value = proto.ReadString();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Sort order within a RowGroup of a leaf column.
    /// </summary>
    public class SortingColumn {
        /// <summary>
        /// The ordinal position of the column (in this row group).
        /// </summary>
        public int ColumnIdx { get; set; }

        /// <summary>
        /// If true, indicates this column is sorted in descending order.
        /// </summary>
        public bool Descending { get; set; }

        /// <summary>
        /// If true, nulls will come before non-null values, otherwise, nulls go at the end.
        /// </summary>
        public bool NullsFirst { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: ColumnIdx, i32
            proto.WriteI32Field(1, ColumnIdx);
            // 2: Descending, bool
            proto.WriteBoolField(2, Descending);
            // 3: NullsFirst, bool
            proto.WriteBoolField(3, NullsFirst);

            proto.StructEnd();
        }

        internal static SortingColumn Read(ThriftCompactProtocolReader proto) {
            var r = new SortingColumn();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // ColumnIdx, i32
                        r.ColumnIdx = proto.ReadI32();
                        break;
                    case 2: // Descending, bool
                        r.Descending = compactType == CompactType.BooleanTrue;
                        break;
                    case 3: // NullsFirst, bool
                        r.NullsFirst = compactType == CompactType.BooleanTrue;
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Statistics of a given page type and encoding.
    /// </summary>
    public class PageEncodingStats {
        /// <summary>
        /// The page type (data/dic/...).
        /// </summary>
        public PageType PageType { get; set; } = new PageType();

        /// <summary>
        /// Encoding of the page.
        /// </summary>
        public Encoding Encoding { get; set; } = new Encoding();

        /// <summary>
        /// Number of pages of this type with this encoding.
        /// </summary>
        public int Count { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: PageType, id
            proto.WriteI32Field(1, (int)PageType);
            // 2: Encoding, id
            proto.WriteI32Field(2, (int)Encoding);
            // 3: Count, i32
            proto.WriteI32Field(3, Count);

            proto.StructEnd();
        }

        internal static PageEncodingStats Read(ThriftCompactProtocolReader proto) {
            var r = new PageEncodingStats();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // PageType, id
                        r.PageType = (PageType)proto.ReadI32();
                        break;
                    case 2: // Encoding, id
                        r.Encoding = (Encoding)proto.ReadI32();
                        break;
                    case 3: // Count, i32
                        r.Count = proto.ReadI32();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Description for column metadata.
    /// </summary>
    public class ColumnMetaData {
        /// <summary>
        /// Type of this column.
        /// </summary>
        public Type Type { get; set; } = new Type();

        /// <summary>
        /// Set of all encodings used for this column. The purpose is to validate whether we can decode those pages.
        /// </summary>
        public List<Encoding> Encodings { get; set; } = new List<Encoding>();

        /// <summary>
        /// Path in schema.
        /// </summary>
        public List<string> PathInSchema { get; set; } = new List<string>();

        /// <summary>
        /// Compression codec.
        /// </summary>
        public CompressionCodec Codec { get; set; } = new CompressionCodec();

        /// <summary>
        /// Number of values in this column.
        /// </summary>
        public long NumValues { get; set; }

        /// <summary>
        /// Total byte size of all uncompressed pages in this column chunk (including the headers).
        /// </summary>
        public long TotalUncompressedSize { get; set; }

        /// <summary>
        /// Total byte size of all compressed, and potentially encrypted, pages in this column chunk (including the headers).
        /// </summary>
        public long TotalCompressedSize { get; set; }

        /// <summary>
        /// Optional key/value metadata.
        /// </summary>
        public List<KeyValue>? KeyValueMetadata { get; set; }

        /// <summary>
        /// Byte offset from beginning of file to first data page.
        /// </summary>
        public long DataPageOffset { get; set; }

        /// <summary>
        /// Byte offset from beginning of file to root index page.
        /// </summary>
        public long? IndexPageOffset { get; set; }

        /// <summary>
        /// Byte offset from the beginning of file to first (only) dictionary page.
        /// </summary>
        public long? DictionaryPageOffset { get; set; }

        /// <summary>
        /// Optional statistics for this column chunk.
        /// </summary>
        public Statistics? Statistics { get; set; }

        /// <summary>
        /// Set of all encodings used for pages in this column chunk. This information can be used to determine if all data pages are dictionary encoded for example.
        /// </summary>
        public List<PageEncodingStats>? EncodingStats { get; set; }

        /// <summary>
        /// Byte offset from beginning of file to Bloom filter data.
        /// </summary>
        public long? BloomFilterOffset { get; set; }

        /// <summary>
        /// Size of Bloom filter data including the serialized header, in bytes. Added in 2.10 so readers may not read this field from old files and it can be obtained after the BloomFilterHeader has been deserialized. Writers should write this field so readers can read the bloom filter in a single I/O.
        /// </summary>
        public int? BloomFilterLength { get; set; }

        /// <summary>
        /// Optional statistics to help estimate total memory when converted to in-memory representations. The histograms contained in these statistics can also be useful in some cases for more fine-grained nullability/list length filter pushdown.
        /// </summary>
        public SizeStatistics? SizeStatistics { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Type, id
            proto.WriteI32Field(1, (int)Type);
            // 2: Encodings, list
            proto.WriteListBegin(2, 5, Encodings.Count);
            foreach(Encoding element in Encodings) {
                proto.WriteI32Value((int)element);
            }
            // 3: PathInSchema, list
            proto.WriteListBegin(3, 8, PathInSchema.Count);
            foreach(string element in PathInSchema) {
                proto.WriteStringValue(element);
            }
            // 4: Codec, id
            proto.WriteI32Field(4, (int)Codec);
            // 5: NumValues, i64
            proto.WriteI64Field(5, NumValues);
            // 6: TotalUncompressedSize, i64
            proto.WriteI64Field(6, TotalUncompressedSize);
            // 7: TotalCompressedSize, i64
            proto.WriteI64Field(7, TotalCompressedSize);
            // 8: KeyValueMetadata, list
            if(KeyValueMetadata != null) {
                proto.WriteListBegin(8, 12, KeyValueMetadata.Count);
                foreach(KeyValue element in KeyValueMetadata) {
                    element.Write(proto);
                }
            }
            // 9: DataPageOffset, i64
            proto.WriteI64Field(9, DataPageOffset);
            // 10: IndexPageOffset, i64
            if(IndexPageOffset != null) {
                proto.WriteI64Field(10, IndexPageOffset.Value);
            }
            // 11: DictionaryPageOffset, i64
            if(DictionaryPageOffset != null) {
                proto.WriteI64Field(11, DictionaryPageOffset.Value);
            }
            // 12: Statistics, id
            if(Statistics != null) {
                proto.BeginInlineStruct(12);
                Statistics.Write(proto);
            }
            // 13: EncodingStats, list
            if(EncodingStats != null) {
                proto.WriteListBegin(13, 12, EncodingStats.Count);
                foreach(PageEncodingStats element in EncodingStats) {
                    element.Write(proto);
                }
            }
            // 14: BloomFilterOffset, i64
            if(BloomFilterOffset != null) {
                proto.WriteI64Field(14, BloomFilterOffset.Value);
            }
            // 15: BloomFilterLength, i32
            if(BloomFilterLength != null) {
                proto.WriteI32Field(15, BloomFilterLength.Value);
            }
            // 16: SizeStatistics, id
            if(SizeStatistics != null) {
                proto.BeginInlineStruct(16);
                SizeStatistics.Write(proto);
            }

            proto.StructEnd();
        }

        internal static ColumnMetaData Read(ThriftCompactProtocolReader proto) {
            var r = new ColumnMetaData();
            proto.StructBegin();
            int elementCount = 0;
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Type, id
                        r.Type = (Type)proto.ReadI32();
                        break;
                    case 2: // Encodings, list
                        elementCount = proto.ReadListHeader(out _);
                        r.Encodings = new List<Encoding>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.Encodings.Add((Encoding)proto.ReadI32()); }
                        break;
                    case 3: // PathInSchema, list
                        elementCount = proto.ReadListHeader(out _);
                        r.PathInSchema = new List<string>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.PathInSchema.Add(proto.ReadString()); }
                        break;
                    case 4: // Codec, id
                        r.Codec = (CompressionCodec)proto.ReadI32();
                        break;
                    case 5: // NumValues, i64
                        r.NumValues = proto.ReadI64();
                        break;
                    case 6: // TotalUncompressedSize, i64
                        r.TotalUncompressedSize = proto.ReadI64();
                        break;
                    case 7: // TotalCompressedSize, i64
                        r.TotalCompressedSize = proto.ReadI64();
                        break;
                    case 8: // KeyValueMetadata, list
                        elementCount = proto.ReadListHeader(out _);
                        r.KeyValueMetadata = new List<KeyValue>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.KeyValueMetadata.Add(KeyValue.Read(proto)); }
                        break;
                    case 9: // DataPageOffset, i64
                        r.DataPageOffset = proto.ReadI64();
                        break;
                    case 10: // IndexPageOffset, i64
                        r.IndexPageOffset = proto.ReadI64();
                        break;
                    case 11: // DictionaryPageOffset, i64
                        r.DictionaryPageOffset = proto.ReadI64();
                        break;
                    case 12: // Statistics, id
                        r.Statistics = Statistics.Read(proto);
                        break;
                    case 13: // EncodingStats, list
                        elementCount = proto.ReadListHeader(out _);
                        r.EncodingStats = new List<PageEncodingStats>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.EncodingStats.Add(PageEncodingStats.Read(proto)); }
                        break;
                    case 14: // BloomFilterOffset, i64
                        r.BloomFilterOffset = proto.ReadI64();
                        break;
                    case 15: // BloomFilterLength, i32
                        r.BloomFilterLength = proto.ReadI32();
                        break;
                    case 16: // SizeStatistics, id
                        r.SizeStatistics = SizeStatistics.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class EncryptionWithFooterKey {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static EncryptionWithFooterKey Read(ThriftCompactProtocolReader proto) {
            var r = new EncryptionWithFooterKey();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class EncryptionWithColumnKey {
        /// <summary>
        /// Column path in schema.
        /// </summary>
        public List<string> PathInSchema { get; set; } = new List<string>();

        /// <summary>
        /// Retrieval metadata of column encryption key.
        /// </summary>
        public byte[]? KeyMetadata { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: PathInSchema, list
            proto.WriteListBegin(1, 8, PathInSchema.Count);
            foreach(string element in PathInSchema) {
                proto.WriteStringValue(element);
            }
            // 2: KeyMetadata, binary
            if(KeyMetadata != null) {
                proto.WriteBinaryField(2, KeyMetadata);
            }

            proto.StructEnd();
        }

        internal static EncryptionWithColumnKey Read(ThriftCompactProtocolReader proto) {
            var r = new EncryptionWithColumnKey();
            proto.StructBegin();
            int elementCount = 0;
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // PathInSchema, list
                        elementCount = proto.ReadListHeader(out _);
                        r.PathInSchema = new List<string>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.PathInSchema.Add(proto.ReadString()); }
                        break;
                    case 2: // KeyMetadata, binary
                        r.KeyMetadata = proto.ReadBinary();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class ColumnCryptoMetaData {
        public EncryptionWithFooterKey? ENCRYPTIONWITHFOOTERKEY { get; set; }

        public EncryptionWithColumnKey? ENCRYPTIONWITHCOLUMNKEY { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: ENCRYPTIONWITHFOOTERKEY, id
            if(ENCRYPTIONWITHFOOTERKEY != null) {
                proto.BeginInlineStruct(1);
                ENCRYPTIONWITHFOOTERKEY.Write(proto);
            }
            // 2: ENCRYPTIONWITHCOLUMNKEY, id
            if(ENCRYPTIONWITHCOLUMNKEY != null) {
                proto.BeginInlineStruct(2);
                ENCRYPTIONWITHCOLUMNKEY.Write(proto);
            }

            proto.StructEnd();
        }

        internal static ColumnCryptoMetaData Read(ThriftCompactProtocolReader proto) {
            var r = new ColumnCryptoMetaData();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // ENCRYPTIONWITHFOOTERKEY, id
                        r.ENCRYPTIONWITHFOOTERKEY = EncryptionWithFooterKey.Read(proto);
                        break;
                    case 2: // ENCRYPTIONWITHCOLUMNKEY, id
                        r.ENCRYPTIONWITHCOLUMNKEY = EncryptionWithColumnKey.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class ColumnChunk {
        /// <summary>
        /// File where column data is stored.  If not set, assumed to be same file as metadata.  This path is relative to the current file.
        /// </summary>
        public string? FilePath { get; set; }

        /// <summary>
        /// Deprecated: Byte offset in file_path to the ColumnMetaData  Past use of this field has been inconsistent, with some implementations using it to point to the ColumnMetaData and some using it to point to the first page in the column chunk. In many cases, the ColumnMetaData at this location is wrong. This field is now deprecated and should not be used. Writers should set this field to 0 if no ColumnMetaData has been written outside the footer.
        /// </summary>
        public long FileOffset { get; set; }

        /// <summary>
        /// Column metadata for this chunk. Some writers may also replicate this at the location pointed to by file_path/file_offset. Note: while marked as optional, this field is in fact required by most major Parquet implementations. As such, writers MUST populate this field.
        /// </summary>
        public ColumnMetaData? MetaData { get; set; }

        /// <summary>
        /// File offset of ColumnChunk&#39;s OffsetIndex.
        /// </summary>
        public long? OffsetIndexOffset { get; set; }

        /// <summary>
        /// Size of ColumnChunk&#39;s OffsetIndex, in bytes.
        /// </summary>
        public int? OffsetIndexLength { get; set; }

        /// <summary>
        /// File offset of ColumnChunk&#39;s ColumnIndex.
        /// </summary>
        public long? ColumnIndexOffset { get; set; }

        /// <summary>
        /// Size of ColumnChunk&#39;s ColumnIndex, in bytes.
        /// </summary>
        public int? ColumnIndexLength { get; set; }

        /// <summary>
        /// Crypto metadata of encrypted columns.
        /// </summary>
        public ColumnCryptoMetaData? CryptoMetadata { get; set; }

        /// <summary>
        /// Encrypted column metadata for this chunk.
        /// </summary>
        public byte[]? EncryptedColumnMetadata { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: FilePath, string
            if(FilePath != null) {
                proto.WriteStringField(1, FilePath);
            }
            // 2: FileOffset, i64
            proto.WriteI64Field(2, FileOffset);
            // 3: MetaData, id
            if(MetaData != null) {
                proto.BeginInlineStruct(3);
                MetaData.Write(proto);
            }
            // 4: OffsetIndexOffset, i64
            if(OffsetIndexOffset != null) {
                proto.WriteI64Field(4, OffsetIndexOffset.Value);
            }
            // 5: OffsetIndexLength, i32
            if(OffsetIndexLength != null) {
                proto.WriteI32Field(5, OffsetIndexLength.Value);
            }
            // 6: ColumnIndexOffset, i64
            if(ColumnIndexOffset != null) {
                proto.WriteI64Field(6, ColumnIndexOffset.Value);
            }
            // 7: ColumnIndexLength, i32
            if(ColumnIndexLength != null) {
                proto.WriteI32Field(7, ColumnIndexLength.Value);
            }
            // 8: CryptoMetadata, id
            if(CryptoMetadata != null) {
                proto.BeginInlineStruct(8);
                CryptoMetadata.Write(proto);
            }
            // 9: EncryptedColumnMetadata, binary
            if(EncryptedColumnMetadata != null) {
                proto.WriteBinaryField(9, EncryptedColumnMetadata);
            }

            proto.StructEnd();
        }

        internal static ColumnChunk Read(ThriftCompactProtocolReader proto) {
            var r = new ColumnChunk();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // FilePath, string
                        r.FilePath = proto.ReadString();
                        break;
                    case 2: // FileOffset, i64
                        r.FileOffset = proto.ReadI64();
                        break;
                    case 3: // MetaData, id
                        r.MetaData = ColumnMetaData.Read(proto);
                        break;
                    case 4: // OffsetIndexOffset, i64
                        r.OffsetIndexOffset = proto.ReadI64();
                        break;
                    case 5: // OffsetIndexLength, i32
                        r.OffsetIndexLength = proto.ReadI32();
                        break;
                    case 6: // ColumnIndexOffset, i64
                        r.ColumnIndexOffset = proto.ReadI64();
                        break;
                    case 7: // ColumnIndexLength, i32
                        r.ColumnIndexLength = proto.ReadI32();
                        break;
                    case 8: // CryptoMetadata, id
                        r.CryptoMetadata = ColumnCryptoMetaData.Read(proto);
                        break;
                    case 9: // EncryptedColumnMetadata, binary
                        r.EncryptedColumnMetadata = proto.ReadBinary();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class RowGroup {
        /// <summary>
        /// Metadata for each column chunk in this row group. This list must have the same order as the SchemaElement list in FileMetaData.
        /// </summary>
        public List<ColumnChunk> Columns { get; set; } = new List<ColumnChunk>();

        /// <summary>
        /// Total byte size of all the uncompressed column data in this row group.
        /// </summary>
        public long TotalByteSize { get; set; }

        /// <summary>
        /// Number of rows in this row group.
        /// </summary>
        public long NumRows { get; set; }

        /// <summary>
        /// If set, specifies a sort ordering of the rows in this RowGroup. The sorting columns can be a subset of all the columns.
        /// </summary>
        public List<SortingColumn>? SortingColumns { get; set; }

        /// <summary>
        /// Byte offset from beginning of file to first page (data or dictionary) in this row group.
        /// </summary>
        public long? FileOffset { get; set; }

        /// <summary>
        /// Total byte size of all compressed (and potentially encrypted) column data in this row group.
        /// </summary>
        public long? TotalCompressedSize { get; set; }

        /// <summary>
        /// Row group ordinal in the file.
        /// </summary>
        public short? Ordinal { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Columns, list
            proto.WriteListBegin(1, 12, Columns.Count);
            foreach(ColumnChunk element in Columns) {
                element.Write(proto);
            }
            // 2: TotalByteSize, i64
            proto.WriteI64Field(2, TotalByteSize);
            // 3: NumRows, i64
            proto.WriteI64Field(3, NumRows);
            // 4: SortingColumns, list
            if(SortingColumns != null) {
                proto.WriteListBegin(4, 12, SortingColumns.Count);
                foreach(SortingColumn element in SortingColumns) {
                    element.Write(proto);
                }
            }
            // 5: FileOffset, i64
            if(FileOffset != null) {
                proto.WriteI64Field(5, FileOffset.Value);
            }
            // 6: TotalCompressedSize, i64
            if(TotalCompressedSize != null) {
                proto.WriteI64Field(6, TotalCompressedSize.Value);
            }
            // 7: Ordinal, i16
            if(Ordinal != null) {
                proto.WriteI16Field(7, Ordinal.Value);
            }

            proto.StructEnd();
        }

        internal static RowGroup Read(ThriftCompactProtocolReader proto) {
            var r = new RowGroup();
            proto.StructBegin();
            int elementCount = 0;
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Columns, list
                        elementCount = proto.ReadListHeader(out _);
                        r.Columns = new List<ColumnChunk>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.Columns.Add(ColumnChunk.Read(proto)); }
                        break;
                    case 2: // TotalByteSize, i64
                        r.TotalByteSize = proto.ReadI64();
                        break;
                    case 3: // NumRows, i64
                        r.NumRows = proto.ReadI64();
                        break;
                    case 4: // SortingColumns, list
                        elementCount = proto.ReadListHeader(out _);
                        r.SortingColumns = new List<SortingColumn>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.SortingColumns.Add(SortingColumn.Read(proto)); }
                        break;
                    case 5: // FileOffset, i64
                        r.FileOffset = proto.ReadI64();
                        break;
                    case 6: // TotalCompressedSize, i64
                        r.TotalCompressedSize = proto.ReadI64();
                        break;
                    case 7: // Ordinal, i16
                        r.Ordinal = proto.ReadI16();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Empty struct to signal the order defined by the physical or logical type.
    /// </summary>
    public class TypeDefinedOrder {

        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.WriteEmptyStruct();
        }

        internal static TypeDefinedOrder Read(ThriftCompactProtocolReader proto) {
            var r = new TypeDefinedOrder();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Union to specify the order used for the min_value and max_value fields for a column. This union takes the role of an enhanced enum that allows rich elements (which will be needed for a collation-based ordering in the future).  Possible values are: * TypeDefinedOrder - the column uses the order defined by its logical or                      physical type (if there is no logical type).  If the reader does not support the value of this union, min and max stats for this column should be ignored.
    /// </summary>
    public class ColumnOrder {
        /// <summary>
        /// The sort orders for logical types are:   UTF8 - unsigned byte-wise comparison   INT8 - signed comparison   INT16 - signed comparison   INT32 - signed comparison   INT64 - signed comparison   UINT8 - unsigned comparison   UINT16 - unsigned comparison   UINT32 - unsigned comparison   UINT64 - unsigned comparison   DECIMAL - signed comparison of the represented value   DATE - signed comparison   TIME_MILLIS - signed comparison   TIME_MICROS - signed comparison   TIMESTAMP_MILLIS - signed comparison   TIMESTAMP_MICROS - signed comparison   INTERVAL - undefined   JSON - unsigned byte-wise comparison   BSON - unsigned byte-wise comparison   ENUM - unsigned byte-wise comparison   LIST - undefined   MAP - undefined   VARIANT - undefined  In the absence of logical types, the sort order is determined by the physical type:   BOOLEAN - false, true   INT32 - signed comparison   INT64 - signed comparison   INT96 (only used for legacy timestamps) - undefined   FLOAT - signed comparison of the represented value (*)   DOUBLE - signed comparison of the represented value (*)   BYTE_ARRAY - unsigned byte-wise comparison   FIXED_LEN_BYTE_ARRAY - unsigned byte-wise comparison  (*) Because the sorting order is not specified properly for floating     point values (relations vs. total ordering) the following     compatibility rules should be applied when reading statistics:     - If the min is a NaN, it should be ignored.     - If the max is a NaN, it should be ignored.     - If the min is +0, the row group may contain -0 values as well.     - If the max is -0, the row group may contain +0 values as well.     - When looking for NaN values, min and max should be ignored.      When writing statistics the following rules should be followed:     - NaNs should not be written to min or max statistics fields.     - If the computed max value is zero (whether negative or positive),       `+0.0` should be written into the max statistics field.     - If the computed min value is zero (whether negative or positive),       `-0.0` should be written into the min statistics field.
        /// </summary>
        public TypeDefinedOrder? TYPEORDER { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: TYPEORDER, id
            if(TYPEORDER != null) {
                proto.BeginInlineStruct(1);
                TYPEORDER.Write(proto);
            }

            proto.StructEnd();
        }

        internal static ColumnOrder Read(ThriftCompactProtocolReader proto) {
            var r = new ColumnOrder();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // TYPEORDER, id
                        r.TYPEORDER = TypeDefinedOrder.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class PageLocation {
        /// <summary>
        /// Offset of the page in the file.
        /// </summary>
        public long Offset { get; set; }

        /// <summary>
        /// Size of the page, including header. Sum of compressed_page_size and header length.
        /// </summary>
        public int CompressedPageSize { get; set; }

        /// <summary>
        /// Index within the RowGroup of the first row of the page. When an OffsetIndex is present, pages must begin on row boundaries (repetition_level = 0).
        /// </summary>
        public long FirstRowIndex { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Offset, i64
            proto.WriteI64Field(1, Offset);
            // 2: CompressedPageSize, i32
            proto.WriteI32Field(2, CompressedPageSize);
            // 3: FirstRowIndex, i64
            proto.WriteI64Field(3, FirstRowIndex);

            proto.StructEnd();
        }

        internal static PageLocation Read(ThriftCompactProtocolReader proto) {
            var r = new PageLocation();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Offset, i64
                        r.Offset = proto.ReadI64();
                        break;
                    case 2: // CompressedPageSize, i32
                        r.CompressedPageSize = proto.ReadI32();
                        break;
                    case 3: // FirstRowIndex, i64
                        r.FirstRowIndex = proto.ReadI64();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Optional offsets for each data page in a ColumnChunk.  Forms part of the page index, along with ColumnIndex.  OffsetIndex may be present even if ColumnIndex is not.
    /// </summary>
    public class OffsetIndex {
        /// <summary>
        /// PageLocations, ordered by increasing PageLocation.offset. It is required that page_locations[i].first_row_index &lt; page_locations[i+1].first_row_index.
        /// </summary>
        public List<PageLocation> PageLocations { get; set; } = new List<PageLocation>();

        /// <summary>
        /// Unencoded/uncompressed size for BYTE_ARRAY types.  See documention for unencoded_byte_array_data_bytes in SizeStatistics for more details on this field.
        /// </summary>
        public List<long>? UnencodedByteArrayDataBytes { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: PageLocations, list
            proto.WriteListBegin(1, 12, PageLocations.Count);
            foreach(PageLocation element in PageLocations) {
                element.Write(proto);
            }
            // 2: UnencodedByteArrayDataBytes, list
            if(UnencodedByteArrayDataBytes != null) {
                proto.WriteListBegin(2, 6, UnencodedByteArrayDataBytes.Count);
                foreach(long element in UnencodedByteArrayDataBytes) {
                    proto.WriteI64Value(element);
                }
            }

            proto.StructEnd();
        }

        internal static OffsetIndex Read(ThriftCompactProtocolReader proto) {
            var r = new OffsetIndex();
            proto.StructBegin();
            int elementCount = 0;
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // PageLocations, list
                        elementCount = proto.ReadListHeader(out _);
                        r.PageLocations = new List<PageLocation>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.PageLocations.Add(PageLocation.Read(proto)); }
                        break;
                    case 2: // UnencodedByteArrayDataBytes, list
                        elementCount = proto.ReadListHeader(out _);
                        r.UnencodedByteArrayDataBytes = new List<long>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.UnencodedByteArrayDataBytes.Add(proto.ReadI64()); }
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Optional statistics for each data page in a ColumnChunk.  Forms part the page index, along with OffsetIndex.  If this structure is present, OffsetIndex must also be present.  For each field in this structure, &lt;field&gt;[i] refers to the page at OffsetIndex.page_locations[i].
    /// </summary>
    public class ColumnIndex {
        /// <summary>
        /// A list of Boolean values to determine the validity of the corresponding min and max values. If true, a page contains only null values, and writers have to set the corresponding entries in min_values and max_values to byte[0], so that all lists have the same length. If false, the corresponding entries in min_values and max_values must be valid.
        /// </summary>
        public List<bool> NullPages { get; set; } = new List<bool>();

        /// <summary>
        /// Two lists containing lower and upper bounds for the values of each page determined by the ColumnOrder of the column. These may be the actual minimum and maximum values found on a page, but can also be (more compact) values that do not exist on a page. For example, instead of storing &quot;&quot;Blart Versenwald III&quot;, a writer may set min_values[i]=&quot;B&quot;, max_values[i]=&quot;C&quot;. Such more compact values must still be valid values within the column&#39;s logical type. Readers must make sure that list entries are populated before using them by inspecting null_pages.
        /// </summary>
        public List<byte[]> MinValues { get; set; } = new List<byte[]>();

        public List<byte[]> MaxValues { get; set; } = new List<byte[]>();

        /// <summary>
        /// Stores whether both min_values and max_values are ordered and if so, in which direction. This allows readers to perform binary searches in both lists. Readers cannot assume that max_values[i] &lt;= min_values[i+1], even if the lists are ordered.
        /// </summary>
        public BoundaryOrder BoundaryOrder { get; set; } = new BoundaryOrder();

        /// <summary>
        /// A list containing the number of null values for each page  Writers SHOULD always write this field even if no null values are present or the column is not nullable. Readers MUST distinguish between null_counts not being present and null_count being 0. If null_counts are not present, readers MUST NOT assume all null counts are 0.
        /// </summary>
        public List<long>? NullCounts { get; set; }

        /// <summary>
        /// Contains repetition level histograms for each page concatenated together.  The repetition_level_histogram field on SizeStatistics contains more details.  When present the length should always be (number of pages * (max_repetition_level + 1)) elements.  Element 0 is the first element of the histogram for the first page. Element (max_repetition_level + 1) is the first element of the histogram for the second page.
        /// </summary>
        public List<long>? RepetitionLevelHistograms { get; set; }

        /// <summary>
        /// Same as repetition_level_histograms except for definitions levels.
        /// </summary>
        public List<long>? DefinitionLevelHistograms { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: NullPages, list
            proto.WriteListBegin(1, 1, NullPages.Count);
            foreach(bool element in NullPages) {
                proto.WriteBoolValue(element);
            }
            // 2: MinValues, list
            proto.WriteListBegin(2, 8, MinValues.Count);
            foreach(byte[] element in MinValues) {
                proto.WriteBinaryValue(element);
            }
            // 3: MaxValues, list
            proto.WriteListBegin(3, 8, MaxValues.Count);
            foreach(byte[] element in MaxValues) {
                proto.WriteBinaryValue(element);
            }
            // 4: BoundaryOrder, id
            proto.WriteI32Field(4, (int)BoundaryOrder);
            // 5: NullCounts, list
            if(NullCounts != null) {
                proto.WriteListBegin(5, 6, NullCounts.Count);
                foreach(long element in NullCounts) {
                    proto.WriteI64Value(element);
                }
            }
            // 6: RepetitionLevelHistograms, list
            if(RepetitionLevelHistograms != null) {
                proto.WriteListBegin(6, 6, RepetitionLevelHistograms.Count);
                foreach(long element in RepetitionLevelHistograms) {
                    proto.WriteI64Value(element);
                }
            }
            // 7: DefinitionLevelHistograms, list
            if(DefinitionLevelHistograms != null) {
                proto.WriteListBegin(7, 6, DefinitionLevelHistograms.Count);
                foreach(long element in DefinitionLevelHistograms) {
                    proto.WriteI64Value(element);
                }
            }

            proto.StructEnd();
        }

        internal static ColumnIndex Read(ThriftCompactProtocolReader proto) {
            var r = new ColumnIndex();
            proto.StructBegin();
            int elementCount = 0;
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // NullPages, list
                        elementCount = proto.ReadListHeader(out _);
                        r.NullPages = new List<bool>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.NullPages.Add(proto.ReadBool()); }
                        break;
                    case 2: // MinValues, list
                        elementCount = proto.ReadListHeader(out _);
                        r.MinValues = new List<byte[]>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.MinValues.Add(proto.ReadBinary()); }
                        break;
                    case 3: // MaxValues, list
                        elementCount = proto.ReadListHeader(out _);
                        r.MaxValues = new List<byte[]>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.MaxValues.Add(proto.ReadBinary()); }
                        break;
                    case 4: // BoundaryOrder, id
                        r.BoundaryOrder = (BoundaryOrder)proto.ReadI32();
                        break;
                    case 5: // NullCounts, list
                        elementCount = proto.ReadListHeader(out _);
                        r.NullCounts = new List<long>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.NullCounts.Add(proto.ReadI64()); }
                        break;
                    case 6: // RepetitionLevelHistograms, list
                        elementCount = proto.ReadListHeader(out _);
                        r.RepetitionLevelHistograms = new List<long>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.RepetitionLevelHistograms.Add(proto.ReadI64()); }
                        break;
                    case 7: // DefinitionLevelHistograms, list
                        elementCount = proto.ReadListHeader(out _);
                        r.DefinitionLevelHistograms = new List<long>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.DefinitionLevelHistograms.Add(proto.ReadI64()); }
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class AesGcmV1 {
        /// <summary>
        /// AAD prefix.
        /// </summary>
        public byte[]? AadPrefix { get; set; }

        /// <summary>
        /// Unique file identifier part of AAD suffix.
        /// </summary>
        public byte[]? AadFileUnique { get; set; }

        /// <summary>
        /// In files encrypted with AAD prefix without storing it, readers must supply the prefix.
        /// </summary>
        public bool? SupplyAadPrefix { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: AadPrefix, binary
            if(AadPrefix != null) {
                proto.WriteBinaryField(1, AadPrefix);
            }
            // 2: AadFileUnique, binary
            if(AadFileUnique != null) {
                proto.WriteBinaryField(2, AadFileUnique);
            }
            // 3: SupplyAadPrefix, bool
            if(SupplyAadPrefix != null) {
                proto.WriteBoolField(3, SupplyAadPrefix.Value);
            }

            proto.StructEnd();
        }

        internal static AesGcmV1 Read(ThriftCompactProtocolReader proto) {
            var r = new AesGcmV1();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // AadPrefix, binary
                        r.AadPrefix = proto.ReadBinary();
                        break;
                    case 2: // AadFileUnique, binary
                        r.AadFileUnique = proto.ReadBinary();
                        break;
                    case 3: // SupplyAadPrefix, bool
                        r.SupplyAadPrefix = compactType == CompactType.BooleanTrue;
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class AesGcmCtrV1 {
        /// <summary>
        /// AAD prefix.
        /// </summary>
        public byte[]? AadPrefix { get; set; }

        /// <summary>
        /// Unique file identifier part of AAD suffix.
        /// </summary>
        public byte[]? AadFileUnique { get; set; }

        /// <summary>
        /// In files encrypted with AAD prefix without storing it, readers must supply the prefix.
        /// </summary>
        public bool? SupplyAadPrefix { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: AadPrefix, binary
            if(AadPrefix != null) {
                proto.WriteBinaryField(1, AadPrefix);
            }
            // 2: AadFileUnique, binary
            if(AadFileUnique != null) {
                proto.WriteBinaryField(2, AadFileUnique);
            }
            // 3: SupplyAadPrefix, bool
            if(SupplyAadPrefix != null) {
                proto.WriteBoolField(3, SupplyAadPrefix.Value);
            }

            proto.StructEnd();
        }

        internal static AesGcmCtrV1 Read(ThriftCompactProtocolReader proto) {
            var r = new AesGcmCtrV1();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // AadPrefix, binary
                        r.AadPrefix = proto.ReadBinary();
                        break;
                    case 2: // AadFileUnique, binary
                        r.AadFileUnique = proto.ReadBinary();
                        break;
                    case 3: // SupplyAadPrefix, bool
                        r.SupplyAadPrefix = compactType == CompactType.BooleanTrue;
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    public class EncryptionAlgorithm {
        public AesGcmV1? AESGCMV1 { get; set; }

        public AesGcmCtrV1? AESGCMCTRV1 { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: AESGCMV1, id
            if(AESGCMV1 != null) {
                proto.BeginInlineStruct(1);
                AESGCMV1.Write(proto);
            }
            // 2: AESGCMCTRV1, id
            if(AESGCMCTRV1 != null) {
                proto.BeginInlineStruct(2);
                AESGCMCTRV1.Write(proto);
            }

            proto.StructEnd();
        }

        internal static EncryptionAlgorithm Read(ThriftCompactProtocolReader proto) {
            var r = new EncryptionAlgorithm();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // AESGCMV1, id
                        r.AESGCMV1 = AesGcmV1.Read(proto);
                        break;
                    case 2: // AESGCMCTRV1, id
                        r.AESGCMCTRV1 = AesGcmCtrV1.Read(proto);
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Description for file metadata.
    /// </summary>
    public class FileMetaData {
        /// <summary>
        /// Version of this file.
        /// </summary>
        public int Version { get; set; }

        /// <summary>
        /// Parquet schema for this file.  This schema contains metadata for all the columns. The schema is represented as a tree with a single root.  The nodes of the tree are flattened to a list by doing a depth-first traversal. The column metadata contains the path in the schema for that column which can be used to map columns to nodes in the schema. The first element is the root.
        /// </summary>
        public List<SchemaElement> Schema { get; set; } = new List<SchemaElement>();

        /// <summary>
        /// Number of rows in this file.
        /// </summary>
        public long NumRows { get; set; }

        /// <summary>
        /// Row groups in this file.
        /// </summary>
        public List<RowGroup> RowGroups { get; set; } = new List<RowGroup>();

        /// <summary>
        /// Optional key/value metadata.
        /// </summary>
        public List<KeyValue>? KeyValueMetadata { get; set; }

        /// <summary>
        /// String for application that wrote this file.  This should be in the format &lt;Application&gt; version &lt;App Version&gt; (build &lt;App Build Hash&gt;). e.g. impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55).
        /// </summary>
        public string? CreatedBy { get; set; }

        /// <summary>
        /// Sort order used for the min_value and max_value fields in the Statistics objects and the min_values and max_values fields in the ColumnIndex objects of each column in this file. Sort orders are listed in the order matching the columns in the schema. The indexes are not necessary the same though, because only leaf nodes of the schema are represented in the list of sort orders.  Without column_orders, the meaning of the min_value and max_value fields in the Statistics object and the ColumnIndex object is undefined. To ensure well-defined behaviour, if these fields are written to a Parquet file, column_orders must be written as well.  The obsolete min and max fields in the Statistics object are always sorted by signed comparison regardless of column_orders.
        /// </summary>
        public List<ColumnOrder>? ColumnOrders { get; set; }

        /// <summary>
        /// Encryption algorithm. This field is set only in encrypted files with plaintext footer. Files with encrypted footer store algorithm id in FileCryptoMetaData structure.
        /// </summary>
        public EncryptionAlgorithm? EncryptionAlgorithm { get; set; }

        /// <summary>
        /// Retrieval metadata of key used for signing the footer. Used only in encrypted files with plaintext footer.
        /// </summary>
        public byte[]? FooterSigningKeyMetadata { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: Version, i32
            proto.WriteI32Field(1, Version);
            // 2: Schema, list
            proto.WriteListBegin(2, 12, Schema.Count);
            foreach(SchemaElement element in Schema) {
                element.Write(proto);
            }
            // 3: NumRows, i64
            proto.WriteI64Field(3, NumRows);
            // 4: RowGroups, list
            proto.WriteListBegin(4, 12, RowGroups.Count);
            foreach(RowGroup element in RowGroups) {
                element.Write(proto);
            }
            // 5: KeyValueMetadata, list
            if(KeyValueMetadata != null) {
                proto.WriteListBegin(5, 12, KeyValueMetadata.Count);
                foreach(KeyValue element in KeyValueMetadata) {
                    element.Write(proto);
                }
            }
            // 6: CreatedBy, string
            if(CreatedBy != null) {
                proto.WriteStringField(6, CreatedBy);
            }
            // 7: ColumnOrders, list
            if(ColumnOrders != null) {
                proto.WriteListBegin(7, 12, ColumnOrders.Count);
                foreach(ColumnOrder element in ColumnOrders) {
                    element.Write(proto);
                }
            }
            // 8: EncryptionAlgorithm, id
            if(EncryptionAlgorithm != null) {
                proto.BeginInlineStruct(8);
                EncryptionAlgorithm.Write(proto);
            }
            // 9: FooterSigningKeyMetadata, binary
            if(FooterSigningKeyMetadata != null) {
                proto.WriteBinaryField(9, FooterSigningKeyMetadata);
            }

            proto.StructEnd();
        }

        internal static FileMetaData Read(ThriftCompactProtocolReader proto) {
            var r = new FileMetaData();
            proto.StructBegin();
            int elementCount = 0;
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // Version, i32
                        r.Version = proto.ReadI32();
                        break;
                    case 2: // Schema, list
                        elementCount = proto.ReadListHeader(out _);
                        r.Schema = new List<SchemaElement>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.Schema.Add(SchemaElement.Read(proto)); }
                        break;
                    case 3: // NumRows, i64
                        r.NumRows = proto.ReadI64();
                        break;
                    case 4: // RowGroups, list
                        elementCount = proto.ReadListHeader(out _);
                        r.RowGroups = new List<RowGroup>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.RowGroups.Add(RowGroup.Read(proto)); }
                        break;
                    case 5: // KeyValueMetadata, list
                        elementCount = proto.ReadListHeader(out _);
                        r.KeyValueMetadata = new List<KeyValue>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.KeyValueMetadata.Add(KeyValue.Read(proto)); }
                        break;
                    case 6: // CreatedBy, string
                        r.CreatedBy = proto.ReadString();
                        break;
                    case 7: // ColumnOrders, list
                        elementCount = proto.ReadListHeader(out _);
                        r.ColumnOrders = new List<ColumnOrder>(elementCount);
                        for(int i = 0; i < elementCount; i++) { r.ColumnOrders.Add(ColumnOrder.Read(proto)); }
                        break;
                    case 8: // EncryptionAlgorithm, id
                        r.EncryptionAlgorithm = EncryptionAlgorithm.Read(proto);
                        break;
                    case 9: // FooterSigningKeyMetadata, binary
                        r.FooterSigningKeyMetadata = proto.ReadBinary();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

    /// <summary>
    /// Crypto metadata for files with encrypted footer.
    /// </summary>
    public class FileCryptoMetaData {
        /// <summary>
        /// Encryption algorithm. This field is only used for files with encrypted footer. Files with plaintext footer store algorithm id inside footer (FileMetaData structure).
        /// </summary>
        public EncryptionAlgorithm EncryptionAlgorithm { get; set; } = new EncryptionAlgorithm();

        /// <summary>
        /// Retrieval metadata of key used for encryption of footer, and (possibly) columns.
        /// </summary>
        public byte[]? KeyMetadata { get; set; }


        internal void Write(ThriftCompactProtocolWriter proto) {
            proto.StructBegin();

            // 1: EncryptionAlgorithm, id
            proto.BeginInlineStruct(1);
            EncryptionAlgorithm.Write(proto);
            // 2: KeyMetadata, binary
            if(KeyMetadata != null) {
                proto.WriteBinaryField(2, KeyMetadata);
            }

            proto.StructEnd();
        }

        internal static FileCryptoMetaData Read(ThriftCompactProtocolReader proto) {
            var r = new FileCryptoMetaData();
            proto.StructBegin();
            while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
                switch(fieldId) {
                    case 1: // EncryptionAlgorithm, id
                        r.EncryptionAlgorithm = EncryptionAlgorithm.Read(proto);
                        break;
                    case 2: // KeyMetadata, binary
                        r.KeyMetadata = proto.ReadBinary();
                        break;
                    default:
                        proto.SkipField(compactType);
                        break;
                }
            }
            proto.StructEnd();
            return r;
        }
    }

}
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
