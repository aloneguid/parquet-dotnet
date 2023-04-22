#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
using System.Collections.Generic;
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
        /// a BYTE_ARRAY actually contains UTF8 encoded chars.
        /// </summary>
        UTF8 = 0,

        /// <summary>
        /// a map is converted as an optional field containing a repeated key/value pair.
        /// </summary>
        MAP = 1,

        /// <summary>
        /// a key/value pair is converted into a group of two fields.
        /// </summary>
        MAP_KEY_VALUE = 2,

        /// <summary>
        /// a list is converted into an optional field containing a repeated field for its values.
        /// </summary>
        LIST = 3,

        /// <summary>
        /// an enum is converted into a binary field.
        /// </summary>
        ENUM = 4,

        /// <summary>
        /// A decimal value.  This may be used to annotate binary or fixed primitive types. The underlying byte array stores the unscaled value encoded as two&#39;s complement using big-endian byte order (the most significant byte is the zeroth element). The value of the decimal is the value * 10^{-scale}.  This must be accompanied by a (maximum) precision and a scale in the SchemaElement. The precision specifies the number of digits in the decimal and the scale stores the location of the decimal point. For example 1.23 would have precision 3 (3 total digits) and scale 2 (the decimal point is 2 digits over).
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
        /// An embedded BSON document  A BSON document embedded within a single BINARY column.
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
        /// This field is required (can not be null) and each record has exactly 1 value.
        /// </summary>
        REQUIRED = 0,

        /// <summary>
        /// The field is optional (can be null) and each record has 0 or 1 values.
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
        /// Encoding for floating-point data. K byte-streams are created where K is the size in bytes of the data type. The individual bytes of an FP value are scattered to the corresponding stream and the streams are concatenated. This itself does not reduce the size of the data but can lead to better compression afterwards.
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
    /// Statistics per row group and per page All fields are optional.
    /// </summary>
    public class Statistics {
        /// <summary>
        /// DEPRECATED: min and max value of the column. Use min_value and max_value.  Values are encoded using PLAIN encoding, except that variable-length byte arrays do not include a length prefix.  These fields encode min and max values determined by signed comparison only. New files should use the correct order for a column&#39;s logical type and store the values in the min_value and max_value fields.  To support older readers, these may be set when the column order is signed.
        /// </summary>
        public byte[]? Max { get; set; }

        public byte[]? Min { get; set; }

        /// <summary>
        /// count of null value in the column.
        /// </summary>
        public long? NullCount { get; set; }

        /// <summary>
        /// count of distinct values occurring.
        /// </summary>
        public long? DistinctCount { get; set; }

        /// <summary>
        /// Min and max values for the column, determined by its ColumnOrder.  Values are encoded using PLAIN encoding, except that variable-length byte arrays do not include a length prefix.
        /// </summary>
        public byte[]? MaxValue { get; set; }

        public byte[]? MinValue { get; set; }

    }

    /// <summary>
    /// Empty structs to use as logical type annotations.
    /// </summary>
    public class StringType {
    }

    public class UUIDType {
    }

    public class MapType {
    }

    public class ListType {
    }

    public class EnumType {
    }

    public class DateType {
    }

    /// <summary>
    /// Logical type to annotate a column that is always null.  Sometimes when discovering the schema of existing data, values are always null and the physical type can&#39;t be determined. This annotation signals the case where the physical type was guessed from all null values.
    /// </summary>
    public class NullType {
    }

    /// <summary>
    /// Decimal logical type annotation  To maintain forward-compatibility in v1, implementations using this logical type must also set scale and precision on the annotated SchemaElement.  Allowed for physical types: INT32, INT64, FIXED, and BINARY.
    /// </summary>
    public class DecimalType {
        public int Scale { get; set; }

        public int Precision { get; set; }

    }

    /// <summary>
    /// Time units for logical types.
    /// </summary>
    public class MilliSeconds {
    }

    public class MicroSeconds {
    }

    public class NanoSeconds {
    }

    public class TimeUnit {
        public MilliSeconds? MILLIS { get; set; }

        public MicroSeconds? MICROS { get; set; }

        public NanoSeconds? NANOS { get; set; }

    }

    /// <summary>
    /// Timestamp logical type annotation  Allowed for physical types: INT64.
    /// </summary>
    public class TimestampType {
        public bool IsAdjustedToUTC { get; set; }

        public TimeUnit Unit { get; set; } = new TimeUnit();

    }

    /// <summary>
    /// Time logical type annotation  Allowed for physical types: INT32 (millis), INT64 (micros, nanos).
    /// </summary>
    public class TimeType {
        public bool IsAdjustedToUTC { get; set; }

        public TimeUnit Unit { get; set; } = new TimeUnit();

    }

    /// <summary>
    /// Integer logical type annotation  bitWidth must be 8, 16, 32, or 64.  Allowed for physical types: INT32, INT64.
    /// </summary>
    public class IntType {
        public sbyte BitWidth { get; set; }

        public bool IsSigned { get; set; }

    }

    /// <summary>
    /// Embedded JSON logical type annotation  Allowed for physical types: BINARY.
    /// </summary>
    public class JsonType {
    }

    /// <summary>
    /// Embedded BSON logical type annotation  Allowed for physical types: BINARY.
    /// </summary>
    public class BsonType {
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
        /// If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the vales. Otherwise, if specified, this is the maximum bit length to store any of the values. (e.g. a low cardinality INT col could have this set to 3).  Note that this is in the schema, and therefore fixed for the entire file.
        /// </summary>
        public int? TypeLength { get; set; }

        /// <summary>
        /// repetition of the field. The root of the schema does not have a repetition_type. All other nodes must have one.
        /// </summary>
        public FieldRepetitionType? RepetitionType { get; set; }

        /// <summary>
        /// Name of the field in the schema.
        /// </summary>
        public string? Name { get; set; }

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

    }

    /// <summary>
    /// Data page header.
    /// </summary>
    public class DataPageHeader {
        /// <summary>
        /// Number of values, including NULLs, in this data page.
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

    }

    public class IndexPageHeader {
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
        /// Number of rows in this data page. which means pages change on record boundaries (r = 0).
        /// </summary>
        public int NumRows { get; set; }

        /// <summary>
        /// Encoding used for data in this page.
        /// </summary>
        public Encoding Encoding { get; set; } = new Encoding();

        /// <summary>
        /// length of the definition levels.
        /// </summary>
        public int DefinitionLevelsByteLength { get; set; }

        /// <summary>
        /// length of the repetition levels.
        /// </summary>
        public int RepetitionLevelsByteLength { get; set; }

        /// <summary>
        /// whether the values are compressed. Which means the section of the page between definition_levels_byte_length + repetition_levels_byte_length + 1 and compressed_page_size (included) is compressed with the compression_codec. If missing it is considered compressed.
        /// </summary>
        public bool? IsCompressed { get; set; }

        /// <summary>
        /// optional statistics for the data in this page.
        /// </summary>
        public Statistics? Statistics { get; set; }

    }

    /// <summary>
    /// Block-based algorithm type annotation.
    /// </summary>
    public class SplitBlockAlgorithm {
    }

    /// <summary>
    /// The algorithm used in Bloom filter.
    /// </summary>
    public class BloomFilterAlgorithm {
        /// <summary>
        /// Block-based Bloom filter.
        /// </summary>
        public SplitBlockAlgorithm? BLOCK { get; set; }

    }

    /// <summary>
    /// Hash strategy type annotation. xxHash is an extremely fast non-cryptographic hash algorithm. It uses 64 bits version of xxHash.
    /// </summary>
    public class XxHash {
    }

    /// <summary>
    /// The hash function used in Bloom filter. This function takes the hash of a column value using plain encoding.
    /// </summary>
    public class BloomFilterHash {
        /// <summary>
        /// xxHash Strategy.
        /// </summary>
        public XxHash? XXHASH { get; set; }

    }

    /// <summary>
    /// The compression used in the Bloom filter.
    /// </summary>
    public class Uncompressed {
    }

    public class BloomFilterCompression {
        public Uncompressed? UNCOMPRESSED { get; set; }

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

    }

    public class PageHeader {
        /// <summary>
        /// the type of the page: indicates which of the *_header fields is set.
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
        /// The 32bit CRC for the page, to be be calculated as follows: - Using the standard CRC32 algorithm - On the data only, i.e. this header should not be included. &#39;Data&#39;   hereby refers to the concatenation of the repetition levels, the   definition levels and the column value, in this exact order. - On the encoded versions of the repetition levels, definition levels and   column values - On the compressed versions of the repetition levels, definition levels   and column values where possible;   - For v1 data pages, the repetition levels, definition levels and column     values are always compressed together. If a compression scheme is     specified, the CRC shall be calculated on the compressed version of     this concatenation. If no compression scheme is specified, the CRC     shall be calculated on the uncompressed version of this concatenation.   - For v2 data pages, the repetition levels and definition levels are     handled separately from the data and are never compressed (only     encoded). If a compression scheme is specified, the CRC shall be     calculated on the concatenation of the uncompressed repetition levels,     uncompressed definition levels and the compressed column values.     If no compression scheme is specified, the CRC shall be calculated on     the uncompressed concatenation. - In encrypted columns, CRC is calculated after page encryption; the   encryption itself is performed after page compression (if compressed) If enabled, this allows for disabling checksumming in HDFS if only a few pages need to be read.
        /// </summary>
        public int? Crc { get; set; }

        public DataPageHeader? DataPageHeader { get; set; }

        public IndexPageHeader? IndexPageHeader { get; set; }

        public DictionaryPageHeader? DictionaryPageHeader { get; set; }

        public DataPageHeaderV2? DataPageHeaderV2 { get; set; }

    }

    /// <summary>
    /// Wrapper struct to store key values.
    /// </summary>
    public class KeyValue {
        public string? Key { get; set; }

        public string? Value { get; set; }

    }

    /// <summary>
    /// Wrapper struct to specify sort order.
    /// </summary>
    public class SortingColumn {
        /// <summary>
        /// The column index (in this row group).
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

    }

    /// <summary>
    /// statistics of a given page type and encoding.
    /// </summary>
    public class PageEncodingStats {
        /// <summary>
        /// the page type (data/dic/...).
        /// </summary>
        public PageType PageType { get; set; } = new PageType();

        /// <summary>
        /// encoding of the page.
        /// </summary>
        public Encoding Encoding { get; set; } = new Encoding();

        /// <summary>
        /// number of pages of this type with this encoding.
        /// </summary>
        public int Count { get; set; }

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
        public List<string?> PathInSchema { get; set; } = new List<string?>();

        /// <summary>
        /// Compression codec.
        /// </summary>
        public CompressionCodec Codec { get; set; } = new CompressionCodec();

        /// <summary>
        /// Number of values in this column.
        /// </summary>
        public long NumValues { get; set; }

        /// <summary>
        /// total byte size of all uncompressed pages in this column chunk (including the headers).
        /// </summary>
        public long TotalUncompressedSize { get; set; }

        /// <summary>
        /// total byte size of all compressed, and potentially encrypted, pages in this column chunk (including the headers).
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
        /// optional statistics for this column chunk.
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

    }

    public class EncryptionWithFooterKey {
    }

    public class EncryptionWithColumnKey {
        /// <summary>
        /// Column path in schema.
        /// </summary>
        public List<string?> PathInSchema { get; set; } = new List<string?>();

        /// <summary>
        /// Retrieval metadata of column encryption key.
        /// </summary>
        public byte[]? KeyMetadata { get; set; }

    }

    public class ColumnCryptoMetaData {
        public EncryptionWithFooterKey? ENCRYPTIONWITHFOOTERKEY { get; set; }

        public EncryptionWithColumnKey? ENCRYPTIONWITHCOLUMNKEY { get; set; }

    }

    public class ColumnChunk {
        /// <summary>
        /// File where column data is stored.  If not set, assumed to be same file as metadata.  This path is relative to the current file.
        /// </summary>
        public string? FilePath { get; set; }

        /// <summary>
        /// Byte offset in file_path to the ColumnMetaData.
        /// </summary>
        public long FileOffset { get; set; }

        /// <summary>
        /// Column metadata for this chunk. This is the same content as what is at file_path/file_offset.  Having it here has it replicated in the file metadata.
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

    }

    /// <summary>
    /// Empty struct to signal the order defined by the physical or logical type.
    /// </summary>
    public class TypeDefinedOrder {
    }

    /// <summary>
    /// Union to specify the order used for the min_value and max_value fields for a column. This union takes the role of an enhanced enum that allows rich elements (which will be needed for a collation-based ordering in the future).  Possible values are: * TypeDefinedOrder - the column uses the order defined by its logical or                      physical type (if there is no logical type).  If the reader does not support the value of this union, min and max stats for this column should be ignored.
    /// </summary>
    public class ColumnOrder {
        /// <summary>
        /// The sort orders for logical types are:   UTF8 - unsigned byte-wise comparison   INT8 - signed comparison   INT16 - signed comparison   INT32 - signed comparison   INT64 - signed comparison   UINT8 - unsigned comparison   UINT16 - unsigned comparison   UINT32 - unsigned comparison   UINT64 - unsigned comparison   DECIMAL - signed comparison of the represented value   DATE - signed comparison   TIME_MILLIS - signed comparison   TIME_MICROS - signed comparison   TIMESTAMP_MILLIS - signed comparison   TIMESTAMP_MICROS - signed comparison   INTERVAL - unsigned comparison   JSON - unsigned byte-wise comparison   BSON - unsigned byte-wise comparison   ENUM - unsigned byte-wise comparison   LIST - undefined   MAP - undefined  In the absence of logical types, the sort order is determined by the physical type:   BOOLEAN - false, true   INT32 - signed comparison   INT64 - signed comparison   INT96 (only used for legacy timestamps) - undefined   FLOAT - signed comparison of the represented value (*)   DOUBLE - signed comparison of the represented value (*)   BYTE_ARRAY - unsigned byte-wise comparison   FIXED_LEN_BYTE_ARRAY - unsigned byte-wise comparison  (*) Because the sorting order is not specified properly for floating     point values (relations vs. total ordering) the following     compatibility rules should be applied when reading statistics:     - If the min is a NaN, it should be ignored.     - If the max is a NaN, it should be ignored.     - If the min is +0, the row group may contain -0 values as well.     - If the max is -0, the row group may contain +0 values as well.     - When looking for NaN values, min and max should be ignored.
        /// </summary>
        public TypeDefinedOrder? TYPEORDER { get; set; }

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
        /// Index within the RowGroup of the first row of the page; this means pages change on record boundaries (r = 0).
        /// </summary>
        public long FirstRowIndex { get; set; }

    }

    public class OffsetIndex {
        /// <summary>
        /// PageLocations, ordered by increasing PageLocation.offset. It is required that page_locations[i].first_row_index &lt; page_locations[i+1].first_row_index.
        /// </summary>
        public List<PageLocation> PageLocations { get; set; } = new List<PageLocation>();

    }

    /// <summary>
    /// Description for ColumnIndex. Each &lt;array-field&gt;[i] refers to the page at OffsetIndex.page_locations[i].
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
        /// Stores whether both min_values and max_values are orderd and if so, in which direction. This allows readers to perform binary searches in both lists. Readers cannot assume that max_values[i] &lt;= min_values[i+1], even if the lists are ordered.
        /// </summary>
        public BoundaryOrder BoundaryOrder { get; set; } = new BoundaryOrder();

        /// <summary>
        /// A list containing the number of null values for each page.
        /// </summary>
        public List<long>? NullCounts { get; set; }

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

    }

    public class EncryptionAlgorithm {
        public AesGcmV1? AESGCMV1 { get; set; }

        public AesGcmCtrV1? AESGCMCTRV1 { get; set; }

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

    }

}
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
