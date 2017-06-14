using System;
using System.Collections.Generic;
using System.Text;
using Encoding = Parquet.Thrift.Encoding;

namespace Parquet
{
   public class ParquetTypes
   {
      /**
       * Types supported by Parquet.  These types are intended to be used in combination
       * with the encodings to control the on disk storage format.
       * For example INT16 is not included as a type since a good encoding of INT32
       * would handle this.
       */
      public enum Type
      {
         Boolean = 0,
         Int32 = 1,
         Int64 = 2,
         Int96 = 3,
         Float = 4,
         Double = 5,
         ByteArray = 6,
         FixedLenByteArray = 7
      }

      /**
       * Common types used by frameworks(e.g. hive, pig) using parquet.  This helps map
       * between types in those frameworks to the base types in parquet.  This is only
       * metadata and not needed to read or write the data.
       */
      public enum ConvertedType
      {
         /** a BYTE_ARRAY actually contains UTF8 encoded chars */
         Utf8 = 0,

         /** a map is converted as an optional field containing a repeated key/value pair */
         Map = 1,

         /** a key/value pair is converted into a group of two fields */
         MapKeyValue = 2,

         /** a list is converted into an optional field containing a repeated field for its
          * values */
         List = 3,

         /** an enum is converted into a binary field */
         Enum = 4,

         /**
          * A decimal value.
          *
          * This may be used to annotate binary or fixed primitive types. The
          * underlying byte array stores the unscaled value encoded as two's
          * complement using big-endian byte order (the most significant byte is the
          * zeroth element). The value of the decimal is the value * 10^{-scale}.
          *
          * This must be accompanied by a (maximum) precision and a scale in the
          * SchemaElement. The precision specifies the number of digits in the decimal
          * and the scale stores the location of the decimal point. For example 1.23
          * would have precision 3 (3 total digits) and scale 2 (the decimal point is
          * 2 digits over).
          */
         DecimalValue = 5,

         /**
          * A Date
          *
          * Stored as days since Unix epoch, encoded as the INT32 physical type.
          *
          */
         Date = 6,

         /**
          * A time
          *
          * The total number of milliseconds since midnight.  The value is stored
          * as an INT32 physical type.
          */
         TimeMillis = 7,

         /**
          * A time.
          *
          * The total number of microseconds since midnight.  The value is stored as
          * an INT64 physical type.
          */
         TimeMicros = 8,

         /**
          * A date/time combination
          *
          * Date and time recorded as milliseconds since the Unix epoch.  Recorded as
          * a physical type of INT64.
          */
         TimestampMillis = 9,

         /**
          * A date/time combination
          *
          * Date and time recorded as microseconds since the Unix epoch.  The value is
          * stored as an INT64 physical type.
          */
         TimestampMicros = 10,


         /**
          * An unsigned integer value.
          *
          * The number describes the maximum number of meainful data bits in
          * the stored value. 8, 16 and 32 bit values are stored using the
          * INT32 physical type.  64 bit values are stored using the INT64
          * physical type.
          *
          */
         UInt8 = 11,
         UInt16 = 12,
         UInt32 = 13,
         UInt64 = 14,

         /**
          * A signed integer value.
          *
          * The number describes the maximum number of meainful data bits in
          * the stored value. 8, 16 and 32 bit values are stored using the
          * INT32 physical type.  64 bit values are stored using the INT64
          * physical type.
          *
          */
         Int8 = 15,
         Int16 = 16,
         Int32 = 17,
         Int64 = 18,

         /**
          * An embedded JSON document
          *
          * A JSON document embedded within a single UTF8 column.
          */
         Json = 19,

         /**
          * An embedded BSON document
          *
          * A BSON document embedded within a single BINARY column.
          */
         Bson = 20,

         /**
          * An interval of time
          *
          * This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12
          * This data is composed of three separate little endian unsigned
          * integers.  Each stores a component of a duration of time.  The first
          * integer identifies the number of months associated with the duration,
          * the second identifies the number of days associated with the duration
          * and the third identifies the number of milliseconds associated with
          * the provided duration.  This duration of time is independent of any
          * particular timezone or date.
          */
         Interval = 21
      }


      /**
       * Representation of Schemas
       */
      public enum FieldRepetitionType
      {
         /** This field is required (can not be null) and each record has exactly 1 value. */
         Required = 0,

         /** The field is optional (can be null) and each record has 0 or 1 values. */
         Optional = 1,

         /** The field is repeated and can contain 0 or more values */
         Repeated = 2
      }

      /**
      * Statistics per row group and per page
      * All fields are optional.
      */
      public struct Statistics
      {
         /** min and max value of the column, encoded in PLAIN encoding */
         public byte?[] Max;

         public byte?[] Min;

         /** count of null value in the column */
         public Int64 NullCount;

         /** count of distinct values occurring */
         public Int64 DistinctCount;
      }

      /**
       * Represents a element inside a schema definition.
       *  - if it is a group (inner node) then type is undefined and num_children is defined
       *  - if it is a primitive type (leaf) then type is defined and num_children is undefined
       * the nodes are listed in depth first traversal order.
       */
      public struct SchemaElement
      {
         /** Data type for this field. Not set if the current element is a non-leaf node */
         public Type? Type;

         /** If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the vales.
          * Otherwise, if specified, this is the maximum bit length to store any of the values.
          * (e.g. a low cardinality INT col could have this set to 3).  Note that this is
          * in the schema, and therefore fixed for the entire file.
          */
         public Int32 TypeLength;

         /** repetition of the field. The root of the schema does not have a repetition_type.
          * All other nodes must have one */
         public FieldRepetitionType? RepetitionType;

         /** Name of the field in the schema */
         public string Name;

         /** Nested fields.  Since thrift does not support nested fields,
          * the nesting is flattened to a single list by a depth-first traversal.
          * The children count is used to construct the nested relationship.
          * This field is not set when the element is a primitive type
          */
         public Int32 NumChildren;

         /** When the schema is the result of a conversion from another model
          * Used to record the original type to help with cross conversion.
          */
         public ConvertedType? ConvertedType;

         /** Used when this column contains decimal data.
          * See the DECIMAL converted type for more details.
          */
         public Int32 Scale;

         public Int32 Precision;

         /** When the original schema supports field ids, this will save the
          * original field id in the parquet schema
          */
         public Int32 FieldId;
      }

      /**
       * Encodings supported by Parquet.  Not all encodings are valid for all types.  These
       * enums are also used to specify the encoding of definition and repetition levels.
       * See the accompanying doc for the details of the more complicated encodings.
       */
      public enum ParquetEncoding
      {
         /** Default encoding.
          * BOOLEAN - 1 bit per value. 0 is false; 1 is true.
          * INT32 - 4 bytes per value.  Stored as little-endian.
          * INT64 - 8 bytes per value.  Stored as little-endian.
          * FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
          * DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
          * BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
          * FIXED_LEN_BYTE_ARRAY - Just the bytes.
          */
         Plain = 0,

         /** Group VarInt encoding for INT32/INT64.
          * This encoding is deprecated. It was never used
          */
         //  GROUP_VAR_INT = 1;

         /**
          * Deprecated: Dictionary encoding. The values in the dictionary are encoded in the
          * plain type.
          * in a data page use RLE_DICTIONARY instead.
          * in a Dictionary page use PLAIN instead
          */
         PlainDictionary = 2,

         /** Group packed run length encoding. Usable for definition/repetition levels
          * encoding and Booleans (on one bit: 0 is false; 1 is true.)
          */
         Rle = 3,

         /** Bit packed encoding.  This can only be used if the data has a known max
          * width.  Usable for definition/repetition levels encoding.
          */
         BitPacked = 4,

         /** Delta encoding for integers. This can be used for int columns and works best
          * on sorted data
          */
         DeltaBinaryPacked = 5,

         /** Encoding for byte arrays to separate the length values and the data. The lengths
          * are encoded using DELTA_BINARY_PACKED
          */
         DeltaLengthBinaryArray = 6,

         /** Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
          * Suffixes are stored as delta length byte arrays.
          */
         DeltaByteArray = 7,

         /** Dictionary encoding: the ids are encoded using the RLE encoding
          */
         RleDictionary = 8
      }

      /**
       * Supported compression algorithms.
       */
      public enum CompressionCodec
      {
         Uncompressed = 0,
         Snappy = 1,
         Gzip = 2,
         Lzo = 3,
         Brotli = 4
      }

      public enum PageType
      {
         DataPage = 0,
         IndexPage = 1,
         DictionaryPage = 2,
         DataPagev2 = 3
      }

/** Data page header */
      public struct DataPageHeader
      {
         /** Number of values, including NULLs, in this data page. **/
         public Int32? NumValues;

         /** Encoding used for this data page **/
         //public Encoding? DataPageEncoding;

         /** Encoding used for definition levels **/
         //public Encoding? DefinitionLevelEncoding;

         /** Encoding used for repetition levels **/
         //public Encoding? RepetitionLevelEncoding;

         /** Optional statistics for the data in this page**/
         public Statistics? ParquetStatistics;
      }

      public struct IndexPageHeader
      {
         /** TODO: **/
      }

      public struct DictionaryPageHeader
      {
         /** Number of values in the dictionary **/
         public Int32 NumValues;

         /** Encoding using this dictionary page **/
         //public Encoding? ParquetEncoding;

         /** If true, the entries in the dictionary are sorted in ascending order **/
         public bool IsSorted;
      }

/**
 * New page format alowing reading levels without decompressing the data
 * Repetition and definition levels are uncompressed
 * The remaining section containing the data is compressed if is_compressed is true
 **/
      public struct DataPageHeaderV2
      {
/** Number of values, including NULLs, in this data page. **/
         public Int32 NumValues;

/** Number of NULL values, in this data page.
    Number of non-null = num_values - num_nulls which is also the number of values in the data section **/
         public Int32 NumNulls;

/** Number of rows in this data page. which means pages change on record boundaries (r = 0) **/
         public Int32 NumRows;

/** Encoding used for data in this page **/
         //public Encoding? ParquetEncoding;

// repetition levels and definition levels are always using RLE (without size in it)

/** length of the repetition levels */
         public Int32 DefinitionLevelsByteLength;

         /** length of the definition levels */
         public Int32 RepetitionLevelsByteLength;

/**  whether the values are compressed.
Which means the section of the page between
definition_levels_byte_length + repetition_levels_byte_length + 1 and compressed_page_size (included)
is compressed with the compression_codec.
If missing it is considered compressed */
         public const bool IsCompressed = true;

/** optional statistics for this column chunk */
         public Statistics? ParquetStatistics;
      }

      public struct PageHeader
      {
/** the type of the page: indicates which of the *_header fields is set **/
         public PageType Type;

/** Uncompressed page size in bytes (not including this header) **/
         public Int32 UncompressedPageSize;

/** Compressed page size in bytes (not including this header) **/
         public Int32 CompressedPageSize;

/** 32bit crc for the data below. This allows for disabling checksumming in HDFS
 *  if only a few pages needs to be read
 **/
         public Int32 Crc;

// Headers for page specific data.  One only will be set.
         public DataPageHeader DataPageHeader;

         public IndexPageHeader IndexPageHeader;
         public DictionaryPageHeader DictionaryPageHeader;
         public DataPageHeaderV2 DataPageHeaderv2;
      }

      /**
       * Wrapper struct to store key values
       */
      public struct KeyValue
      {
         public string key;
         public string value;
      }

      /**
       * Wrapper struct to specify sort order
       */
      public struct SortingColumn
      {
/** The column index (in this row group) **/
         public Int32 ColumnIdx;

/** If true, indicates this column is sorted in descending order. **/
         public bool Descending;

/** If true, nulls will come before non-null values, otherwise,
 * nulls go at the end. */
         public bool NullsFirst;
      }

/**
 * statistics of a given page type and encoding
 */
      public struct PageEncodingStats
      {

/** the page type (data/dic/...) **/
         public PageType PageType;

/** encoding of the page **/
         //public Encoding Encoding;

/** number of pages of this type with this encoding **/
         public Int32 Count;
      }

/**
 * Description for column metadata
 */
      public struct ColumnMetaData
      {
/** Type of this column **/
         public Type Type;

/** Set of all encodings used for this column. The purpose is to validate
 * whether we can decode those pages. **/
         //public List<Encoding> Encodings;

/** Path in schema **/
         public List<string> PathInSchema;

/** Compression codec **/
         public CompressionCodec Codec;

/** Number of values in this column **/
         public Int64 NumValues;

/** total byte size of all uncompressed pages in this column chunk (including the headers) **/
         public Int64 TotalUncompressedSize;

/** total byte size of all compressed pages in this column chunk (including the headers) **/
         public Int64 TotalCompressedSize;

/** Optional key/value metadata **/
         public List<KeyValue> KeyValueMetadata;

/** Byte offset from beginning of file to first data page **/
         public Int64 DataPageOffset;

/** Byte offset from beginning of file to root index page **/
         public Int64 IndexPageOffset;

/** Byte offset from the beginning of file to first (only) dictionary page **/
         public Int64 DictionaryPageOffset;

/** optional statistics for this column chunk */
         public Statistics Statistics;

/** Set of all encodings used for pages in this column chunk.
 * This information can be used to determine if all data pages are
 * dictionary encoded for example **/
         public List<PageEncodingStats> EncodingStats;
      }

      public struct ColumnChunk
      {
/** File where column data is stored.  If not set, assumed to be same file as
  * metadata.  This path is relative to the current file.
  **/
         public string FilePath;

/** Byte offset in file_path to the ColumnMetaData **/
         public Int64 FileOffset;

/** Column metadata for this chunk. This is the same content as what is at
 * file_path/file_offset.  Having it here has it replicated in the file
 * metadata.
 **/
         public ColumnMetaData Metadata;
      }

      public struct RowGroup
      {
/** Metadata for each column chunk in this row group.
 * This list must have the same order as the SchemaElement list in FileMetaData.
 **/
         public List<ColumnChunk> Columns;

/** Total byte size of all the uncompressed column data in this row group **/
         public Int64 TotalByteSize;

/** Number of rows in this row group **/
         public Int64 NumRows;

/** If set, specifies a sort ordering of the rows in this RowGroup.
 * The sorting columns can be a subset of all the columns.
 */
         public List<SortingColumn> SortingColumns;
      }

/**
 * Description for file metadata
 */
      public struct FileMetaData
      {
/** Version of this file **/
         public Int32 Version;

/** Parquet schema for this file.  This schema contains metadata for all the columns.
 * The schema is represented as a tree with a single root.  The nodes of the tree
 * are flattened to a list by doing a depth-first traversal.
 * The column metadata contains the path in the schema for that column which can be
 * used to map columns to nodes in the schema.
 * The first element is the root **/
         public List<SchemaElement> Schema;

/** Number of rows in this file **/
         public Int64 NumRows;

/** Row groups in this file **/
         public List<RowGroup> RowGroups;

/** Optional key/value metadata **/
         public List<KeyValue> KeyValueMetadata;

/** String for application that wrote this file.  This should be in the format
 * <Application> version <App Version> (build <App Build Hash>).
 * e.g. impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
 **/
         public string CreatedBy;
      }

   }
}