using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File.Values;
using Parquet.Thrift;

namespace Parquet.File {
    class DataColumnReader {
        private readonly DataField _dataField;
        private readonly Stream _inputStream;
        private readonly Thrift.ColumnChunk _thriftColumnChunk;
        private readonly Thrift.SchemaElement _thriftSchemaElement;
        private readonly ThriftFooter _footer;
        private readonly ParquetOptions _parquetOptions;
        private readonly ThriftStream _thriftStream;
        private readonly int _maxRepetitionLevel;
        private readonly int _maxDefinitionLevel;
        private readonly IDataTypeHandler _dataTypeHandler;

        private class ColumnRawData {
            public int maxCount;

            public int[] repetitions;
            public int repetitionsOffset;

            public int[] definitions;
            public int definitionsOffset;

            public int[] indexes;

            public Array values;
            public int valuesOffset;

            public Array dictionary;
            public int dictionaryOffset;
        }

        public DataColumnReader(
           DataField dataField,
           Stream inputStream,
           Thrift.ColumnChunk thriftColumnChunk,
           ThriftFooter footer,
           ParquetOptions parquetOptions) {
            _dataField = dataField ?? throw new ArgumentNullException(nameof(dataField));
            _inputStream = inputStream ?? throw new ArgumentNullException(nameof(inputStream));
            _thriftColumnChunk = thriftColumnChunk ?? throw new ArgumentNullException(nameof(thriftColumnChunk));
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _parquetOptions = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

            _thriftStream = new ThriftStream(inputStream);
            _footer.GetLevels(_thriftColumnChunk, out int mrl, out int mdl);
            _dataField.MaxRepetitionLevel = mrl;
            _dataField.MaxDefinitionLevel = mdl;
            _maxRepetitionLevel = mrl;
            _maxDefinitionLevel = mdl;
            _thriftSchemaElement = _footer.GetSchemaElement(_thriftColumnChunk);
            _dataTypeHandler = DataTypeFactory.Match(_thriftSchemaElement, _parquetOptions);
        }

        public async Task<DataColumn> ReadAsync(CancellationToken cancellationToken = default) {
            var colData = new ColumnRawData();
            long fileOffset = GetFileOffset();
            _inputStream.Seek(fileOffset, SeekOrigin.Begin);
            
            Thrift.PageHeader ph = await _thriftStream.ReadAsync<Thrift.PageHeader>(cancellationToken);
            (bool read, Array data, int offset) = await TryReadDictionaryPage(ph);
            if(read) {
                colData.dictionary = data;
                colData.dictionaryOffset = offset;
                ph = await _thriftStream.ReadAsync<Thrift.PageHeader>(cancellationToken);
            }

            long maxValues = _thriftColumnChunk.Meta_data.Num_values;

            colData.maxCount = (int)_thriftColumnChunk.Meta_data.Num_values;

            int pagesRead = 0;

            while(true) {
                if(ph.Type == PageType.DATA_PAGE_V2) {
                    await ReadDataPageV2(ph, colData, maxValues);
                }
                else {
                    await ReadDataPage(ph, colData, maxValues);
                }

                pagesRead++;

                int totalCount = Math.Max(
                   (colData.values == null ? 0 : colData.valuesOffset),
                   (colData.definitions == null ? 0 : colData.definitionsOffset));
                if(totalCount >= maxValues)
                    break; //limit reached

                ph = await _thriftStream.ReadAsync<Thrift.PageHeader>(cancellationToken);
                if(ph.Type != Thrift.PageType.DATA_PAGE && ph.Type != Thrift.PageType.DATA_PAGE_V2)
                    break;
            }

            // all the data is available here!

            var finalColumn = new DataColumn(
               _dataField, colData.values,
               colData.definitions, _maxDefinitionLevel,
               colData.repetitions, _maxRepetitionLevel,
               colData.dictionary,
               colData.indexes);

            if(_thriftColumnChunk.Meta_data.Statistics != null) {
                finalColumn.Statistics = new DataColumnStatistics(
                   _thriftColumnChunk.Meta_data.Statistics.Null_count,
                   _thriftColumnChunk.Meta_data.Statistics.Distinct_count,
                   _dataTypeHandler.PlainDecode(_thriftSchemaElement, _thriftColumnChunk.Meta_data.Statistics.Min_value),
                   _dataTypeHandler.PlainDecode(_thriftSchemaElement, _thriftColumnChunk.Meta_data.Statistics.Max_value));
            }

            // Fix for nullable booleans
            if(finalColumn.Data.GetType().GetElementType() == typeof(Boolean?)) {
                finalColumn.Field.ClrNullableIfHasNullsType = DataTypeFactory.Match(finalColumn.Field.DataType).ClrType.GetNullable();
                _dataField.ClrNullableIfHasNullsType = DataTypeFactory.Match(finalColumn.Field.DataType).ClrType.GetNullable();
            }

            return finalColumn;
        }

        private async Task<IronCompress.DataBuffer> ReadPageData(Thrift.PageHeader ph) {

            byte[] data = ArrayPool<byte>.Shared.Rent(ph.Compressed_page_size);

            int totalBytesRead = 0, remainingBytes = ph.Compressed_page_size;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            if(_thriftColumnChunk.Meta_data.Codec == Thrift.CompressionCodec.UNCOMPRESSED) {
                return new IronCompress.DataBuffer(data, ph.Compressed_page_size, ArrayPool<byte>.Shared);
            }

            return Compressor.Decompress((CompressionMethod)(int)_thriftColumnChunk.Meta_data.Codec,
                data.AsSpan(0, ph.Compressed_page_size),
                ph.Uncompressed_page_size);
        }
        
        private async Task<byte[]> ReadPageDataV2(Thrift.PageHeader ph) {

            int pageSize = ph.Compressed_page_size;
            
            byte[] data = ArrayPool<byte>.Shared.Rent(pageSize);

            int totalBytesRead = 0, remainingBytes = pageSize;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            return data;
        }

        private async Task<(bool, Array, int)> TryReadDictionaryPage(Thrift.PageHeader ph) {
            if(ph.Type != Thrift.PageType.DICTIONARY_PAGE) {
                return (false, null, 0);
            }

            //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain encoding.
            using(IronCompress.DataBuffer bytes = await ReadPageData(ph)) {
                //todo: this is ugly, but will be removed once other parts are migrated to System.Memory
                using(var ms = new MemoryStream(bytes.AsSpan().ToArray())) {
                    using(var dataReader = new BinaryReader(ms)) {
                        // Dictionary should not contains null values
                        Array dictionary = _dataTypeHandler.GetArray(ph.Dictionary_page_header.Num_values, false, false);
                        int dictionaryOffset = _dataTypeHandler.Read(dataReader, _thriftSchemaElement, dictionary, 0);
                        return (true, dictionary, dictionaryOffset);
                    }
                }
            }
        }

        private long GetFileOffset() {
            // get the minimum offset, we'll just read pages in sequence as Dictionary_page_offset/Data_page_offset are not reliable

            return
               new[]
               {
               _thriftColumnChunk.Meta_data.Dictionary_page_offset,
               _thriftColumnChunk.Meta_data.Data_page_offset
               }
               .Where(e => e != 0)
               .Min();
        }

        private async Task ReadDataPage(Thrift.PageHeader ph, ColumnRawData cd, long maxValues) {
            using(IronCompress.DataBuffer bytes = await ReadPageData(ph)) {
                //todo: this is ugly, but will be removed once other parts are migrated to System.Memory
                if(ph.Data_page_header == null) {
                    throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
                }
 
                using(var ms = new MemoryStream(bytes.AsSpan().ToArray())) {
                    int valueCount = ph.Data_page_header.Num_values;
                    using(var reader = new BinaryReader(ms)) {
                        if(_maxRepetitionLevel > 0) {
                            //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.
                            if(cd.repetitions == null)
                                cd.repetitions = new int[cd.maxCount];

                            cd.repetitionsOffset += ReadLevels(reader, _maxRepetitionLevel, cd.repetitions, cd.repetitionsOffset, ph.Data_page_header.Num_values);
                        }

                        if(_maxDefinitionLevel > 0) {
                            if(cd.definitions == null)
                                cd.definitions = new int[cd.maxCount];

                            int offsetBeforeReading = cd.definitionsOffset;
                            cd.definitionsOffset += ReadLevels(reader, _maxDefinitionLevel, cd.definitions, cd.definitionsOffset, ph.Data_page_header.Num_values);
                            // if no statistics are available, we use the number of values expected, based on the definitions
                            if(ph.Data_page_header.Statistics == null) {
                                valueCount = cd.definitions
                                   .Skip(offsetBeforeReading).Take(cd.definitionsOffset - offsetBeforeReading)
                                   .Count(v => v > 0);
                            }
                        }

                        // if statistics are defined, use null count to determine the exact number of items we should read,
                        // otherwise the previously counted value from definitions
                        int maxReadCount = ph.Data_page_header.Statistics == null ? valueCount
                           : ph.Data_page_header.Num_values - (int)ph.Data_page_header.Statistics.Null_count;
                        ReadColumn(reader, ph.Data_page_header.Encoding, maxValues, maxReadCount, cd);
                    }
                }
            }
        }
        private async Task ReadDataPageV2(Thrift.PageHeader ph, ColumnRawData cd, long maxValues) {
            if(ph.Data_page_header_v2 == null) {
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
            } 
            
            byte[] bytes = await ReadPageDataV2(ph);

            using var ms = new MemoryStream(bytes);
            using var reader = new BinaryReader(ms);

            if(_maxRepetitionLevel > 0) {
                //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.
                cd.repetitions ??= new int[cd.maxCount];
                cd.repetitionsOffset += ReadLevels(reader, _maxRepetitionLevel, cd.repetitions, cd.repetitionsOffset, ph.Data_page_header_v2.Num_values, ph.Data_page_header_v2.Repetition_levels_byte_length);
            }

            if(_maxDefinitionLevel > 0) {
                cd.definitions ??= new int[cd.maxCount];
                cd.definitionsOffset += ReadLevels(reader, _maxDefinitionLevel, cd.definitions, cd.definitionsOffset, ph.Data_page_header_v2.Num_values, ph.Data_page_header_v2.Definition_levels_byte_length);
            }

            int maxReadCount = ph.Data_page_header_v2.Num_values - ph.Data_page_header_v2.Num_nulls;

            if(!ph.Data_page_header_v2.Is_compressed) {
                ReadColumn(reader, ph.Data_page_header_v2.Encoding, maxValues, maxReadCount, cd);
                return;
            }

            int dataSize = ph.Compressed_page_size - ph.Data_page_header_v2.Repetition_levels_byte_length -
                           ph.Data_page_header_v2.Definition_levels_byte_length;

            int decompressedSize = ph.Uncompressed_page_size - ph.Data_page_header_v2.Repetition_levels_byte_length -
                                   ph.Data_page_header_v2.Definition_levels_byte_length;
            
            byte[] dataBytes = reader.ReadBytes(dataSize);
            
            IronCompress.DataBuffer decompressedDataByes = Compressor.Decompress(
                (CompressionMethod)(int)_thriftColumnChunk.Meta_data.Codec,
                dataBytes.AsSpan(),
                decompressedSize);

            var dataMs = new MemoryStream(decompressedDataByes.AsSpan().ToArray());
            var dataReader = new BinaryReader(dataMs);

            ReadColumn(dataReader, ph.Data_page_header_v2.Encoding, maxValues, maxReadCount, cd);
        }

        private int ReadLevels(BinaryReader reader, int maxLevel, int[] dest, int offset, int pageSize, int length = 0) {
            int bitWidth = maxLevel.GetBitWidth();

            return RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, length, dest, offset, pageSize);
        }

        private void ReadColumn(BinaryReader reader, Thrift.Encoding encoding, long totalValues, int maxReadCount, ColumnRawData cd) {
            //dictionary encoding uses RLE to encode data

            if(cd.values == null) {
                cd.values = _dataTypeHandler.GetArray((int)totalValues, false, false);
            }

            switch(encoding) {
                case Thrift.Encoding.PLAIN:
                    cd.valuesOffset += _dataTypeHandler.Read(reader, _thriftSchemaElement, cd.values, cd.valuesOffset);
                    break;

                case Thrift.Encoding.RLE:
                    if(cd.indexes == null)
                        cd.indexes = new int[(int)totalValues];
                    int indexCount = RunLengthBitPackingHybridValuesReader.Read(reader, _thriftSchemaElement.Type_length, cd.indexes, 0, maxReadCount);
                    _dataTypeHandler.MergeDictionary(cd.dictionary, cd.indexes, cd.values, cd.valuesOffset, indexCount);
                    cd.valuesOffset += indexCount;
                    break;

                case Thrift.Encoding.PLAIN_DICTIONARY:
                case Thrift.Encoding.RLE_DICTIONARY:
                    if(cd.indexes == null)
                        cd.indexes = new int[(int)totalValues];
                    indexCount = ReadPlainDictionary(reader, maxReadCount, cd.indexes, 0);
                    _dataTypeHandler.MergeDictionary(cd.dictionary, cd.indexes, cd.values, cd.valuesOffset, indexCount);
                    cd.valuesOffset += indexCount;
                    break;

                default:
                    throw new ParquetException($"encoding {encoding} is not supported.");
            }
        }

        private static int ReadPlainDictionary(BinaryReader reader, int maxReadCount, int[] dest, int offset) {
            int start = offset;
            int bitWidth = reader.ReadByte();

            int length = GetRemainingLength(reader);

            //when bit width is zero reader must stop and just repeat zero maxValue number of times
            if(bitWidth == 0 || length == 0) {
                for(int i = 0; i < maxReadCount; i++) {
                    dest[offset++] = 0;
                }
            }
            else {
                if(length != 0) {
                    offset += RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, length, dest, offset, maxReadCount);
                }
            }

            return offset - start;
        }

        private static int GetRemainingLength(BinaryReader reader) {
            return (int)(reader.BaseStream.Length - reader.BaseStream.Position);
        }
    }
}
