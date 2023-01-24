using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Thrift;
using Parquet.Schema;
using Parquet.Extensions;
using Parquet.File.Values;

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
        }

        public async Task<DataColumn> ReadAsync(CancellationToken cancellationToken = default) {
            var colData = new ColumnRawData();
            long fileOffset = GetFileOffset();
            _inputStream.Seek(fileOffset, SeekOrigin.Begin);
            
            Thrift.PageHeader ph = await _thriftStream.ReadAsync<Thrift.PageHeader>(cancellationToken);
            (bool read, Array data, int offset) = await TryReadDictionaryPageAsync(ph);
            if(read) {
                colData.dictionary = data;
                colData.dictionaryOffset = offset;
                ph = await _thriftStream.ReadAsync<Thrift.PageHeader>(cancellationToken);
            }

            long totalValuesInChunk = _thriftColumnChunk.Meta_data.Num_values;

            colData.maxCount = (int)_thriftColumnChunk.Meta_data.Num_values;

            while(true) {
                if(ph.Type == PageType.DATA_PAGE_V2) {
                    await ReadDataPageV2Async(ph, colData, totalValuesInChunk);
                }
                else {
                    await ReadDataPageAsync(ph, colData, totalValuesInChunk);
                }

                int totalCount = Math.Max(
                   (colData.values == null ? 0 : colData.valuesOffset),
                   (colData.definitions == null ? 0 : colData.definitionsOffset));
                if(totalCount >= totalValuesInChunk)
                    break; //limit reached

                ph = await _thriftStream.ReadAsync<Thrift.PageHeader>(cancellationToken);
                if(ph.Type != Thrift.PageType.DATA_PAGE && ph.Type != Thrift.PageType.DATA_PAGE_V2)
                    break;
            }

            // all the data is available here!

            var finalColumn = new DataColumn(
               _dataField, colData.values,
               colData.definitions, _maxDefinitionLevel,
               colData.repetitions, _maxRepetitionLevel);

            if(_thriftColumnChunk.Meta_data.Statistics != null) {

                ParquetPlainEncoder.TryDecode(_thriftColumnChunk.Meta_data.Statistics.Min_value, _thriftSchemaElement, out object min);
                ParquetPlainEncoder.TryDecode(_thriftColumnChunk.Meta_data.Statistics.Max_value, _thriftSchemaElement, out object max);

                finalColumn.Statistics = new DataColumnStatistics(
                   _thriftColumnChunk.Meta_data.Statistics.Null_count,
                   _thriftColumnChunk.Meta_data.Statistics.Distinct_count,
                   min, max);
            }

            // i don't understand the point of this, but leaving as a comment as looks suspicious
            // Fix for nullable booleans
            /*if(finalColumn.Data.GetType().GetElementType() == typeof(Boolean?)) {
                finalColumn.Field.ClrNullableIfHasNullsType = DataTypeFactory.Match(finalColumn.Field.DataType).ClrType.GetNullable();
                _dataField.ClrNullableIfHasNullsType = DataTypeFactory.Match(finalColumn.Field.DataType).ClrType.GetNullable();
            }*/

            return finalColumn;
        }

        private async Task<IronCompress.DataBuffer> ReadPageDataAsync(Thrift.PageHeader ph) {

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
        
        private async Task<IronCompress.DataBuffer> ReadPageDataV2Async(Thrift.PageHeader ph) {

            int pageSize = ph.Compressed_page_size;
            
            byte[] data = ArrayPool<byte>.Shared.Rent(pageSize);

            int totalBytesRead = 0, remainingBytes = pageSize;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            return new IronCompress.DataBuffer(data, pageSize, ArrayPool<byte>.Shared);
        }

        private async Task<(bool, Array, int)> TryReadDictionaryPageAsync(Thrift.PageHeader ph) {
            if(ph.Type != Thrift.PageType.DICTIONARY_PAGE) {
                return (false, null, 0);
            }

            //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain encoding.
            using IronCompress.DataBuffer bytes = await ReadPageDataAsync(ph);
            //todo: this is ugly, but will be removed once other parts are migrated to System.Memory
            using var ms = new MemoryStream(bytes.AsSpan().ToArray());
            // Dictionary should not contains null values
            Array dictionary = _dataField.CreateArray(ph.Dictionary_page_header.Num_values);

            if(!ParquetPlainEncoder.Decode(dictionary, 0, ph.Dictionary_page_header.Num_values, 
                   _thriftSchemaElement, ms, out int dictionaryOffset)) {
                throw new IOException("could not decode");
            }

            return (true, dictionary, dictionaryOffset);
        }

        private long GetFileOffset() =>
            // get the minimum offset, we'll just read pages in sequence as Dictionary_page_offset/Data_page_offset are not reliable
            new[]
                {
                    _thriftColumnChunk.Meta_data.Dictionary_page_offset,
                    _thriftColumnChunk.Meta_data.Data_page_offset
                }
                .Where(e => e != 0)
                .Min();

        private async Task ReadDataPageAsync(Thrift.PageHeader ph, ColumnRawData cd, long totalValuesInChunk) {
            using IronCompress.DataBuffer bytes = await ReadPageDataAsync(ph);
            //todo: this is ugly, but will be removed once other parts are migrated to System.Memory
            if(ph.Data_page_header == null) {
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
            }

            using var ms = new MemoryStream(bytes.AsSpan().ToArray());
            int valueCount = ph.Data_page_header.Num_values;
            if(_maxRepetitionLevel > 0) {
                //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.
                if(cd.repetitions == null)
                    cd.repetitions = new int[cd.maxCount];

                cd.repetitionsOffset += ReadLevels(ms, _maxRepetitionLevel, cd.repetitions, cd.repetitionsOffset, ph.Data_page_header.Num_values);
            }

            if(_maxDefinitionLevel > 0) {
                cd.definitions ??= new int[cd.maxCount];

                int offsetBeforeReading = cd.definitionsOffset;
                cd.definitionsOffset += ReadLevels(ms, _maxDefinitionLevel, cd.definitions, cd.definitionsOffset, ph.Data_page_header.Num_values);
                // if no statistics are available, we use the number of values expected, based on the definitions
                if(ph.Data_page_header.Statistics == null) {
                    valueCount = cd.definitions
                        .Skip(offsetBeforeReading).Take(cd.definitionsOffset - offsetBeforeReading)
                        .Count(v => v > 0);
                }
            }

            // if statistics are defined, use null count to determine the exact number of items we should read,
            // otherwise the previously counted value from definitions
            int totalValuesInPage = ph.Data_page_header.Statistics == null ? valueCount
                : ph.Data_page_header.Num_values - (int)ph.Data_page_header.Statistics.Null_count;
            ReadColumn(ms, ph.Data_page_header.Encoding, totalValuesInChunk, totalValuesInPage, cd);
        }

        private async Task ReadDataPageV2Async(Thrift.PageHeader ph, ColumnRawData cd, long maxValues) {
            if(ph.Data_page_header_v2 == null) {
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
            } 
            
            using IronCompress.DataBuffer bytes = await ReadPageDataV2Async(ph);

            using var ms = new MemoryStream(bytes.AsSpan().ToArray());

            if(_maxRepetitionLevel > 0) {
                //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.
                cd.repetitions ??= new int[cd.maxCount];
                cd.repetitionsOffset += ReadLevels(ms, _maxRepetitionLevel, cd.repetitions, cd.repetitionsOffset, ph.Data_page_header_v2.Num_values, ph.Data_page_header_v2.Repetition_levels_byte_length);
            }

            if(_maxDefinitionLevel > 0) {
                cd.definitions ??= new int[cd.maxCount];
                cd.definitionsOffset += ReadLevels(ms, _maxDefinitionLevel, cd.definitions, cd.definitionsOffset, ph.Data_page_header_v2.Num_values, ph.Data_page_header_v2.Definition_levels_byte_length);
            }

            int maxReadCount = ph.Data_page_header_v2.Num_values - ph.Data_page_header_v2.Num_nulls;

            if((!ph.Data_page_header_v2.Is_compressed) || _thriftColumnChunk.Meta_data.Codec == Thrift.CompressionCodec.UNCOMPRESSED) {
                ReadColumn(ms, ph.Data_page_header_v2.Encoding, maxValues, maxReadCount, cd);
                return;
            }

            int dataSize = ph.Compressed_page_size - ph.Data_page_header_v2.Repetition_levels_byte_length -
                           ph.Data_page_header_v2.Definition_levels_byte_length;

            int decompressedSize = ph.Uncompressed_page_size - ph.Data_page_header_v2.Repetition_levels_byte_length -
                                   ph.Data_page_header_v2.Definition_levels_byte_length;
            
            byte[] dataBytes = ms.ReadBytesExactly(dataSize);
            
            IronCompress.DataBuffer decompressedDataByes = Compressor.Decompress(
                (CompressionMethod)(int)_thriftColumnChunk.Meta_data.Codec,
                dataBytes.AsSpan(),
                decompressedSize);

            using var dataMs = new MemoryStream(decompressedDataByes.AsSpan().ToArray());

            ReadColumn(dataMs, ph.Data_page_header_v2.Encoding, maxValues, maxReadCount, cd);
        }

        private int ReadLevels(Stream s, int maxLevel, int[] dest, int offset, int pageSize, int length = 0) {
            int bitWidth = maxLevel.GetBitWidth();

            return RleEncoder.Decode(s, bitWidth, length, dest, offset, pageSize);
        }

        private void ReadColumn(Stream s, Thrift.Encoding encoding, long totalValuesInChunk, int totalValuesInPage,
            ColumnRawData cd) {

            //dictionary encoding uses RLE to encode data

            cd.values ??= _dataField.CreateArray((int)totalValuesInChunk);

            switch(encoding) {
                case Thrift.Encoding.PLAIN: {
                        if(!ParquetPlainEncoder.Decode(cd.values,
                            cd.valuesOffset,
                            totalValuesInPage,
                            _thriftSchemaElement, s, out int read)) {
                            throw new IOException("could not decode");
                        }
                        cd.valuesOffset += read;
                    }
                    break;

                case Thrift.Encoding.RLE: {
                        cd.indexes ??= new int[(int)totalValuesInChunk];
                        int indexCount = RleEncoder.Decode(s, cd.indexes, 0, _thriftSchemaElement.Type_length, totalValuesInPage);
                        cd.dictionary.ExplodeFast(cd.indexes.AsSpan(), cd.values, cd.valuesOffset, indexCount);
                        cd.valuesOffset += indexCount;
                    }
                    break;

                case Thrift.Encoding.PLAIN_DICTIONARY:  // values are still encoded in RLE
                case Thrift.Encoding.RLE_DICTIONARY: {
                        cd.indexes ??= new int[(int)totalValuesInChunk];
                        int indexCount = ReadRleDictionary(s, totalValuesInPage, cd.indexes, 0);
                        cd.dictionary.ExplodeFast(cd.indexes.AsSpan(), cd.values, cd.valuesOffset, indexCount);
                        cd.valuesOffset += indexCount;
                    }
                    break;

                case Thrift.Encoding.DELTA_BYTE_ARRAY:
                    cd.valuesOffset += DeltaByteArrayReader.Read(s, cd.values, cd.valuesOffset, totalValuesInPage);
                    break;

                case Encoding.BIT_PACKED:
                case Encoding.DELTA_BINARY_PACKED:
                case Encoding.DELTA_LENGTH_BYTE_ARRAY:
                case Encoding.BYTE_STREAM_SPLIT:
                default:
                    throw new ParquetException($"encoding {encoding} is not supported.");
            }
        }

        private static int ReadRleDictionary(Stream s, int maxReadCount, int[] dest, int offset) {
            int start = offset;
            int bitWidth = s.ReadByte();

            int length = GetRemainingLength(s);

            //when bit width is zero reader must stop and just repeat zero maxValue number of times
            if(bitWidth == 0 || length == 0) {
                for(int i = 0; i < maxReadCount; i++) {
                    dest[offset++] = 0;
                }
            }
            else {
                if(length != 0) {
                    offset += RleEncoder.Decode(s, bitWidth, length, dest, offset, maxReadCount);
                }
            }

            return offset - start;
        }

        private static int GetRemainingLength(Stream s) => (int)(s.Length - s.Position);
    }
}
