using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using IronCompress;
using Parquet.Data;
using Parquet.Thrift;
using Parquet.Schema;
using Parquet.Encodings;

namespace Parquet.File {
    class DataColumnReader {
        private readonly DataField _dataField;
        private readonly Stream _inputStream;
        private readonly Thrift.ColumnChunk _thriftColumnChunk;
        private readonly Thrift.SchemaElement? _thriftSchemaElement;
        private readonly ThriftFooter _footer;
        private readonly ParquetOptions _options;
        private readonly ThriftStream _thriftStream;
        private readonly DataColumnStatistics? _stats;

        public DataColumnReader(
           DataField dataField,
           Stream inputStream,
           Thrift.ColumnChunk thriftColumnChunk,
           ThriftFooter footer,
           ParquetOptions? parquetOptions) {
            _dataField = dataField ?? throw new ArgumentNullException(nameof(dataField));
            _inputStream = inputStream ?? throw new ArgumentNullException(nameof(inputStream));
            _thriftColumnChunk = thriftColumnChunk ?? throw new ArgumentNullException(nameof(thriftColumnChunk));
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _options = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

            dataField.EnsureAttachedToSchema(nameof(dataField));

            _thriftStream = new ThriftStream(inputStream);
            _thriftSchemaElement = _footer.GetSchemaElement(_thriftColumnChunk);

            // read stats as soon as possible
            Thrift.Statistics st = _thriftColumnChunk.Meta_data.Statistics;
            if(st != null) {

                ParquetPlainEncoder.TryDecode(st.Min_value, _thriftSchemaElement!, _options, out object? min);
                ParquetPlainEncoder.TryDecode(st.Max_value, _thriftSchemaElement!, _options, out object? max);

                _stats = new DataColumnStatistics(
                    st.__isset.null_count ? st.Null_count : null,
                    st.__isset.distinct_count ? st.Distinct_count : null,
                    min, max);
            }
        }

        public async Task<DataColumn> ReadAsync(CancellationToken cancellationToken = default) {

            // how many values are in column chunk, as there may be multiple data pages
            int totalValuesInChunk = (int)_thriftColumnChunk.Meta_data.Num_values;
            int definedValuesCount = totalValuesInChunk;
            if(_stats?.NullCount != null)
                definedValuesCount -= (int)_stats.NullCount.Value;
            using var pc = new PackedColumn(_dataField, totalValuesInChunk, definedValuesCount);
            long fileOffset = GetFileOffset();
            _inputStream.Seek(fileOffset, SeekOrigin.Begin);

            while(pc.ValuesRead < totalValuesInChunk) {
                Thrift.PageHeader ph = await _thriftStream.ReadAsync<Thrift.PageHeader>(cancellationToken);

                switch(ph.Type) {
                    case PageType.DICTIONARY_PAGE:
                        await ReadDictionaryPage(ph, pc);
                        break;
                    case PageType.DATA_PAGE:
                        await ReadDataPageV1Async(ph, pc);
                        break;
                    case PageType.DATA_PAGE_V2:
                        await ReadDataPageV2Async(ph, pc, totalValuesInChunk);
                        break;
                    default:
                        throw new NotSupportedException($"can't read page type {ph.Type}"); ;
                }
            }

            // all the data is available here!
            DataColumn column = pc.Unpack();
            if(_stats != null) column.Statistics = _stats;
            return column;
        }

        private async Task<IronCompress.IronCompressResult> ReadPageDataAsync(Thrift.PageHeader ph) {

            byte[] data = ArrayPool<byte>.Shared.Rent(ph.Compressed_page_size);

            int totalBytesRead = 0, remainingBytes = ph.Compressed_page_size;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            if(_thriftColumnChunk.Meta_data.Codec == Thrift.CompressionCodec.UNCOMPRESSED) {
                return new IronCompress.IronCompressResult(data, Codec.Snappy, false, ph.Compressed_page_size, ArrayPool<byte>.Shared);
            }

            return Compressor.Decompress((CompressionMethod)(int)_thriftColumnChunk.Meta_data.Codec,
                data.AsSpan(0, ph.Compressed_page_size),
                ph.Uncompressed_page_size);
        }
        
        private async Task<IronCompress.IronCompressResult> ReadPageDataV2Async(Thrift.PageHeader ph) {

            int pageSize = ph.Compressed_page_size;
            
            byte[] data = ArrayPool<byte>.Shared.Rent(pageSize);

            int totalBytesRead = 0, remainingBytes = pageSize;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            return new IronCompress.IronCompressResult(data, Codec.Snappy, false, pageSize, ArrayPool<byte>.Shared);
        }

        private async ValueTask ReadDictionaryPage(Thrift.PageHeader ph, PackedColumn pc) {

            if(pc.HasDictionary)
                throw new InvalidOperationException("dictionary already read");

            //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain encoding.
            using IronCompress.IronCompressResult bytes = await ReadPageDataAsync(ph);

            // Dictionary should not contains null values
            Array dictionary = _dataField.CreateArray(ph.Dictionary_page_header.Num_values);

            ParquetPlainEncoder.Decode(dictionary, 0, ph.Dictionary_page_header.Num_values,
                   _thriftSchemaElement!, bytes.AsSpan(), out int dictionaryOffset);

            pc.AssignDictionary(dictionary);
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

        private async Task ReadDataPageV1Async(Thrift.PageHeader ph, PackedColumn pc) {
            using IronCompress.IronCompressResult bytes = await ReadPageDataAsync(ph);

            if(ph.Data_page_header == null) {
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
            }

            int dataUsed = 0;
            int allValueCount = (int)_thriftColumnChunk.Meta_data.Num_values;
            int pageValueCount = ph.Data_page_header.Num_values;

            if(_dataField.MaxRepetitionLevel > 0) {
                //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.

                int levelsRead = ReadLevels(
                    bytes.AsSpan(), _dataField.MaxRepetitionLevel,
                    pc.GetWriteableRepetitionLevelSpan(),
                    pageValueCount, null, out int usedLength);
                pc.MarkRepetitionLevels(levelsRead);
                dataUsed += usedLength;
            }

            int defNulls = 0;
            if(_dataField.MaxDefinitionLevel > 0) {
                int levelsRead = ReadLevels(
                    bytes.AsSpan().Slice(dataUsed), _dataField.MaxDefinitionLevel,
                    pc.GetWriteableDefinitionLevelSpan(),
                    pageValueCount, null, out int usedLength);
                dataUsed += usedLength;
                defNulls = pc.MarkDefinitionLevels(levelsRead, _dataField.MaxDefinitionLevel);
            }

            // try to be clever to detect how many elements to read
            int dataElementCount = pageValueCount - defNulls;

            ReadColumn(
                bytes.AsSpan().Slice(dataUsed),
                ph.Data_page_header.Encoding,
                allValueCount, dataElementCount,
                pc);
        }

        private async Task ReadDataPageV2Async(Thrift.PageHeader ph, PackedColumn pc, long maxValues) {
            if(ph.Data_page_header_v2 == null) {
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
            } 
            
            using IronCompress.IronCompressResult bytes = await ReadPageDataV2Async(ph);
            int dataUsed = 0;

            if(_dataField.MaxRepetitionLevel > 0) {
                //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.
                int levelsRead = ReadLevels(bytes.AsSpan(),
                    _dataField.MaxRepetitionLevel, pc.GetWriteableRepetitionLevelSpan(),
                    ph.Data_page_header_v2.Num_values, ph.Data_page_header_v2.Repetition_levels_byte_length, out int usedLength);
                dataUsed += usedLength;
                pc.MarkRepetitionLevels(levelsRead);
            }

            if(_dataField.MaxDefinitionLevel > 0) {
                int levelsRead = ReadLevels(bytes.AsSpan().Slice(dataUsed),
                    _dataField.MaxDefinitionLevel, pc.GetWriteableDefinitionLevelSpan(),
                    ph.Data_page_header_v2.Num_values, ph.Data_page_header_v2.Definition_levels_byte_length, out int usedLength);
                dataUsed += usedLength;
                pc.MarkDefinitionLevels(levelsRead);
            }

            int maxReadCount = ph.Data_page_header_v2.Num_values - ph.Data_page_header_v2.Num_nulls;

            if((!ph.Data_page_header_v2.Is_compressed) || _thriftColumnChunk.Meta_data.Codec == Thrift.CompressionCodec.UNCOMPRESSED) {
                ReadColumn(bytes.AsSpan().Slice(dataUsed), ph.Data_page_header_v2.Encoding, maxValues, maxReadCount, pc);
                return;
            }

            int dataSize = ph.Compressed_page_size - ph.Data_page_header_v2.Repetition_levels_byte_length -
                           ph.Data_page_header_v2.Definition_levels_byte_length;

            int decompressedSize = ph.Uncompressed_page_size - ph.Data_page_header_v2.Repetition_levels_byte_length -
                                   ph.Data_page_header_v2.Definition_levels_byte_length;
            
            IronCompress.IronCompressResult decompressedDataByes = Compressor.Decompress(
                (CompressionMethod)(int)_thriftColumnChunk.Meta_data.Codec,
                bytes.AsSpan().Slice(dataUsed),
                decompressedSize);

            ReadColumn(decompressedDataByes.AsSpan(),
                ph.Data_page_header_v2.Encoding,
                maxValues, maxReadCount,
                pc);
        }

        private int ReadLevels(Span<byte> s, int maxLevel,
            Span<int> dest,
            int pageSize,
            int? length, out int usedLength) {

            int bitWidth = maxLevel.GetBitWidth();

            return RleBitpackedHybridEncoder.Decode(s, bitWidth, length, out usedLength, dest, pageSize);
        }

        private void ReadColumn(Span<byte> src,
            Thrift.Encoding encoding, long totalValuesInChunk, int totalValuesInPage,
            PackedColumn pc) {

            //dictionary encoding uses RLE to encode data

            switch(encoding) {
                case Thrift.Encoding.PLAIN: { // 0
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        ParquetPlainEncoder.Decode(plainData,
                            offset,
                            totalValuesInPage,
                            _thriftSchemaElement!, src, out int read);
                        pc.MarkUsefulPlainData(read);
                    }
                    break;

                case Thrift.Encoding.PLAIN_DICTIONARY: // 2  // values are still encoded in RLE
                case Thrift.Encoding.RLE_DICTIONARY: { // 8
                        Span<int> span = pc.AllocateOrGetDictionaryIndexes(totalValuesInPage);
                        int indexCount = ReadRleDictionary(src, totalValuesInPage, span);
                        pc.MarkUsefulDictionaryIndexes(indexCount);
                        pc.Checkpoint();
                    }
                    break;

                case Thrift.Encoding.RLE: { // 3
                        Span<int> span = pc.AllocateOrGetDictionaryIndexes(totalValuesInPage);
                        int indexCount = RleBitpackedHybridEncoder.Decode(src,
                            _thriftSchemaElement!.Type_length,
                            src.Length, out int usedLength, span, totalValuesInPage);
                        pc.MarkUsefulDictionaryIndexes(indexCount);
                        pc.Checkpoint();
                    }
                    break;

                case Thrift.Encoding.DELTA_BINARY_PACKED: {// 5
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        int read = DeltaBinaryPackedEncoder.Decode(src, plainData, offset, totalValuesInPage, out _);
                        pc.MarkUsefulPlainData(read);
                    }
                    break;

                case Thrift.Encoding.DELTA_LENGTH_BYTE_ARRAY: {  // 6
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        int read = DeltaLengthByteArrayEncoder.Decode(src, plainData, offset, totalValuesInPage);
                        pc.MarkUsefulPlainData(read);
                    }
                    break;

                case Thrift.Encoding.DELTA_BYTE_ARRAY: {         // 7
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        int read = DeltaByteArrayEncoder.Decode(src, plainData, offset, totalValuesInPage);
                        pc.MarkUsefulPlainData(read);
                    }
                    break;

                case Thrift.Encoding.BIT_PACKED:                // 4 (deprecated)
                case Thrift.Encoding.BYTE_STREAM_SPLIT:         // 9
                default:
                    throw new ParquetException($"encoding {encoding} is not supported.");
            }
        }

        private static int ReadRleDictionary(Span<byte> s, int maxReadCount, Span<int> dest) {
            int offset = 0;
            int destOffset = 0;
            int start = destOffset;
            int bitWidth = s[offset++];

            int length = s.Length - 1;

            //when bit width is zero reader must stop and just repeat zero maxValue number of times
            if(bitWidth == 0 || length == 0) {
                for(int i = 0; i < maxReadCount; i++) {
                    dest[destOffset++] = 0;
                }
            }
            else {
                if(length != 0) {
                    destOffset += RleBitpackedHybridEncoder.Decode(s.Slice(1), bitWidth, length, out int usedLength, dest, maxReadCount);
                }
            }

            return destOffset - start;
        }

        private static int GetRemainingLength(Stream s) => (int)(s.Length - s.Position);
    }
}
