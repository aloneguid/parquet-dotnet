using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Extensions;

namespace Parquet.Meta.Proto;

class ThriftCompactProtocolReader {
    private readonly Stream _inputStream;
    private readonly Stack<short> _lastField = new Stack<short>(15);
    private short _lastFieldId;
    private readonly byte[] _singleByte = new byte[1];

    public ThriftCompactProtocolReader(Stream inputStream) {
        _inputStream = inputStream;
    }

    public void StructBegin() {
        _lastField.Push(_lastFieldId);
        _lastFieldId = 0;
    }

    public void StructEnd() {
        _lastFieldId = _lastField.Pop();
    }

    private static int ZigzagToInt(uint n) {
        return (int)(n >> 1) ^ -(int)(n & 1);
    }

    private static long ZigzagToLong(ulong n) {
        return (long)(n >> 1) ^ -(long)(n & 1);
    }

    private uint ReadVarInt32() {
        /*
        Read an i32 from the wire as a varint. The MSB of each byte is set
        if there is another byte to follow. This can Read up to 5 bytes.
        */

        uint result = 0;
        int shift = 0;

        while(true) {
            byte b = (byte)_inputStream.ReadByte();
            result |= (uint)(b & 0x7f) << shift;
            if((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }

        return result;
    }

    private ulong ReadVarInt64() {
        /*
        Read an i64 from the wire as a proper varint. The MSB of each byte is set
        if there is another byte to follow. This can Read up to 10 bytes.
        */

        int shift = 0;
        ulong result = 0;
        while(true) {
            byte b = (byte)_inputStream.ReadByte();
            result |= (ulong)(b & 0x7f) << shift;
            if((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }

        return result;
    }

    public bool ReadBool() {
        byte b = (byte)_inputStream.ReadByte();
        return (CompactType)b == CompactType.BooleanTrue;
    }

    public sbyte ReadByte() {
        return (sbyte)_inputStream.ReadByte();
    }

    public short ReadI16() {
        return (short)ZigzagToInt(ReadVarInt32());
    }

    public int ReadI32() {
        return ZigzagToInt(ReadVarInt32());
    }

    public long ReadI64() {
        return ZigzagToLong(ReadVarInt64());
    }

    public double ReadDouble() {
        byte[] data = _inputStream.ReadBytesExactly(sizeof(double));
        if(!BitConverter.IsLittleEndian) {
            Array.Reverse(data);
        }

        return BitConverter.ToDouble(data, 0);
    }

    public byte[] ReadBinary() {
        // read length
        int length = (int)ReadVarInt32();
        if(length == 0) {
            return Array.Empty<byte>();
        }

        // read data
        return _inputStream.ReadBytesExactly(length);
    }

    public string ReadString() {
        // read length
        int length = (int)ReadVarInt32();
        if(length == 0) {
            return string.Empty;
        }

        // read data
        byte[] utf8Data = _inputStream.ReadBytesExactly(length);
        return System.Text.Encoding.UTF8.GetString(utf8Data, 0, length);
    }

    public bool ReadNextField(out short fieldId, out CompactType compactType) {

        // Read a field header off the wire.
        byte header = (byte)_inputStream.ReadByte();

        // if it's a stop, then we can return immediately, as the struct is over.
        if((CompactType)header == CompactType.Stop) {
            fieldId = 0;
            compactType = 0;
            return false;
        }

        // mask off the 4 MSB of the exType header. it could contain a field id delta.
        short modifier = (short)((header & 0xf0) >> 4);
        compactType = (CompactType)(byte)(header & 0x0f);

        if(modifier == 0) {
            fieldId = ReadI16();
        } else {
            fieldId = (short)(_lastFieldId + modifier);
        }

        // push the new field onto the field stack so we can keep the deltas going.
        _lastFieldId = fieldId;
        return true;
    }

    public int ReadListHeader(out CompactType elementType) {
        /*
        Read a list header off the wire. If the list size is 0-14, the size will
        be packed into the element exType header. If it's a longer list, the 4 MSB
        of the element exType header will be 0xF, and a varint will follow with the
        true size.
        */

        byte sizeAndType = (byte)_inputStream.ReadByte();
        int size = (sizeAndType >> 4) & 0x0f;
        if(size == 15) {
            size = (int)ReadVarInt32();
        }
        elementType = (CompactType)(byte)(sizeAndType & 0x0f);
        return size;
    }

    public void SkipField(CompactType compactType) {
        switch(compactType) {
            case CompactType.BooleanTrue:
            case CompactType.BooleanFalse:
                break;
            case CompactType.Byte:
                _inputStream.Seek(1, SeekOrigin.Current);
                break;
            case CompactType.I16:
                ReadI16();
                break;
            case CompactType.I32:
                ReadI32();
                break;
            case CompactType.I64:
                ReadI64();
                break;
            case CompactType.Double:
                ReadDouble();
                break;
            case CompactType.Binary:
                // Don't try to decode the string, just skip it.
                ReadBinary();
                break;
            //case TType.Uuid:
            //    await protocol.ReadUuidAsync(cancellationToken);
            //    break;
            case CompactType.Struct:
                StructBegin();
                while(ReadNextField(out _, out _)) {

                }
                StructEnd();
                break;
            case CompactType.List:
                int elementCount = ReadListHeader(out CompactType elementType);
                for(int i = 0; i < elementCount; i++) {
                    SkipField(elementType);
                }
                break;
            default:
                throw new ThriftProtocolException($"Invalid data in Thrift header. File is most likely corrupt (unexpected data type {compactType}).");
        }
    }

    private async ValueTask<byte> ReadByteAsync(CancellationToken cancellationToken) {
        await _inputStream.ReadExactlyAsync(_singleByte, cancellationToken);
        return _singleByte[0];
    }

    private async ValueTask<ulong> ReadVarIntAsync(CancellationToken cancellationToken) {
        ulong result = 0;
        for(int shift = 0; shift < 64; shift += 7) {
            byte b = await ReadByteAsync(cancellationToken);
            if(shift == 63 && (b & 0xfe) != 0)
                throw new ThriftProtocolException("Invalid variable-length integer in Thrift header.");
            result |= (ulong)(b & 0x7f) << shift;
            if((b & 0x80) == 0)
                return result;
        }
        throw new ThriftProtocolException("Invalid variable-length integer in Thrift header.");
    }

    public async ValueTask<int> ReadI32Async(CancellationToken cancellationToken) =>
        ZigzagToInt(checked((uint)await ReadVarIntAsync(cancellationToken)));

    public async ValueTask<long> ReadI64Async(CancellationToken cancellationToken) =>
        ZigzagToLong(await ReadVarIntAsync(cancellationToken));

    public async ValueTask<byte[]> ReadBinaryAsync(CancellationToken cancellationToken) {
        int length = checked((int)await ReadVarIntAsync(cancellationToken));
        if(length == 0)
            return Array.Empty<byte>();
        byte[] data = new byte[length];
        await _inputStream.ReadExactlyAsync(data, cancellationToken);
        return data;
    }

    public async ValueTask<(short FieldId, CompactType CompactType)> ReadNextFieldAsync(CancellationToken cancellationToken) {
        byte header = await ReadByteAsync(cancellationToken);
        if(header == (byte)CompactType.Stop)
            return (0, CompactType.Stop);

        short modifier = (short)((header & 0xf0) >> 4);
        short fieldId = modifier == 0
            ? (short)await ReadI32Async(cancellationToken)
            : (short)(_lastFieldId + modifier);
        _lastFieldId = fieldId;
        return (fieldId, (CompactType)(header & 0x0f));
    }

    private async ValueTask<(int ElementCount, CompactType ElementType)> ReadListHeaderAsync(CancellationToken cancellationToken) {
        byte sizeAndType = await ReadByteAsync(cancellationToken);
        int count = (sizeAndType >> 4) & 0x0f;
        if(count == 15)
            count = checked((int)await ReadVarIntAsync(cancellationToken));
        return (count, (CompactType)(sizeAndType & 0x0f));
    }

    private async ValueTask SkipCollectionValueAsync(CompactType compactType, CancellationToken cancellationToken) {
        if(compactType is CompactType.BooleanTrue or CompactType.BooleanFalse)
            await ReadByteAsync(cancellationToken);
        else
            await SkipFieldAsync(compactType, cancellationToken);
    }

    public async ValueTask SkipFieldAsync(CompactType compactType, CancellationToken cancellationToken) {
        switch(compactType) {
            case CompactType.BooleanTrue:
            case CompactType.BooleanFalse:
                break;
            case CompactType.Byte:
                await ReadByteAsync(cancellationToken);
                break;
            case CompactType.I16:
            case CompactType.I32:
            case CompactType.I64:
                await ReadVarIntAsync(cancellationToken);
                break;
            case CompactType.Double:
                await _inputStream.ReadExactlyAsync(new byte[sizeof(double)], cancellationToken);
                break;
            case CompactType.Binary:
                await ReadBinaryAsync(cancellationToken);
                break;
            case CompactType.Struct:
                StructBegin();
                while(true) {
                    var field = await ReadNextFieldAsync(cancellationToken);
                    if(field.CompactType == CompactType.Stop)
                        break;
                    await SkipFieldAsync(field.CompactType, cancellationToken);
                }
                StructEnd();
                break;
            case CompactType.List:
            case CompactType.Set:
                var list = await ReadListHeaderAsync(cancellationToken);
                for(int i = 0; i < list.ElementCount; i++)
                    await SkipCollectionValueAsync(list.ElementType, cancellationToken);
                break;
            case CompactType.Map:
                int count = checked((int)await ReadVarIntAsync(cancellationToken));
                if(count == 0)
                    break;
                byte types = await ReadByteAsync(cancellationToken);
                for(int i = 0; i < count; i++) {
                    await SkipCollectionValueAsync((CompactType)(types >> 4), cancellationToken);
                    await SkipCollectionValueAsync((CompactType)(types & 0x0f), cancellationToken);
                }
                break;
            case CompactType.Uuid:
                await _inputStream.ReadExactlyAsync(new byte[16], cancellationToken);
                break;
            default:
                throw new ThriftProtocolException($"Invalid data in Thrift header. File is most likely corrupt (unexpected data type {compactType}).");
        }
    }
}
