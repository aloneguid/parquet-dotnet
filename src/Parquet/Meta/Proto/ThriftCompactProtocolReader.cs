using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Thrift.Protocol;
using System.Diagnostics;
using Parquet.Extensions;

namespace Parquet.Meta.Proto {
    class ThriftCompactProtocolReader {
        private readonly Stream _inputStream;
        private readonly Stack<short> _lastField = new Stack<short>(15);
        private short _lastFieldId;

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

        public bool ReadNextField(out short fieldId, out byte compactType) {

            // Read a field header off the wire.
            byte typeId = (byte)_inputStream.ReadByte();

            // if it's a stop, then we can return immediately, as the struct is over.
            if(typeId == Types.Stop) {
                fieldId = 0;
                compactType = 0;
                return false;
            }

            // mask off the 4 MSB of the exType header. it could contain a field id delta.
            short modifier = (short)((typeId & 0xf0) >> 4);
            compactType = (byte)(typeId & 0x0f);

            if(modifier == 0) {
                fieldId = ReadI16();
            } else {
                fieldId = (short)(_lastFieldId + modifier);
            }

            // push the new field onto the field stack so we can keep the deltas going.
            _lastFieldId = fieldId;
            return true;
        }

        public void SkipField(byte compactType) {
            switch(compactType) {
                case Types.BooleanTrue:
                case Types.BooleanFalse:
                    break;
                /*case TType.Byte:
                    await protocol.ReadByteAsync(cancellationToken);
                    break;
                case TType.I16:
                    await protocol.ReadI16Async(cancellationToken);
                    break;
                case TType.I32:
                    await protocol.ReadI32Async(cancellationToken);
                    break;
                case TType.I64:
                    await protocol.ReadI64Async(cancellationToken);
                    break;
                case TType.Double:
                    await protocol.ReadDoubleAsync(cancellationToken);
                    break;
                case TType.String:
                    // Don't try to decode the string, just skip it.
                    await protocol.ReadBinaryAsync(cancellationToken);
                    break;
                case TType.Uuid:
                    await protocol.ReadUuidAsync(cancellationToken);
                    break;
                case TType.Struct:
                    await protocol.ReadStructBeginAsync(cancellationToken);
                    while(true) {
                        var field = await protocol.ReadFieldBeginAsync(cancellationToken);
                        if(field.Type == TType.Stop) {
                            break;
                        }
                        await SkipAsync(protocol, field.Type, cancellationToken);
                        await protocol.ReadFieldEndAsync(cancellationToken);
                    }
                    await protocol.ReadStructEndAsync(cancellationToken);
                    break;
                case TType.Map:
                    var map = await protocol.ReadMapBeginAsync(cancellationToken);
                    for(var i = 0; i < map.Count; i++) {
                        await SkipAsync(protocol, map.KeyType, cancellationToken);
                        await SkipAsync(protocol, map.ValueType, cancellationToken);
                    }
                    await protocol.ReadMapEndAsync(cancellationToken);
                    break;
                case TType.Set:
                    var set = await protocol.ReadSetBeginAsync(cancellationToken);
                    for(var i = 0; i < set.Count; i++) {
                        await SkipAsync(protocol, set.ElementType, cancellationToken);
                    }
                    await protocol.ReadSetEndAsync(cancellationToken);
                    break;
                case TType.List:
                    var list = await protocol.ReadListBeginAsync(cancellationToken);
                    for(var i = 0; i < list.Count; i++) {
                        await SkipAsync(protocol, list.ElementType, cancellationToken);
                    }
                    await protocol.ReadListEndAsync(cancellationToken);
                    break;*/
                default:
                    throw new InvalidOperationException($"don't know how to skip type {compactType}");
            }
        }
    }
}
