using System;
using System.Collections.Generic;
using System.IO;
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
                //case Types.Double:
                    //await protocol.ReadDoubleAsync(cancellationToken);
                    //break;
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
                    throw new InvalidOperationException($"don't know how to skip type {compactType}");
            }
        }
    }
}
