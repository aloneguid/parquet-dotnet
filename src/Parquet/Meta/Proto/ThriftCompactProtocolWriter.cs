using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Parquet.Meta.Proto {

    class ThriftCompactProtocolWriter {

        private static class Types {
            public const byte Stop = 0x00;
            public const byte BooleanTrue = 0x01;
            public const byte BooleanFalse = 0x02;
            public const byte Byte = 0x03;
            public const byte I16 = 0x04;
            public const byte I32 = 0x05;
            public const byte I64 = 0x06;
            public const byte Double = 0x07;
            public const byte Binary = 0x08;
            public const byte List = 0x09;
            public const byte Set = 0x0A;
            public const byte Map = 0x0B;
            public const byte Struct = 0x0C;
            public const byte Uuid = 0x0D;
        }

        // Used to keep track of the last field for the current and previous structs, so we can do the delta stuff.
        private readonly Stack<short> _lastField = new Stack<short>(15);    // 15 is max recursion for thrift
        private readonly Stream _outputStream;
        private short _lastFieldId;

        public ThriftCompactProtocolWriter(Stream outputStream) {
            _outputStream = outputStream;
        }

        // note: increment/decrement recursion depth from the original only performs sanity check, no real logic

        // todo: there is some weird logic for booleans to implement

        public void StructBegin() {
            _lastField.Push(_lastFieldId);
            _lastFieldId = 0;
        }

        public void StructEnd() {
            _lastFieldId = _lastField.Pop();

            // write stop field
            _outputStream.WriteByte(Types.Stop);
        }

        /// <summary>
        /// Optimisation to write an empty struct - avoid push/pop
        /// </summary>
        public void WriteEmptyStruct() {
            // write stop field
            _outputStream.WriteByte(Types.Stop);
        }

        private struct VarInt {
            public byte[] bytes;
            public int count;
        }

        // minimize memory allocations by means of an preallocated VarInt buffer
        private VarInt PreAllocatedVarInt = new VarInt() {
            bytes = new byte[10], // see Int64ToVarInt()
            count = 0
        };

        private static void Int32ToVarInt(uint n, ref VarInt varint) {
            // Write an i32 as a varint. Results in 1 - 5 bytes on the wire.
            varint.count = 0;

            while(true) {
                if((n & ~0x7F) == 0) {
                    varint.bytes[varint.count++] = (byte)n;
                    break;
                }

                varint.bytes[varint.count++] = (byte)((n & 0x7F) | 0x80);
                n >>= 7;
            }
        }

        private static void Int64ToVarInt(ulong n, ref VarInt varint) {
            // Write an i64 as a varint. Results in 1-10 bytes on the wire.
            varint.count = 0;

            while(true) {
                if((n & ~(ulong)0x7FL) == 0) {
                    varint.bytes[varint.count++] = (byte)n;
                    break;
                }
                varint.bytes[varint.count++] = (byte)((n & 0x7F) | 0x80);
                n >>= 7;
            }
        }

        private static uint IntToZigzag(int n) {
            // Convert n into a zigzag int. This allows negative numbers to be represented compactly as a varint
            return (uint)(n << 1) ^ (uint)(n >> 31);
        }

        private static ulong LongToZigzag(long n) {
            // Convert l into a zigzag long. This allows negative numbers to be represented compactly as a varint
            return (ulong)(n << 1) ^ (ulong)(n >> 63);
        }

        private void WriteI16Async(short i16) {
            Int32ToVarInt(IntToZigzag(i16), ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
        }

        private void WriteFieldBegin(short fieldId, byte fieldType) {
            // check if we can use delta encoding for the field id
            if(fieldId > _lastFieldId) {
                int delta = fieldId - _lastFieldId;
                if(delta <= 15) {
                    // Write them together
                    byte b = (byte)((delta << 4) | fieldType);
                    _outputStream.WriteByte(b);
                    _lastFieldId = fieldId;
                    return;
                }
            }

            // Write them separate
            _outputStream.WriteByte(fieldType);

            WriteI16Async(fieldId);
            _lastFieldId = fieldId;
        }

        #region [ Writers for various field types ]

        public void BeginInlineStruct(short fieldId) {
            WriteFieldBegin(fieldId, Types.Struct);
        }

        public void WriteBoolValue(bool value) {
            _outputStream.WriteByte(value ? Types.BooleanTrue : Types.BooleanFalse);
        }

        public void WriteBoolField(short fieldId, bool value) {
            if(value)
                WriteFieldBegin(fieldId, Types.BooleanTrue);
            else
                WriteFieldBegin(fieldId, Types.BooleanFalse);
        }

        public void WriteByteField(short fieldId, sbyte value) {
            WriteFieldBegin(fieldId, Types.Byte);
            _outputStream.WriteByte((byte)value);
        }

        public void WriteI16Field(short fieldId, short value) {
            WriteFieldBegin(fieldId, Types.I16);

            Int32ToVarInt(IntToZigzag(value), ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
        }

        public void WriteI32Value(int value) {
            Int32ToVarInt(IntToZigzag(value), ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
        }

        public void WriteI32Field(short fieldId, int value) {
            WriteFieldBegin(fieldId, Types.I32);
            WriteI32Value(value);
        }

        public void WriteI64Value(long value) {
            Int64ToVarInt(LongToZigzag(value), ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
        }

        public void WriteI64Field(short fieldId, long value) {
            WriteFieldBegin(fieldId, Types.I64);
            WriteI64Value(value);
        }

        public void WriteBinaryValue(byte[] value) {
            Int32ToVarInt((uint)value.Length, ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
            _outputStream.Write(value, 0, value.Length);
        }

        public void WriteBinaryField(short fieldId, byte[] value) {
            WriteFieldBegin(fieldId, Types.Binary);
            WriteBinaryValue(value);
        }

        public void WriteStringValue(string value) {
            byte[] buf = ArrayPool<byte>.Shared.Rent(System.Text.Encoding.UTF8.GetByteCount(value));
            try {
                int numberOfBytes = System.Text.Encoding.UTF8.GetBytes(value, 0, value.Length, buf, 0);
                Int32ToVarInt((uint)numberOfBytes, ref PreAllocatedVarInt);
                _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
                _outputStream.Write(buf, 0, numberOfBytes);
            } finally {
                ArrayPool<byte>.Shared.Return(buf);
            }
        }

        public void WriteStringField(short fieldId, string value) {
            WriteFieldBegin(fieldId, Types.Binary);
            WriteStringValue(value);
        }

        public void WriteListBegin(short fieldId, byte elementType, int elementCount) {
            WriteFieldBegin(fieldId, Types.List);

            if(elementCount <= 14) {
                byte b = (byte)((elementCount << 4) | elementType);
                _outputStream.WriteByte(b);
            } else {
                byte b = (byte)(0xf0 | elementType);
                _outputStream.WriteByte(b);

                Int32ToVarInt((uint)elementCount, ref PreAllocatedVarInt);
                _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
            }
        }

        #endregion
    }
}
