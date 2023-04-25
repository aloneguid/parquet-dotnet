using System.Buffers;
using System.Collections.Generic;
using System.IO;

namespace Parquet.Meta.Proto {

    class ThriftCompactProtocolWriter {
        // Used to keep track of the last field for the current and previous structs, so we can do the delta stuff.
        private readonly Stack<short> _lastField = new Stack<short>(15);    // 15 is max recursion
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
            _outputStream.WriteByte((byte)CompactType.Stop);
        }

        /// <summary>
        /// Optimisation to write an empty struct - avoid push/pop
        /// </summary>
        public void WriteEmptyStruct() {
            // write stop field
            _outputStream.WriteByte((byte)CompactType.Stop);
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

        private void WriteFieldBegin(short fieldId, CompactType fieldType) {
            // check if we can use delta encoding for the field id
            if(fieldId > _lastFieldId) {
                int delta = fieldId - _lastFieldId;
                if(delta <= 15) {
                    // Write them together
                    byte b = (byte)((delta << 4) | (byte)fieldType);
                    _outputStream.WriteByte(b);
                    _lastFieldId = fieldId;
                    return;
                }
            }

            // Write them separate
            _outputStream.WriteByte((byte)fieldType);

            WriteI16Async(fieldId);
            _lastFieldId = fieldId;
        }

        #region [ Writers for various field types ]

        public void BeginInlineStruct(short fieldId) {
            WriteFieldBegin(fieldId, CompactType.Struct);
        }

        public void WriteBoolValue(bool value) {
            _outputStream.WriteByte((byte)(value ? CompactType.BooleanTrue : CompactType.BooleanFalse));
        }

        public void WriteBoolField(short fieldId, bool value) {
            if(value)
                WriteFieldBegin(fieldId, CompactType.BooleanTrue);
            else
                WriteFieldBegin(fieldId, CompactType.BooleanFalse);
        }

        public void WriteByteField(short fieldId, sbyte value) {
            WriteFieldBegin(fieldId, CompactType.Byte);
            _outputStream.WriteByte((byte)value);
        }

        public void WriteI16Field(short fieldId, short value) {
            WriteFieldBegin(fieldId, CompactType.I16);

            Int32ToVarInt(IntToZigzag(value), ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
        }

        public void WriteI32Value(int value) {
            Int32ToVarInt(IntToZigzag(value), ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
        }

        public void WriteI32Field(short fieldId, int value) {
            WriteFieldBegin(fieldId, CompactType.I32);
            WriteI32Value(value);
        }

        public void WriteI64Value(long value) {
            Int64ToVarInt(LongToZigzag(value), ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
        }

        public void WriteI64Field(short fieldId, long value) {
            WriteFieldBegin(fieldId, CompactType.I64);
            WriteI64Value(value);
        }

        public void WriteBinaryValue(byte[] value) {
            Int32ToVarInt((uint)value.Length, ref PreAllocatedVarInt);
            _outputStream.Write(PreAllocatedVarInt.bytes, 0, PreAllocatedVarInt.count);
            _outputStream.Write(value, 0, value.Length);
        }

        public void WriteBinaryField(short fieldId, byte[] value) {
            WriteFieldBegin(fieldId, CompactType.Binary);
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
            WriteFieldBegin(fieldId, CompactType.Binary);
            WriteStringValue(value);
        }

        public void WriteListBegin(short fieldId, byte elementType, int elementCount) {
            WriteFieldBegin(fieldId, CompactType.List);

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
