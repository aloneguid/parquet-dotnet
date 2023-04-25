namespace Parquet.Meta.Proto {
    enum CompactType : byte {
        Stop = 0x00,
        BooleanTrue = 0x01,
        BooleanFalse = 0x02,
        Byte = 0x03,
        I16 = 0x04,
        I32 = 0x05,
        I64 = 0x06,
        Double = 0x07,
        Binary = 0x08,
        List = 0x09,
        Set = 0x0A,
        Map = 0x0B,
        Struct = 0x0C,
        Uuid = 0x0D
    }
}
