using System.IO;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Xunit;

namespace Parquet.Test;

public class ThriftTest : TestBase {
    [Fact]
    public void TestFileRead_Table() {
        using Stream fs = OpenTestFile("thrift/wide.bin");
        FileMetaData fileMeta = FileMetaData.Read(new ThriftCompactProtocolReader(fs));
    }

    [Fact]
    public void SkipField_NestedStruct_ConsumesAllFieldValues() {
        // Hand-crafted Thrift compact-protocol payload representing an
        // outer struct with three fields:
        //   1: i32 = 42
        //   2: nested struct { 1: i32 = 99 }   <-- skipped via SkipField
        //   3: i32 = 7                          <-- must remain reachable
        //
        // If SkipField(Struct) only reads the nested field's HEADER but
        // not its VALUE, the read cursor is left mid-value and field 3
        // is unreachable: ReadNextField mis-parses the leftover value
        // bytes as a new field header.
        //
        // Compact-protocol encoding details:
        //   field header byte = (delta << 4) | type
        //   i32 value         = zigzag-varint
        //
        //   42 -> zigzag 84       -> varint 0x54
        //   99 -> zigzag 198      -> varint 0xC6 0x01
        //    7 -> zigzag 14       -> varint 0x0E
        //   I32 type=5, Struct type=12, Stop type=0
        byte[] payload = new byte[] {
            0x15,                   // field 1, I32
            0x54,                   //   value 42
            0x1C,                   // field 2, Struct
              0x15,                 //   nested field 1, I32
              0xC6, 0x01,           //     value 99
              0x00,                 //   nested struct stop
            0x15,                   // field 3, I32
            0x0E,                   //   value 7
            0x00,                   // outer struct stop
        };

        using var stream = new MemoryStream(payload);
        var reader = new ThriftCompactProtocolReader(stream);

        reader.StructBegin();

        bool sawField1 = false;
        bool sawField3 = false;
        while(reader.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1:
                    Assert.Equal(CompactType.I32, compactType);
                    Assert.Equal(42, reader.ReadI32());
                    sawField1 = true;
                    break;
                case 3:
                    Assert.Equal(CompactType.I32, compactType);
                    Assert.Equal(7, reader.ReadI32());
                    sawField3 = true;
                    break;
                default:
                    // Treat field 2 (the nested struct) as unknown and
                    // skip it. This is the path Parquet.Net's auto-
                    // generated Read methods take for any field id
                    // newer than they were generated against.
                    reader.SkipField(compactType);
                    break;
            }
        }

        reader.StructEnd();

        Assert.True(sawField1, "field 1 should have been read");
        Assert.True(sawField3,
            "field 3 should still be reachable after SkipField on the " +
            "non-empty nested struct -- if this fails, SkipField left " +
            "the read cursor mis-aligned");
    }
}
