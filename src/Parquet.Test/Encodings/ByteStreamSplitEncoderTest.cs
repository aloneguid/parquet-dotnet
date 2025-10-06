using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Encodings;
using Xunit;

namespace Parquet.Test.Encodings {
    public class ByteStreamSplitEncoderTest : TestBase {

        [Fact]
        public void TestApacheExample() {
            /*
            * Example: Original data is three 32-bit floats and for simplicity we look at their raw representation.
            *        Element 0      Element 1      Element 2
            * Bytes  AA BB CC DD    00 11 22 33    A3 B4 C5 D6
            * 
            * After applying the transformation, the data has the following representation:
            * Bytes  AA 00 A3 BB 11 B4 CC 22 C5 DD 33 D6
            */

            float[] expected = [FromHex("AABBCCDD"), FromHex("00112233"), FromHex("A3B4C5D6")];
            byte[] bytes = ToByteArray("AA00A3BB11B4CC22C5DD33D6");

            float[] dest = new float[3];
            ByteStreamSplitEncoder.DecodeByteStreamSplit(bytes, dest, 0, 3);
            for(int i = 0; i < 3; i++) {
                Assert.Equal(expected[i], dest[i]);
            }

            float FromHex(string hex) {
                Span<byte> b = stackalloc byte[4];
                for(int i = 0; i < 4; i++) {
                    b[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
                }
                return BitConverter.ToSingle(b);
            }
            byte[] ToByteArray(string hex) {
                int len = hex.Length / 2;
                byte[] b = new byte[len];
                for(int i = 0; i < len; i++) {
                    b[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
                }
                return b;
            }
        }

        [Theory]
        [InlineData("byte_stream_split_256.parquet")]
        public async Task TestFloatDoubleValues(string parquetFile) {
            /*
             * 256 records, two columns value and fvalue
             * Each row value is index * 1.5.
             */
            using ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false);
            Assert.Equal(1, reader.RowGroupCount);
            IParquetRowGroupReader row = reader.OpenRowGroupReader(0);

            DataColumn doubleCol = await row.ReadColumnAsync(reader.Schema.FindDataField("value"));
            DataColumn floatCol = await row.ReadColumnAsync(reader.Schema.FindDataField("fvalue"));
            Assert.Equal(256, doubleCol.NumValues);
            Assert.Equal(256, floatCol.NumValues);

            for(int i = 0; i < 256; i++) {
                double d = i * 1.5d;
                float f = i * 1.5f;
                Assert.Equal(d, (double)doubleCol.Data.GetValue(i)!, 5);
                Assert.Equal(f, (float)floatCol.Data.GetValue(i)!, 5);
            }
        }

        [Theory]
        [InlineData("bss_with_nulls_double.parquet")]
        [InlineData("bss_with_nulls_float.parquet")]
        public async Task TestNullValues(string parquetFile) {
            /*
             * 5 records, column named floats with values [1.1, null, 3.3, null, 5.5]
             */
            using ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false);
            Assert.Equal(1, reader.RowGroupCount);
            IParquetRowGroupReader row = reader.OpenRowGroupReader(0);

            DataColumn floatsCol = await row.ReadColumnAsync(reader.Schema.FindDataField("floats"));
            Assert.Equal(5, floatsCol.NumValues);
            Assert.Equal(5, floatsCol.Data.Length);
            Assert.Equal(3, floatsCol.DefinedData.Length);

            if(parquetFile.Contains("double")) {
                double[] definedValues = (double[])floatsCol.DefinedData;
                Assert.Equal(1.1, definedValues[0]);
                Assert.Equal(3.3, definedValues[1]);
                Assert.Equal(5.5, definedValues[2]);

                double?[] dataValues = (double?[])floatsCol.Data;
                Assert.Equal(1.1, dataValues[0]);
                Assert.Null(dataValues[1]);
                Assert.Equal(3.3, dataValues[2]);
                Assert.Null(dataValues[3]);
                Assert.Equal(5.5, dataValues[4]);
            } else {
                float[] definedValues = (float[])floatsCol.DefinedData;
                Assert.Equal(1.1f, definedValues[0]);
                Assert.Equal(3.3f, definedValues[1]);
                Assert.Equal(5.5f, definedValues[2]);

                float?[] dataValues = (float?[])floatsCol.Data;
                Assert.Equal(1.1f, dataValues[0]);
                Assert.Null(dataValues[1]);
                Assert.Equal(3.3f, dataValues[2]);
                Assert.Null(dataValues[3]);
                Assert.Equal(5.5f, dataValues[4]);
            }
        }

    }
}
