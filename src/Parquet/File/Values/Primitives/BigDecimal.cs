using System;
using System.Linq;
using System.Numerics;
using Parquet.Meta;

namespace Parquet.File.Values.Primitives {
    /// <summary>
    /// A class that encapsulates BigDecimal like the java class
    /// </summary>
    struct BigDecimal {
        /// <summary>
        /// Contains a Decimal value that is the big integer
        /// </summary>
        public decimal DecimalValue { get; set; }

        public BigInteger UnscaledValue { get; set; }

        /// <summary>
        /// The scale of the decimal value
        /// </summary>
        public int Scale { get; set; }

        /// <summary>
        /// The precision of the decimal value
        /// </summary>
        public int Precision { get; set; }

        public BigDecimal(byte[] data, SchemaElement schema, bool isReversed = false) {
            if(!isReversed) data = data.Reverse().ToArray();

            UnscaledValue = new BigInteger(data);
            Precision = schema.Precision!.Value;
            Scale = schema.Scale!.Value;

            BigInteger scaleMultiplier = BigInteger.Pow(10, Scale);
            decimal ipScaled = (decimal)BigInteger.DivRem(UnscaledValue, scaleMultiplier, out BigInteger fpUnscaled);
            decimal fpScaled = (decimal)fpUnscaled / (decimal)scaleMultiplier;

            DecimalValue = ipScaled + fpScaled;
        }

        public BigDecimal(decimal d, int precision, int scale) {
            DecimalValue = d;
            Precision = precision;
            Scale = scale;

            BigInteger scaleMultiplier = BigInteger.Pow(10, scale);
            BigInteger bscaled = new BigInteger(d);
            decimal scaled = d - (decimal)bscaled;
            decimal unscaled = scaled * (decimal)scaleMultiplier;
            UnscaledValue = (bscaled * scaleMultiplier) + new BigInteger(unscaled);
        }

        /// <summary>
        /// Converts a BigDecimal to a decimal
        /// </summary>
        /// <param name="bd">The BigDecimal value</param>
        public static implicit operator decimal(BigDecimal bd) => bd.DecimalValue;

        /// <summary>
        /// Gets buffer size enough to be able to hold the decimal number of a specific precision
        /// </summary>
        /// <param name="precision">Precision value</param>
        /// <returns>Length in bytes</returns>
        public static int GetBufferSize(int precision) {
            //according to impala source: http://impala.io/doc/html/parquet-common_8h_source.html

            int size;

            switch(precision) {
                case 1:
                case 2:
                    size = 1;
                    break;
                case 3:
                case 4:
                    size = 2;
                    break;
                case 5:
                case 6:
                    size = 3;
                    break;
                case 7:
                case 8:
                case 9:
                    size = 4;
                    break;
                case 10:
                case 11:
                    size = 5;
                    break;
                case 12:
                case 13:
                case 14:
                    size = 6;
                    break;
                case 15:
                case 16:
                    size = 7;
                    break;
                case 17:
                case 18:
                    size = 8;
                    break;
                case 19:
                case 20:
                case 21:
                    size = 9;
                    break;
                case 22:
                case 23:
                    size = 10;
                    break;
                case 24:
                case 25:
                case 26:
                    size = 11;
                    break;
                case 27:
                case 28:
                    size = 12;
                    break;
                case 29:
                case 30:
                case 31:
                    size = 13;
                    break;
                case 32:
                case 33:
                    size = 14;
                    break;
                case 34:
                case 35:
                    size = 15;
                    break;
                case 36:
                case 37:
                case 38:
                    size = 16;
                    break;
                default:
                    size = 16;
                    break;
            }

            return size;
        }

        private byte[] AllocateResult() {
            int size = GetBufferSize(Precision);
            return new byte[size];
        }

        public byte[] GetBytes() {
            /*
             * Java: https://docs.oracle.com/javase/7/docs/api/java/math/BigInteger.html#toByteArray()
             * 
             * Returns a byte array containing the two's-complement representation of this BigInteger. The byte array will be in big-endian byte-order: the most significant byte is in the zeroth element. The array will contain the minimum number of bytes required to represent this BigInteger, including at least one sign bit, which is (ceil((this.bitLength() + 1)/8)). (This representation is compatible with the (byte[]) constructor.)
             * 
             * C#:   https://msdn.microsoft.com/en-us/library/system.numerics.biginteger.tobytearray(v=vs.110).aspx
             * 
             * 
             *  value | C# | Java
             * 
             * -1 | [1111 1111] | [1111 1111] - no difference, so maybe buffer size?
             * 
             */


            byte[] result = AllocateResult();

            byte[] data = UnscaledValue.ToByteArray();
            if(data.Length > result.Length)
                throw new NotSupportedException($"decimal data buffer is {data.Length} but result must fit into {result.Length} bytes");

            Array.Copy(data, result, data.Length);

            //if value is negative fill the remaining bytes with [1111 1111] i.e. negative flag bit (0xFF)
            if(UnscaledValue.Sign == -1) {
                for(int i = data.Length; i < result.Length; i++) {
                    result[i] = 0xFF;
                }
            }

            result = result.Reverse().ToArray();
            return result;
        }
    }
}