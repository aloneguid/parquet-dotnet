namespace Parquet.Encodings {
    using System;

    static partial class BitPackedEncoder {

        #region [ Public Methods ]

        /// <summary>
        /// Encodes exactly 8 values from input span. Unlike pack, checks for boundaries.
        /// </summary>
        /// <returns>Number of bytes written</returns>
        public static int Encode8ValuesLE(Span<int> src, Span<byte> dest, int bitWidth) {

            bool needsTempBuffer = dest.Length < bitWidth;
            int written = bitWidth;

            Span<int> src1;
            Span<byte> dest1;
            if(needsTempBuffer) {
                src1 = new int[8];
                src.CopyTo(src1);
                dest1 = new byte[bitWidth];
                written = dest.Length;
            } else {
                src1 = src;
                dest1 = dest;
            }

            Pack8ValuesLE(src1, dest1, bitWidth);

            if(needsTempBuffer) {
                for(int i = 0; i < bitWidth && i < dest.Length; i++) {
                    dest[i] = dest1[i];
                }
            }

            return written;
        }
    

        /// <summary>
        /// Packs exactly 8 int values using little-endian encoding from src into dest without checking for boundaries.
        /// </summary>
        public static void Pack8ValuesLE(Span<int> src, Span<byte> dest, int bitWidth) {
            if(bitWidth == 0)
                Pack8IntValuesLE0(src, dest);
            else if(bitWidth == 1)
                Pack8IntValuesLE1(src, dest);
            else if(bitWidth == 2)
                Pack8IntValuesLE2(src, dest);
            else if(bitWidth == 3)
                Pack8IntValuesLE3(src, dest);
            else if(bitWidth == 4)
                Pack8IntValuesLE4(src, dest);
            else if(bitWidth == 5)
                Pack8IntValuesLE5(src, dest);
            else if(bitWidth == 6)
                Pack8IntValuesLE6(src, dest);
            else if(bitWidth == 7)
                Pack8IntValuesLE7(src, dest);
            else if(bitWidth == 8)
                Pack8IntValuesLE8(src, dest);
            else if(bitWidth == 9)
                Pack8IntValuesLE9(src, dest);
            else if(bitWidth == 10)
                Pack8IntValuesLE10(src, dest);
            else if(bitWidth == 11)
                Pack8IntValuesLE11(src, dest);
            else if(bitWidth == 12)
                Pack8IntValuesLE12(src, dest);
            else if(bitWidth == 13)
                Pack8IntValuesLE13(src, dest);
            else if(bitWidth == 14)
                Pack8IntValuesLE14(src, dest);
            else if(bitWidth == 15)
                Pack8IntValuesLE15(src, dest);
            else if(bitWidth == 16)
                Pack8IntValuesLE16(src, dest);
            else if(bitWidth == 17)
                Pack8IntValuesLE17(src, dest);
            else if(bitWidth == 18)
                Pack8IntValuesLE18(src, dest);
            else if(bitWidth == 19)
                Pack8IntValuesLE19(src, dest);
            else if(bitWidth == 20)
                Pack8IntValuesLE20(src, dest);
            else if(bitWidth == 21)
                Pack8IntValuesLE21(src, dest);
            else if(bitWidth == 22)
                Pack8IntValuesLE22(src, dest);
            else if(bitWidth == 23)
                Pack8IntValuesLE23(src, dest);
            else if(bitWidth == 24)
                Pack8IntValuesLE24(src, dest);
            else if(bitWidth == 25)
                Pack8IntValuesLE25(src, dest);
            else if(bitWidth == 26)
                Pack8IntValuesLE26(src, dest);
            else if(bitWidth == 27)
                Pack8IntValuesLE27(src, dest);
            else if(bitWidth == 28)
                Pack8IntValuesLE28(src, dest);
            else if(bitWidth == 29)
                Pack8IntValuesLE29(src, dest);
            else if(bitWidth == 30)
                Pack8IntValuesLE30(src, dest);
            else if(bitWidth == 31)
                Pack8IntValuesLE31(src, dest);
            else if(bitWidth == 32)
                Pack8IntValuesLE32(src, dest);
                }

        /// <summary>
        /// Decodes exactly 8 values from input span. Unlike unpack, checks for boundaries.
        /// </summary>
        /// <returns>Number of values unpacked</returns>
        public static int Decode8ValuesLE(Span<byte> src, Span<int> dest, int bitWidth) {

            // we always need at least bitWidth bytes available to decode 8 values
            bool needsTempFuffer = src.Length < bitWidth || dest.Length < 8;
            int decoded = 8;

            Span<byte> src1;
            Span<int> dest1;
            if (needsTempFuffer) {
                src1 = new byte[bitWidth];
                src.CopyTo(src1);
                dest1 = new int[8];
                decoded = Math.Min(src.Length * 8 / bitWidth, dest.Length);
            } else {
                src1 = src;
                dest1 = dest;
            }

            Unpack8ValuesLE(src1, dest1, bitWidth);

            if(needsTempFuffer) {
                for(int i = 0; i < decoded; i++) {
                    dest[i] = dest1[i];
                }
            }

            return decoded;
        }

        /// <summary>
        /// Unpacks exactly 8 int values using little-endian encoding from src into dest without checking for boundaries.
        /// </summary>
        public static void Unpack8ValuesLE(Span<byte> src, Span<int> dest, int bitWidth) {
            if(bitWidth == 0)
                Unpack8IntValuesLE0(src, dest);
            else if(bitWidth == 1)
                Unpack8IntValuesLE1(src, dest);
            else if(bitWidth == 2)
                Unpack8IntValuesLE2(src, dest);
            else if(bitWidth == 3)
                Unpack8IntValuesLE3(src, dest);
            else if(bitWidth == 4)
                Unpack8IntValuesLE4(src, dest);
            else if(bitWidth == 5)
                Unpack8IntValuesLE5(src, dest);
            else if(bitWidth == 6)
                Unpack8IntValuesLE6(src, dest);
            else if(bitWidth == 7)
                Unpack8IntValuesLE7(src, dest);
            else if(bitWidth == 8)
                Unpack8IntValuesLE8(src, dest);
            else if(bitWidth == 9)
                Unpack8IntValuesLE9(src, dest);
            else if(bitWidth == 10)
                Unpack8IntValuesLE10(src, dest);
            else if(bitWidth == 11)
                Unpack8IntValuesLE11(src, dest);
            else if(bitWidth == 12)
                Unpack8IntValuesLE12(src, dest);
            else if(bitWidth == 13)
                Unpack8IntValuesLE13(src, dest);
            else if(bitWidth == 14)
                Unpack8IntValuesLE14(src, dest);
            else if(bitWidth == 15)
                Unpack8IntValuesLE15(src, dest);
            else if(bitWidth == 16)
                Unpack8IntValuesLE16(src, dest);
            else if(bitWidth == 17)
                Unpack8IntValuesLE17(src, dest);
            else if(bitWidth == 18)
                Unpack8IntValuesLE18(src, dest);
            else if(bitWidth == 19)
                Unpack8IntValuesLE19(src, dest);
            else if(bitWidth == 20)
                Unpack8IntValuesLE20(src, dest);
            else if(bitWidth == 21)
                Unpack8IntValuesLE21(src, dest);
            else if(bitWidth == 22)
                Unpack8IntValuesLE22(src, dest);
            else if(bitWidth == 23)
                Unpack8IntValuesLE23(src, dest);
            else if(bitWidth == 24)
                Unpack8IntValuesLE24(src, dest);
            else if(bitWidth == 25)
                Unpack8IntValuesLE25(src, dest);
            else if(bitWidth == 26)
                Unpack8IntValuesLE26(src, dest);
            else if(bitWidth == 27)
                Unpack8IntValuesLE27(src, dest);
            else if(bitWidth == 28)
                Unpack8IntValuesLE28(src, dest);
            else if(bitWidth == 29)
                Unpack8IntValuesLE29(src, dest);
            else if(bitWidth == 30)
                Unpack8IntValuesLE30(src, dest);
            else if(bitWidth == 31)
                Unpack8IntValuesLE31(src, dest);
            else if(bitWidth == 32)
                Unpack8IntValuesLE32(src, dest);
        
        }
    
        /// <summary>
        /// Encodes exactly 8 values from input span. Unlike pack, checks for boundaries.
        /// </summary>
        /// <returns>Number of bytes written</returns>
        public static int Encode8ValuesBE(Span<int> src, Span<byte> dest, int bitWidth) {

            bool needsTempBuffer = dest.Length < bitWidth;
            int written = bitWidth;

            Span<int> src1;
            Span<byte> dest1;
            if(needsTempBuffer) {
                src1 = new int[8];
                src.CopyTo(src1);
                dest1 = new byte[bitWidth];
                written = dest.Length;
            } else {
                src1 = src;
                dest1 = dest;
            }

            Pack8ValuesBE(src1, dest1, bitWidth);

            if(needsTempBuffer) {
                for(int i = 0; i < bitWidth && i < dest.Length; i++) {
                    dest[i] = dest1[i];
                }
            }

            return written;
        }
    

        /// <summary>
        /// Packs exactly 8 int values using big-endian encoding from src into dest without checking for boundaries.
        /// </summary>
        public static void Pack8ValuesBE(Span<int> src, Span<byte> dest, int bitWidth) {
            if(bitWidth == 0)
                Pack8IntValuesBE0(src, dest);
            else if(bitWidth == 1)
                Pack8IntValuesBE1(src, dest);
            else if(bitWidth == 2)
                Pack8IntValuesBE2(src, dest);
            else if(bitWidth == 3)
                Pack8IntValuesBE3(src, dest);
            else if(bitWidth == 4)
                Pack8IntValuesBE4(src, dest);
            else if(bitWidth == 5)
                Pack8IntValuesBE5(src, dest);
            else if(bitWidth == 6)
                Pack8IntValuesBE6(src, dest);
            else if(bitWidth == 7)
                Pack8IntValuesBE7(src, dest);
            else if(bitWidth == 8)
                Pack8IntValuesBE8(src, dest);
            else if(bitWidth == 9)
                Pack8IntValuesBE9(src, dest);
            else if(bitWidth == 10)
                Pack8IntValuesBE10(src, dest);
            else if(bitWidth == 11)
                Pack8IntValuesBE11(src, dest);
            else if(bitWidth == 12)
                Pack8IntValuesBE12(src, dest);
            else if(bitWidth == 13)
                Pack8IntValuesBE13(src, dest);
            else if(bitWidth == 14)
                Pack8IntValuesBE14(src, dest);
            else if(bitWidth == 15)
                Pack8IntValuesBE15(src, dest);
            else if(bitWidth == 16)
                Pack8IntValuesBE16(src, dest);
            else if(bitWidth == 17)
                Pack8IntValuesBE17(src, dest);
            else if(bitWidth == 18)
                Pack8IntValuesBE18(src, dest);
            else if(bitWidth == 19)
                Pack8IntValuesBE19(src, dest);
            else if(bitWidth == 20)
                Pack8IntValuesBE20(src, dest);
            else if(bitWidth == 21)
                Pack8IntValuesBE21(src, dest);
            else if(bitWidth == 22)
                Pack8IntValuesBE22(src, dest);
            else if(bitWidth == 23)
                Pack8IntValuesBE23(src, dest);
            else if(bitWidth == 24)
                Pack8IntValuesBE24(src, dest);
            else if(bitWidth == 25)
                Pack8IntValuesBE25(src, dest);
            else if(bitWidth == 26)
                Pack8IntValuesBE26(src, dest);
            else if(bitWidth == 27)
                Pack8IntValuesBE27(src, dest);
            else if(bitWidth == 28)
                Pack8IntValuesBE28(src, dest);
            else if(bitWidth == 29)
                Pack8IntValuesBE29(src, dest);
            else if(bitWidth == 30)
                Pack8IntValuesBE30(src, dest);
            else if(bitWidth == 31)
                Pack8IntValuesBE31(src, dest);
            else if(bitWidth == 32)
                Pack8IntValuesBE32(src, dest);
                }

        /// <summary>
        /// Decodes exactly 8 values from input span. Unlike unpack, checks for boundaries.
        /// </summary>
        /// <returns>Number of values unpacked</returns>
        public static int Decode8ValuesBE(Span<byte> src, Span<int> dest, int bitWidth) {

            // we always need at least bitWidth bytes available to decode 8 values
            bool needsTempFuffer = src.Length < bitWidth || dest.Length < 8;
            int decoded = 8;

            Span<byte> src1;
            Span<int> dest1;
            if (needsTempFuffer) {
                src1 = new byte[bitWidth];
                src.CopyTo(src1);
                dest1 = new int[8];
                decoded = Math.Min(src.Length * 8 / bitWidth, dest.Length);
            } else {
                src1 = src;
                dest1 = dest;
            }

            Unpack8ValuesBE(src1, dest1, bitWidth);

            if(needsTempFuffer) {
                for(int i = 0; i < decoded; i++) {
                    dest[i] = dest1[i];
                }
            }

            return decoded;
        }

        /// <summary>
        /// Unpacks exactly 8 int values using big-endian encoding from src into dest without checking for boundaries.
        /// </summary>
        public static void Unpack8ValuesBE(Span<byte> src, Span<int> dest, int bitWidth) {
            if(bitWidth == 0)
                Unpack8IntValuesBE0(src, dest);
            else if(bitWidth == 1)
                Unpack8IntValuesBE1(src, dest);
            else if(bitWidth == 2)
                Unpack8IntValuesBE2(src, dest);
            else if(bitWidth == 3)
                Unpack8IntValuesBE3(src, dest);
            else if(bitWidth == 4)
                Unpack8IntValuesBE4(src, dest);
            else if(bitWidth == 5)
                Unpack8IntValuesBE5(src, dest);
            else if(bitWidth == 6)
                Unpack8IntValuesBE6(src, dest);
            else if(bitWidth == 7)
                Unpack8IntValuesBE7(src, dest);
            else if(bitWidth == 8)
                Unpack8IntValuesBE8(src, dest);
            else if(bitWidth == 9)
                Unpack8IntValuesBE9(src, dest);
            else if(bitWidth == 10)
                Unpack8IntValuesBE10(src, dest);
            else if(bitWidth == 11)
                Unpack8IntValuesBE11(src, dest);
            else if(bitWidth == 12)
                Unpack8IntValuesBE12(src, dest);
            else if(bitWidth == 13)
                Unpack8IntValuesBE13(src, dest);
            else if(bitWidth == 14)
                Unpack8IntValuesBE14(src, dest);
            else if(bitWidth == 15)
                Unpack8IntValuesBE15(src, dest);
            else if(bitWidth == 16)
                Unpack8IntValuesBE16(src, dest);
            else if(bitWidth == 17)
                Unpack8IntValuesBE17(src, dest);
            else if(bitWidth == 18)
                Unpack8IntValuesBE18(src, dest);
            else if(bitWidth == 19)
                Unpack8IntValuesBE19(src, dest);
            else if(bitWidth == 20)
                Unpack8IntValuesBE20(src, dest);
            else if(bitWidth == 21)
                Unpack8IntValuesBE21(src, dest);
            else if(bitWidth == 22)
                Unpack8IntValuesBE22(src, dest);
            else if(bitWidth == 23)
                Unpack8IntValuesBE23(src, dest);
            else if(bitWidth == 24)
                Unpack8IntValuesBE24(src, dest);
            else if(bitWidth == 25)
                Unpack8IntValuesBE25(src, dest);
            else if(bitWidth == 26)
                Unpack8IntValuesBE26(src, dest);
            else if(bitWidth == 27)
                Unpack8IntValuesBE27(src, dest);
            else if(bitWidth == 28)
                Unpack8IntValuesBE28(src, dest);
            else if(bitWidth == 29)
                Unpack8IntValuesBE29(src, dest);
            else if(bitWidth == 30)
                Unpack8IntValuesBE30(src, dest);
            else if(bitWidth == 31)
                Unpack8IntValuesBE31(src, dest);
            else if(bitWidth == 32)
                Unpack8IntValuesBE32(src, dest);
        
        }
    
        /// <summary>
        /// Encodes exactly 8 values from input span. Unlike pack, checks for boundaries.
        /// </summary>
        /// <returns>Number of bytes written</returns>
        public static int Encode8ValuesLE(Span<long> src, Span<byte> dest, int bitWidth) {

            bool needsTempBuffer = dest.Length < bitWidth;
            int written = bitWidth;

            Span<long> src1;
            Span<byte> dest1;
            if(needsTempBuffer) {
                src1 = new long[8];
                src.CopyTo(src1);
                dest1 = new byte[bitWidth];
                written = dest.Length;
            } else {
                src1 = src;
                dest1 = dest;
            }

            Pack8ValuesLE(src1, dest1, bitWidth);

            if(needsTempBuffer) {
                for(int i = 0; i < bitWidth && i < dest.Length; i++) {
                    dest[i] = dest1[i];
                }
            }

            return written;
        }
    

        /// <summary>
        /// Packs exactly 8 long values using little-endian encoding from src into dest without checking for boundaries.
        /// </summary>
        public static void Pack8ValuesLE(Span<long> src, Span<byte> dest, int bitWidth) {
            if(bitWidth == 0)
                Pack8LongValuesLE0(src, dest);
            else if(bitWidth == 1)
                Pack8LongValuesLE1(src, dest);
            else if(bitWidth == 2)
                Pack8LongValuesLE2(src, dest);
            else if(bitWidth == 3)
                Pack8LongValuesLE3(src, dest);
            else if(bitWidth == 4)
                Pack8LongValuesLE4(src, dest);
            else if(bitWidth == 5)
                Pack8LongValuesLE5(src, dest);
            else if(bitWidth == 6)
                Pack8LongValuesLE6(src, dest);
            else if(bitWidth == 7)
                Pack8LongValuesLE7(src, dest);
            else if(bitWidth == 8)
                Pack8LongValuesLE8(src, dest);
            else if(bitWidth == 9)
                Pack8LongValuesLE9(src, dest);
            else if(bitWidth == 10)
                Pack8LongValuesLE10(src, dest);
            else if(bitWidth == 11)
                Pack8LongValuesLE11(src, dest);
            else if(bitWidth == 12)
                Pack8LongValuesLE12(src, dest);
            else if(bitWidth == 13)
                Pack8LongValuesLE13(src, dest);
            else if(bitWidth == 14)
                Pack8LongValuesLE14(src, dest);
            else if(bitWidth == 15)
                Pack8LongValuesLE15(src, dest);
            else if(bitWidth == 16)
                Pack8LongValuesLE16(src, dest);
            else if(bitWidth == 17)
                Pack8LongValuesLE17(src, dest);
            else if(bitWidth == 18)
                Pack8LongValuesLE18(src, dest);
            else if(bitWidth == 19)
                Pack8LongValuesLE19(src, dest);
            else if(bitWidth == 20)
                Pack8LongValuesLE20(src, dest);
            else if(bitWidth == 21)
                Pack8LongValuesLE21(src, dest);
            else if(bitWidth == 22)
                Pack8LongValuesLE22(src, dest);
            else if(bitWidth == 23)
                Pack8LongValuesLE23(src, dest);
            else if(bitWidth == 24)
                Pack8LongValuesLE24(src, dest);
            else if(bitWidth == 25)
                Pack8LongValuesLE25(src, dest);
            else if(bitWidth == 26)
                Pack8LongValuesLE26(src, dest);
            else if(bitWidth == 27)
                Pack8LongValuesLE27(src, dest);
            else if(bitWidth == 28)
                Pack8LongValuesLE28(src, dest);
            else if(bitWidth == 29)
                Pack8LongValuesLE29(src, dest);
            else if(bitWidth == 30)
                Pack8LongValuesLE30(src, dest);
            else if(bitWidth == 31)
                Pack8LongValuesLE31(src, dest);
            else if(bitWidth == 32)
                Pack8LongValuesLE32(src, dest);
            else if(bitWidth == 33)
                Pack8LongValuesLE33(src, dest);
            else if(bitWidth == 34)
                Pack8LongValuesLE34(src, dest);
            else if(bitWidth == 35)
                Pack8LongValuesLE35(src, dest);
            else if(bitWidth == 36)
                Pack8LongValuesLE36(src, dest);
            else if(bitWidth == 37)
                Pack8LongValuesLE37(src, dest);
            else if(bitWidth == 38)
                Pack8LongValuesLE38(src, dest);
            else if(bitWidth == 39)
                Pack8LongValuesLE39(src, dest);
            else if(bitWidth == 40)
                Pack8LongValuesLE40(src, dest);
            else if(bitWidth == 41)
                Pack8LongValuesLE41(src, dest);
            else if(bitWidth == 42)
                Pack8LongValuesLE42(src, dest);
            else if(bitWidth == 43)
                Pack8LongValuesLE43(src, dest);
            else if(bitWidth == 44)
                Pack8LongValuesLE44(src, dest);
            else if(bitWidth == 45)
                Pack8LongValuesLE45(src, dest);
            else if(bitWidth == 46)
                Pack8LongValuesLE46(src, dest);
            else if(bitWidth == 47)
                Pack8LongValuesLE47(src, dest);
            else if(bitWidth == 48)
                Pack8LongValuesLE48(src, dest);
            else if(bitWidth == 49)
                Pack8LongValuesLE49(src, dest);
            else if(bitWidth == 50)
                Pack8LongValuesLE50(src, dest);
            else if(bitWidth == 51)
                Pack8LongValuesLE51(src, dest);
            else if(bitWidth == 52)
                Pack8LongValuesLE52(src, dest);
            else if(bitWidth == 53)
                Pack8LongValuesLE53(src, dest);
            else if(bitWidth == 54)
                Pack8LongValuesLE54(src, dest);
            else if(bitWidth == 55)
                Pack8LongValuesLE55(src, dest);
            else if(bitWidth == 56)
                Pack8LongValuesLE56(src, dest);
            else if(bitWidth == 57)
                Pack8LongValuesLE57(src, dest);
            else if(bitWidth == 58)
                Pack8LongValuesLE58(src, dest);
            else if(bitWidth == 59)
                Pack8LongValuesLE59(src, dest);
            else if(bitWidth == 60)
                Pack8LongValuesLE60(src, dest);
            else if(bitWidth == 61)
                Pack8LongValuesLE61(src, dest);
            else if(bitWidth == 62)
                Pack8LongValuesLE62(src, dest);
            else if(bitWidth == 63)
                Pack8LongValuesLE63(src, dest);
            else if(bitWidth == 64)
                Pack8LongValuesLE64(src, dest);
                }

        /// <summary>
        /// Decodes exactly 8 values from input span. Unlike unpack, checks for boundaries.
        /// </summary>
        /// <returns>Number of values unpacked</returns>
        public static int Decode8ValuesLE(Span<byte> src, Span<long> dest, int bitWidth) {

            // we always need at least bitWidth bytes available to decode 8 values
            bool needsTempFuffer = src.Length < bitWidth || dest.Length < 8;
            int decoded = 8;

            Span<byte> src1;
            Span<long> dest1;
            if (needsTempFuffer) {
                src1 = new byte[bitWidth];
                src.CopyTo(src1);
                dest1 = new long[8];
                decoded = Math.Min(src.Length * 8 / bitWidth, dest.Length);
            } else {
                src1 = src;
                dest1 = dest;
            }

            Unpack8ValuesLE(src1, dest1, bitWidth);

            if(needsTempFuffer) {
                for(int i = 0; i < decoded; i++) {
                    dest[i] = dest1[i];
                }
            }

            return decoded;
        }

        /// <summary>
        /// Unpacks exactly 8 long values using little-endian encoding from src into dest without checking for boundaries.
        /// </summary>
        public static void Unpack8ValuesLE(Span<byte> src, Span<long> dest, int bitWidth) {
            if(bitWidth == 0)
                Unpack8LongValuesLE0(src, dest);
            else if(bitWidth == 1)
                Unpack8LongValuesLE1(src, dest);
            else if(bitWidth == 2)
                Unpack8LongValuesLE2(src, dest);
            else if(bitWidth == 3)
                Unpack8LongValuesLE3(src, dest);
            else if(bitWidth == 4)
                Unpack8LongValuesLE4(src, dest);
            else if(bitWidth == 5)
                Unpack8LongValuesLE5(src, dest);
            else if(bitWidth == 6)
                Unpack8LongValuesLE6(src, dest);
            else if(bitWidth == 7)
                Unpack8LongValuesLE7(src, dest);
            else if(bitWidth == 8)
                Unpack8LongValuesLE8(src, dest);
            else if(bitWidth == 9)
                Unpack8LongValuesLE9(src, dest);
            else if(bitWidth == 10)
                Unpack8LongValuesLE10(src, dest);
            else if(bitWidth == 11)
                Unpack8LongValuesLE11(src, dest);
            else if(bitWidth == 12)
                Unpack8LongValuesLE12(src, dest);
            else if(bitWidth == 13)
                Unpack8LongValuesLE13(src, dest);
            else if(bitWidth == 14)
                Unpack8LongValuesLE14(src, dest);
            else if(bitWidth == 15)
                Unpack8LongValuesLE15(src, dest);
            else if(bitWidth == 16)
                Unpack8LongValuesLE16(src, dest);
            else if(bitWidth == 17)
                Unpack8LongValuesLE17(src, dest);
            else if(bitWidth == 18)
                Unpack8LongValuesLE18(src, dest);
            else if(bitWidth == 19)
                Unpack8LongValuesLE19(src, dest);
            else if(bitWidth == 20)
                Unpack8LongValuesLE20(src, dest);
            else if(bitWidth == 21)
                Unpack8LongValuesLE21(src, dest);
            else if(bitWidth == 22)
                Unpack8LongValuesLE22(src, dest);
            else if(bitWidth == 23)
                Unpack8LongValuesLE23(src, dest);
            else if(bitWidth == 24)
                Unpack8LongValuesLE24(src, dest);
            else if(bitWidth == 25)
                Unpack8LongValuesLE25(src, dest);
            else if(bitWidth == 26)
                Unpack8LongValuesLE26(src, dest);
            else if(bitWidth == 27)
                Unpack8LongValuesLE27(src, dest);
            else if(bitWidth == 28)
                Unpack8LongValuesLE28(src, dest);
            else if(bitWidth == 29)
                Unpack8LongValuesLE29(src, dest);
            else if(bitWidth == 30)
                Unpack8LongValuesLE30(src, dest);
            else if(bitWidth == 31)
                Unpack8LongValuesLE31(src, dest);
            else if(bitWidth == 32)
                Unpack8LongValuesLE32(src, dest);
            else if(bitWidth == 33)
                Unpack8LongValuesLE33(src, dest);
            else if(bitWidth == 34)
                Unpack8LongValuesLE34(src, dest);
            else if(bitWidth == 35)
                Unpack8LongValuesLE35(src, dest);
            else if(bitWidth == 36)
                Unpack8LongValuesLE36(src, dest);
            else if(bitWidth == 37)
                Unpack8LongValuesLE37(src, dest);
            else if(bitWidth == 38)
                Unpack8LongValuesLE38(src, dest);
            else if(bitWidth == 39)
                Unpack8LongValuesLE39(src, dest);
            else if(bitWidth == 40)
                Unpack8LongValuesLE40(src, dest);
            else if(bitWidth == 41)
                Unpack8LongValuesLE41(src, dest);
            else if(bitWidth == 42)
                Unpack8LongValuesLE42(src, dest);
            else if(bitWidth == 43)
                Unpack8LongValuesLE43(src, dest);
            else if(bitWidth == 44)
                Unpack8LongValuesLE44(src, dest);
            else if(bitWidth == 45)
                Unpack8LongValuesLE45(src, dest);
            else if(bitWidth == 46)
                Unpack8LongValuesLE46(src, dest);
            else if(bitWidth == 47)
                Unpack8LongValuesLE47(src, dest);
            else if(bitWidth == 48)
                Unpack8LongValuesLE48(src, dest);
            else if(bitWidth == 49)
                Unpack8LongValuesLE49(src, dest);
            else if(bitWidth == 50)
                Unpack8LongValuesLE50(src, dest);
            else if(bitWidth == 51)
                Unpack8LongValuesLE51(src, dest);
            else if(bitWidth == 52)
                Unpack8LongValuesLE52(src, dest);
            else if(bitWidth == 53)
                Unpack8LongValuesLE53(src, dest);
            else if(bitWidth == 54)
                Unpack8LongValuesLE54(src, dest);
            else if(bitWidth == 55)
                Unpack8LongValuesLE55(src, dest);
            else if(bitWidth == 56)
                Unpack8LongValuesLE56(src, dest);
            else if(bitWidth == 57)
                Unpack8LongValuesLE57(src, dest);
            else if(bitWidth == 58)
                Unpack8LongValuesLE58(src, dest);
            else if(bitWidth == 59)
                Unpack8LongValuesLE59(src, dest);
            else if(bitWidth == 60)
                Unpack8LongValuesLE60(src, dest);
            else if(bitWidth == 61)
                Unpack8LongValuesLE61(src, dest);
            else if(bitWidth == 62)
                Unpack8LongValuesLE62(src, dest);
            else if(bitWidth == 63)
                Unpack8LongValuesLE63(src, dest);
            else if(bitWidth == 64)
                Unpack8LongValuesLE64(src, dest);
        
        }
    
        /// <summary>
        /// Encodes exactly 8 values from input span. Unlike pack, checks for boundaries.
        /// </summary>
        /// <returns>Number of bytes written</returns>
        public static int Encode8ValuesBE(Span<long> src, Span<byte> dest, int bitWidth) {

            bool needsTempBuffer = dest.Length < bitWidth;
            int written = bitWidth;

            Span<long> src1;
            Span<byte> dest1;
            if(needsTempBuffer) {
                src1 = new long[8];
                src.CopyTo(src1);
                dest1 = new byte[bitWidth];
                written = dest.Length;
            } else {
                src1 = src;
                dest1 = dest;
            }

            Pack8ValuesBE(src1, dest1, bitWidth);

            if(needsTempBuffer) {
                for(int i = 0; i < bitWidth && i < dest.Length; i++) {
                    dest[i] = dest1[i];
                }
            }

            return written;
        }
    

        /// <summary>
        /// Packs exactly 8 long values using big-endian encoding from src into dest without checking for boundaries.
        /// </summary>
        public static void Pack8ValuesBE(Span<long> src, Span<byte> dest, int bitWidth) {
            if(bitWidth == 0)
                Pack8LongValuesBE0(src, dest);
            else if(bitWidth == 1)
                Pack8LongValuesBE1(src, dest);
            else if(bitWidth == 2)
                Pack8LongValuesBE2(src, dest);
            else if(bitWidth == 3)
                Pack8LongValuesBE3(src, dest);
            else if(bitWidth == 4)
                Pack8LongValuesBE4(src, dest);
            else if(bitWidth == 5)
                Pack8LongValuesBE5(src, dest);
            else if(bitWidth == 6)
                Pack8LongValuesBE6(src, dest);
            else if(bitWidth == 7)
                Pack8LongValuesBE7(src, dest);
            else if(bitWidth == 8)
                Pack8LongValuesBE8(src, dest);
            else if(bitWidth == 9)
                Pack8LongValuesBE9(src, dest);
            else if(bitWidth == 10)
                Pack8LongValuesBE10(src, dest);
            else if(bitWidth == 11)
                Pack8LongValuesBE11(src, dest);
            else if(bitWidth == 12)
                Pack8LongValuesBE12(src, dest);
            else if(bitWidth == 13)
                Pack8LongValuesBE13(src, dest);
            else if(bitWidth == 14)
                Pack8LongValuesBE14(src, dest);
            else if(bitWidth == 15)
                Pack8LongValuesBE15(src, dest);
            else if(bitWidth == 16)
                Pack8LongValuesBE16(src, dest);
            else if(bitWidth == 17)
                Pack8LongValuesBE17(src, dest);
            else if(bitWidth == 18)
                Pack8LongValuesBE18(src, dest);
            else if(bitWidth == 19)
                Pack8LongValuesBE19(src, dest);
            else if(bitWidth == 20)
                Pack8LongValuesBE20(src, dest);
            else if(bitWidth == 21)
                Pack8LongValuesBE21(src, dest);
            else if(bitWidth == 22)
                Pack8LongValuesBE22(src, dest);
            else if(bitWidth == 23)
                Pack8LongValuesBE23(src, dest);
            else if(bitWidth == 24)
                Pack8LongValuesBE24(src, dest);
            else if(bitWidth == 25)
                Pack8LongValuesBE25(src, dest);
            else if(bitWidth == 26)
                Pack8LongValuesBE26(src, dest);
            else if(bitWidth == 27)
                Pack8LongValuesBE27(src, dest);
            else if(bitWidth == 28)
                Pack8LongValuesBE28(src, dest);
            else if(bitWidth == 29)
                Pack8LongValuesBE29(src, dest);
            else if(bitWidth == 30)
                Pack8LongValuesBE30(src, dest);
            else if(bitWidth == 31)
                Pack8LongValuesBE31(src, dest);
            else if(bitWidth == 32)
                Pack8LongValuesBE32(src, dest);
            else if(bitWidth == 33)
                Pack8LongValuesBE33(src, dest);
            else if(bitWidth == 34)
                Pack8LongValuesBE34(src, dest);
            else if(bitWidth == 35)
                Pack8LongValuesBE35(src, dest);
            else if(bitWidth == 36)
                Pack8LongValuesBE36(src, dest);
            else if(bitWidth == 37)
                Pack8LongValuesBE37(src, dest);
            else if(bitWidth == 38)
                Pack8LongValuesBE38(src, dest);
            else if(bitWidth == 39)
                Pack8LongValuesBE39(src, dest);
            else if(bitWidth == 40)
                Pack8LongValuesBE40(src, dest);
            else if(bitWidth == 41)
                Pack8LongValuesBE41(src, dest);
            else if(bitWidth == 42)
                Pack8LongValuesBE42(src, dest);
            else if(bitWidth == 43)
                Pack8LongValuesBE43(src, dest);
            else if(bitWidth == 44)
                Pack8LongValuesBE44(src, dest);
            else if(bitWidth == 45)
                Pack8LongValuesBE45(src, dest);
            else if(bitWidth == 46)
                Pack8LongValuesBE46(src, dest);
            else if(bitWidth == 47)
                Pack8LongValuesBE47(src, dest);
            else if(bitWidth == 48)
                Pack8LongValuesBE48(src, dest);
            else if(bitWidth == 49)
                Pack8LongValuesBE49(src, dest);
            else if(bitWidth == 50)
                Pack8LongValuesBE50(src, dest);
            else if(bitWidth == 51)
                Pack8LongValuesBE51(src, dest);
            else if(bitWidth == 52)
                Pack8LongValuesBE52(src, dest);
            else if(bitWidth == 53)
                Pack8LongValuesBE53(src, dest);
            else if(bitWidth == 54)
                Pack8LongValuesBE54(src, dest);
            else if(bitWidth == 55)
                Pack8LongValuesBE55(src, dest);
            else if(bitWidth == 56)
                Pack8LongValuesBE56(src, dest);
            else if(bitWidth == 57)
                Pack8LongValuesBE57(src, dest);
            else if(bitWidth == 58)
                Pack8LongValuesBE58(src, dest);
            else if(bitWidth == 59)
                Pack8LongValuesBE59(src, dest);
            else if(bitWidth == 60)
                Pack8LongValuesBE60(src, dest);
            else if(bitWidth == 61)
                Pack8LongValuesBE61(src, dest);
            else if(bitWidth == 62)
                Pack8LongValuesBE62(src, dest);
            else if(bitWidth == 63)
                Pack8LongValuesBE63(src, dest);
            else if(bitWidth == 64)
                Pack8LongValuesBE64(src, dest);
                }

        /// <summary>
        /// Decodes exactly 8 values from input span. Unlike unpack, checks for boundaries.
        /// </summary>
        /// <returns>Number of values unpacked</returns>
        public static int Decode8ValuesBE(Span<byte> src, Span<long> dest, int bitWidth) {

            // we always need at least bitWidth bytes available to decode 8 values
            bool needsTempFuffer = src.Length < bitWidth || dest.Length < 8;
            int decoded = 8;

            Span<byte> src1;
            Span<long> dest1;
            if (needsTempFuffer) {
                src1 = new byte[bitWidth];
                src.CopyTo(src1);
                dest1 = new long[8];
                decoded = Math.Min(src.Length * 8 / bitWidth, dest.Length);
            } else {
                src1 = src;
                dest1 = dest;
            }

            Unpack8ValuesBE(src1, dest1, bitWidth);

            if(needsTempFuffer) {
                for(int i = 0; i < decoded; i++) {
                    dest[i] = dest1[i];
                }
            }

            return decoded;
        }

        /// <summary>
        /// Unpacks exactly 8 long values using big-endian encoding from src into dest without checking for boundaries.
        /// </summary>
        public static void Unpack8ValuesBE(Span<byte> src, Span<long> dest, int bitWidth) {
            if(bitWidth == 0)
                Unpack8LongValuesBE0(src, dest);
            else if(bitWidth == 1)
                Unpack8LongValuesBE1(src, dest);
            else if(bitWidth == 2)
                Unpack8LongValuesBE2(src, dest);
            else if(bitWidth == 3)
                Unpack8LongValuesBE3(src, dest);
            else if(bitWidth == 4)
                Unpack8LongValuesBE4(src, dest);
            else if(bitWidth == 5)
                Unpack8LongValuesBE5(src, dest);
            else if(bitWidth == 6)
                Unpack8LongValuesBE6(src, dest);
            else if(bitWidth == 7)
                Unpack8LongValuesBE7(src, dest);
            else if(bitWidth == 8)
                Unpack8LongValuesBE8(src, dest);
            else if(bitWidth == 9)
                Unpack8LongValuesBE9(src, dest);
            else if(bitWidth == 10)
                Unpack8LongValuesBE10(src, dest);
            else if(bitWidth == 11)
                Unpack8LongValuesBE11(src, dest);
            else if(bitWidth == 12)
                Unpack8LongValuesBE12(src, dest);
            else if(bitWidth == 13)
                Unpack8LongValuesBE13(src, dest);
            else if(bitWidth == 14)
                Unpack8LongValuesBE14(src, dest);
            else if(bitWidth == 15)
                Unpack8LongValuesBE15(src, dest);
            else if(bitWidth == 16)
                Unpack8LongValuesBE16(src, dest);
            else if(bitWidth == 17)
                Unpack8LongValuesBE17(src, dest);
            else if(bitWidth == 18)
                Unpack8LongValuesBE18(src, dest);
            else if(bitWidth == 19)
                Unpack8LongValuesBE19(src, dest);
            else if(bitWidth == 20)
                Unpack8LongValuesBE20(src, dest);
            else if(bitWidth == 21)
                Unpack8LongValuesBE21(src, dest);
            else if(bitWidth == 22)
                Unpack8LongValuesBE22(src, dest);
            else if(bitWidth == 23)
                Unpack8LongValuesBE23(src, dest);
            else if(bitWidth == 24)
                Unpack8LongValuesBE24(src, dest);
            else if(bitWidth == 25)
                Unpack8LongValuesBE25(src, dest);
            else if(bitWidth == 26)
                Unpack8LongValuesBE26(src, dest);
            else if(bitWidth == 27)
                Unpack8LongValuesBE27(src, dest);
            else if(bitWidth == 28)
                Unpack8LongValuesBE28(src, dest);
            else if(bitWidth == 29)
                Unpack8LongValuesBE29(src, dest);
            else if(bitWidth == 30)
                Unpack8LongValuesBE30(src, dest);
            else if(bitWidth == 31)
                Unpack8LongValuesBE31(src, dest);
            else if(bitWidth == 32)
                Unpack8LongValuesBE32(src, dest);
            else if(bitWidth == 33)
                Unpack8LongValuesBE33(src, dest);
            else if(bitWidth == 34)
                Unpack8LongValuesBE34(src, dest);
            else if(bitWidth == 35)
                Unpack8LongValuesBE35(src, dest);
            else if(bitWidth == 36)
                Unpack8LongValuesBE36(src, dest);
            else if(bitWidth == 37)
                Unpack8LongValuesBE37(src, dest);
            else if(bitWidth == 38)
                Unpack8LongValuesBE38(src, dest);
            else if(bitWidth == 39)
                Unpack8LongValuesBE39(src, dest);
            else if(bitWidth == 40)
                Unpack8LongValuesBE40(src, dest);
            else if(bitWidth == 41)
                Unpack8LongValuesBE41(src, dest);
            else if(bitWidth == 42)
                Unpack8LongValuesBE42(src, dest);
            else if(bitWidth == 43)
                Unpack8LongValuesBE43(src, dest);
            else if(bitWidth == 44)
                Unpack8LongValuesBE44(src, dest);
            else if(bitWidth == 45)
                Unpack8LongValuesBE45(src, dest);
            else if(bitWidth == 46)
                Unpack8LongValuesBE46(src, dest);
            else if(bitWidth == 47)
                Unpack8LongValuesBE47(src, dest);
            else if(bitWidth == 48)
                Unpack8LongValuesBE48(src, dest);
            else if(bitWidth == 49)
                Unpack8LongValuesBE49(src, dest);
            else if(bitWidth == 50)
                Unpack8LongValuesBE50(src, dest);
            else if(bitWidth == 51)
                Unpack8LongValuesBE51(src, dest);
            else if(bitWidth == 52)
                Unpack8LongValuesBE52(src, dest);
            else if(bitWidth == 53)
                Unpack8LongValuesBE53(src, dest);
            else if(bitWidth == 54)
                Unpack8LongValuesBE54(src, dest);
            else if(bitWidth == 55)
                Unpack8LongValuesBE55(src, dest);
            else if(bitWidth == 56)
                Unpack8LongValuesBE56(src, dest);
            else if(bitWidth == 57)
                Unpack8LongValuesBE57(src, dest);
            else if(bitWidth == 58)
                Unpack8LongValuesBE58(src, dest);
            else if(bitWidth == 59)
                Unpack8LongValuesBE59(src, dest);
            else if(bitWidth == 60)
                Unpack8LongValuesBE60(src, dest);
            else if(bitWidth == 61)
                Unpack8LongValuesBE61(src, dest);
            else if(bitWidth == 62)
                Unpack8LongValuesBE62(src, dest);
            else if(bitWidth == 63)
                Unpack8LongValuesBE63(src, dest);
            else if(bitWidth == 64)
                Unpack8LongValuesBE64(src, dest);
        
        }
            #endregion

        #region [ Precompiled methods per bit width ]

        private static void Unpack8IntValuesLE0(Span<byte> src, Span<int> dest) {
                 }

        private static void Pack8IntValuesLE0(Span<int> src, Span<byte> dest) {
                }
        private static void Unpack8IntValuesBE0(Span<byte> src, Span<int> dest) {
                 }

        private static void Pack8IntValuesBE0(Span<int> src, Span<byte> dest) {
                }
        private static void Unpack8IntValuesLE1(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 1);            dest[1] = ((((int)src[0]) >> 1) & 1);            dest[2] = ((((int)src[0]) >> 2) & 1);            dest[3] = ((((int)src[0]) >> 3) & 1);            dest[4] = ((((int)src[0]) >> 4) & 1);            dest[5] = ((((int)src[0]) >> 5) & 1);            dest[6] = ((((int)src[0]) >> 6) & 1);            dest[7] = ((((int)src[0]) >> 7) & 1);        }

        private static void Pack8IntValuesLE1(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1))                | ((src[1] & 1) << 1)                | ((src[2] & 1) << 2)                | ((src[3] & 1) << 3)                | ((src[4] & 1) << 4)                | ((src[5] & 1) << 5)                | ((src[6] & 1) << 6)                | ((src[7] & 1) << 7)) & 255);
                        }
        private static void Unpack8IntValuesBE1(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) >> 7) & 1);            dest[1] = ((((int)src[0]) >> 6) & 1);            dest[2] = ((((int)src[0]) >> 5) & 1);            dest[3] = ((((int)src[0]) >> 4) & 1);            dest[4] = ((((int)src[0]) >> 3) & 1);            dest[5] = ((((int)src[0]) >> 2) & 1);            dest[6] = ((((int)src[0]) >> 1) & 1);            dest[7] = ((((int)src[0])) & 1);        }

        private static void Pack8IntValuesBE1(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1) << 7)                | ((src[1] & 1) << 6)                | ((src[2] & 1) << 5)                | ((src[3] & 1) << 4)                | ((src[4] & 1) << 3)                | ((src[5] & 1) << 2)                | ((src[6] & 1) << 1)                | ((src[7] & 1))) & 255);
                        }
        private static void Unpack8IntValuesLE2(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 3);            dest[1] = ((((int)src[0]) >> 2) & 3);            dest[2] = ((((int)src[0]) >> 4) & 3);            dest[3] = ((((int)src[0]) >> 6) & 3);            dest[4] = ((((int)src[1])) & 3);            dest[5] = ((((int)src[1]) >> 2) & 3);            dest[6] = ((((int)src[1]) >> 4) & 3);            dest[7] = ((((int)src[1]) >> 6) & 3);        }

        private static void Pack8IntValuesLE2(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 3))                | ((src[1] & 3) << 2)                | ((src[2] & 3) << 4)                | ((src[3] & 3) << 6)) & 255);
                            dest[1] = 
                (byte)((((src[4] & 3))                | ((src[5] & 3) << 2)                | ((src[6] & 3) << 4)                | ((src[7] & 3) << 6)) & 255);
                        }
        private static void Unpack8IntValuesBE2(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) >> 6) & 3);            dest[1] = ((((int)src[0]) >> 4) & 3);            dest[2] = ((((int)src[0]) >> 2) & 3);            dest[3] = ((((int)src[0])) & 3);            dest[4] = ((((int)src[1]) >> 6) & 3);            dest[5] = ((((int)src[1]) >> 4) & 3);            dest[6] = ((((int)src[1]) >> 2) & 3);            dest[7] = ((((int)src[1])) & 3);        }

        private static void Pack8IntValuesBE2(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 3) << 6)                | ((src[1] & 3) << 4)                | ((src[2] & 3) << 2)                | ((src[3] & 3))) & 255);
                            dest[1] = 
                (byte)((((src[4] & 3) << 6)                | ((src[5] & 3) << 4)                | ((src[6] & 3) << 2)                | ((src[7] & 3))) & 255);
                        }
        private static void Unpack8IntValuesLE3(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 7);            dest[1] = ((((int)src[0]) >> 3) & 7);            dest[2] = ((((int)src[0]) >> 6) & 3) | ((((int)src[1]) << 2) & 7);            dest[3] = ((((int)src[1]) >> 1) & 7);            dest[4] = ((((int)src[1]) >> 4) & 7);            dest[5] = ((((int)src[1]) >> 7) & 1) | ((((int)src[2]) << 1) & 7);            dest[6] = ((((int)src[2]) >> 2) & 7);            dest[7] = ((((int)src[2]) >> 5) & 7);        }

        private static void Pack8IntValuesLE3(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 7))                | ((src[1] & 7) << 3)                | ((src[2] & 7) << 6)) & 255);
                            dest[1] = 
                (byte)((((src[2] & 7) >> 2)                | ((src[3] & 7) << 1)                | ((src[4] & 7) << 4)                | ((src[5] & 7) << 7)) & 255);
                            dest[2] = 
                (byte)((((src[5] & 7) >> 1)                | ((src[6] & 7) << 2)                | ((src[7] & 7) << 5)) & 255);
                        }
        private static void Unpack8IntValuesBE3(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) >> 5) & 7);            dest[1] = ((((int)src[0]) >> 2) & 7);            dest[2] = ((((int)src[0]) << 1) & 7) | ((((int)src[1]) >> 7) & 1);            dest[3] = ((((int)src[1]) >> 4) & 7);            dest[4] = ((((int)src[1]) >> 1) & 7);            dest[5] = ((((int)src[1]) << 2) & 7) | ((((int)src[2]) >> 6) & 3);            dest[6] = ((((int)src[2]) >> 3) & 7);            dest[7] = ((((int)src[2])) & 7);        }

        private static void Pack8IntValuesBE3(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 7) << 5)                | ((src[1] & 7) << 2)                | ((src[2] & 7) >> 1)) & 255);
                            dest[1] = 
                (byte)((((src[2] & 7) << 7)                | ((src[3] & 7) << 4)                | ((src[4] & 7) << 1)                | ((src[5] & 7) >> 2)) & 255);
                            dest[2] = 
                (byte)((((src[5] & 7) << 6)                | ((src[6] & 7) << 3)                | ((src[7] & 7))) & 255);
                        }
        private static void Unpack8IntValuesLE4(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 15);            dest[1] = ((((int)src[0]) >> 4) & 15);            dest[2] = ((((int)src[1])) & 15);            dest[3] = ((((int)src[1]) >> 4) & 15);            dest[4] = ((((int)src[2])) & 15);            dest[5] = ((((int)src[2]) >> 4) & 15);            dest[6] = ((((int)src[3])) & 15);            dest[7] = ((((int)src[3]) >> 4) & 15);        }

        private static void Pack8IntValuesLE4(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 15))                | ((src[1] & 15) << 4)) & 255);
                            dest[1] = 
                (byte)((((src[2] & 15))                | ((src[3] & 15) << 4)) & 255);
                            dest[2] = 
                (byte)((((src[4] & 15))                | ((src[5] & 15) << 4)) & 255);
                            dest[3] = 
                (byte)((((src[6] & 15))                | ((src[7] & 15) << 4)) & 255);
                        }
        private static void Unpack8IntValuesBE4(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) >> 4) & 15);            dest[1] = ((((int)src[0])) & 15);            dest[2] = ((((int)src[1]) >> 4) & 15);            dest[3] = ((((int)src[1])) & 15);            dest[4] = ((((int)src[2]) >> 4) & 15);            dest[5] = ((((int)src[2])) & 15);            dest[6] = ((((int)src[3]) >> 4) & 15);            dest[7] = ((((int)src[3])) & 15);        }

        private static void Pack8IntValuesBE4(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 15) << 4)                | ((src[1] & 15))) & 255);
                            dest[1] = 
                (byte)((((src[2] & 15) << 4)                | ((src[3] & 15))) & 255);
                            dest[2] = 
                (byte)((((src[4] & 15) << 4)                | ((src[5] & 15))) & 255);
                            dest[3] = 
                (byte)((((src[6] & 15) << 4)                | ((src[7] & 15))) & 255);
                        }
        private static void Unpack8IntValuesLE5(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 31);            dest[1] = ((((int)src[0]) >> 5) & 7) | ((((int)src[1]) << 3) & 31);            dest[2] = ((((int)src[1]) >> 2) & 31);            dest[3] = ((((int)src[1]) >> 7) & 1) | ((((int)src[2]) << 1) & 31);            dest[4] = ((((int)src[2]) >> 4) & 15) | ((((int)src[3]) << 4) & 31);            dest[5] = ((((int)src[3]) >> 1) & 31);            dest[6] = ((((int)src[3]) >> 6) & 3) | ((((int)src[4]) << 2) & 31);            dest[7] = ((((int)src[4]) >> 3) & 31);        }

        private static void Pack8IntValuesLE5(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 31))                | ((src[1] & 31) << 5)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 31) >> 3)                | ((src[2] & 31) << 2)                | ((src[3] & 31) << 7)) & 255);
                            dest[2] = 
                (byte)((((src[3] & 31) >> 1)                | ((src[4] & 31) << 4)) & 255);
                            dest[3] = 
                (byte)((((src[4] & 31) >> 4)                | ((src[5] & 31) << 1)                | ((src[6] & 31) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[6] & 31) >> 2)                | ((src[7] & 31) << 3)) & 255);
                        }
        private static void Unpack8IntValuesBE5(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) >> 3) & 31);            dest[1] = ((((int)src[0]) << 2) & 31) | ((((int)src[1]) >> 6) & 3);            dest[2] = ((((int)src[1]) >> 1) & 31);            dest[3] = ((((int)src[1]) << 4) & 31) | ((((int)src[2]) >> 4) & 15);            dest[4] = ((((int)src[2]) << 1) & 31) | ((((int)src[3]) >> 7) & 1);            dest[5] = ((((int)src[3]) >> 2) & 31);            dest[6] = ((((int)src[3]) << 3) & 31) | ((((int)src[4]) >> 5) & 7);            dest[7] = ((((int)src[4])) & 31);        }

        private static void Pack8IntValuesBE5(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 31) << 3)                | ((src[1] & 31) >> 2)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 31) << 6)                | ((src[2] & 31) << 1)                | ((src[3] & 31) >> 4)) & 255);
                            dest[2] = 
                (byte)((((src[3] & 31) << 4)                | ((src[4] & 31) >> 1)) & 255);
                            dest[3] = 
                (byte)((((src[4] & 31) << 7)                | ((src[5] & 31) << 2)                | ((src[6] & 31) >> 3)) & 255);
                            dest[4] = 
                (byte)((((src[6] & 31) << 5)                | ((src[7] & 31))) & 255);
                        }
        private static void Unpack8IntValuesLE6(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 63);            dest[1] = ((((int)src[0]) >> 6) & 3) | ((((int)src[1]) << 2) & 63);            dest[2] = ((((int)src[1]) >> 4) & 15) | ((((int)src[2]) << 4) & 63);            dest[3] = ((((int)src[2]) >> 2) & 63);            dest[4] = ((((int)src[3])) & 63);            dest[5] = ((((int)src[3]) >> 6) & 3) | ((((int)src[4]) << 2) & 63);            dest[6] = ((((int)src[4]) >> 4) & 15) | ((((int)src[5]) << 4) & 63);            dest[7] = ((((int)src[5]) >> 2) & 63);        }

        private static void Pack8IntValuesLE6(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 63))                | ((src[1] & 63) << 6)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 63) >> 2)                | ((src[2] & 63) << 4)) & 255);
                            dest[2] = 
                (byte)((((src[2] & 63) >> 4)                | ((src[3] & 63) << 2)) & 255);
                            dest[3] = 
                (byte)((((src[4] & 63))                | ((src[5] & 63) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[5] & 63) >> 2)                | ((src[6] & 63) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[6] & 63) >> 4)                | ((src[7] & 63) << 2)) & 255);
                        }
        private static void Unpack8IntValuesBE6(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) >> 2) & 63);            dest[1] = ((((int)src[0]) << 4) & 63) | ((((int)src[1]) >> 4) & 15);            dest[2] = ((((int)src[1]) << 2) & 63) | ((((int)src[2]) >> 6) & 3);            dest[3] = ((((int)src[2])) & 63);            dest[4] = ((((int)src[3]) >> 2) & 63);            dest[5] = ((((int)src[3]) << 4) & 63) | ((((int)src[4]) >> 4) & 15);            dest[6] = ((((int)src[4]) << 2) & 63) | ((((int)src[5]) >> 6) & 3);            dest[7] = ((((int)src[5])) & 63);        }

        private static void Pack8IntValuesBE6(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 63) << 2)                | ((src[1] & 63) >> 4)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 63) << 4)                | ((src[2] & 63) >> 2)) & 255);
                            dest[2] = 
                (byte)((((src[2] & 63) << 6)                | ((src[3] & 63))) & 255);
                            dest[3] = 
                (byte)((((src[4] & 63) << 2)                | ((src[5] & 63) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[5] & 63) << 4)                | ((src[6] & 63) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[6] & 63) << 6)                | ((src[7] & 63))) & 255);
                        }
        private static void Unpack8IntValuesLE7(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 127);            dest[1] = ((((int)src[0]) >> 7) & 1) | ((((int)src[1]) << 1) & 127);            dest[2] = ((((int)src[1]) >> 6) & 3) | ((((int)src[2]) << 2) & 127);            dest[3] = ((((int)src[2]) >> 5) & 7) | ((((int)src[3]) << 3) & 127);            dest[4] = ((((int)src[3]) >> 4) & 15) | ((((int)src[4]) << 4) & 127);            dest[5] = ((((int)src[4]) >> 3) & 31) | ((((int)src[5]) << 5) & 127);            dest[6] = ((((int)src[5]) >> 2) & 63) | ((((int)src[6]) << 6) & 127);            dest[7] = ((((int)src[6]) >> 1) & 127);        }

        private static void Pack8IntValuesLE7(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 127))                | ((src[1] & 127) << 7)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 127) >> 1)                | ((src[2] & 127) << 6)) & 255);
                            dest[2] = 
                (byte)((((src[2] & 127) >> 2)                | ((src[3] & 127) << 5)) & 255);
                            dest[3] = 
                (byte)((((src[3] & 127) >> 3)                | ((src[4] & 127) << 4)) & 255);
                            dest[4] = 
                (byte)((((src[4] & 127) >> 4)                | ((src[5] & 127) << 3)) & 255);
                            dest[5] = 
                (byte)((((src[5] & 127) >> 5)                | ((src[6] & 127) << 2)) & 255);
                            dest[6] = 
                (byte)((((src[6] & 127) >> 6)                | ((src[7] & 127) << 1)) & 255);
                        }
        private static void Unpack8IntValuesBE7(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) >> 1) & 127);            dest[1] = ((((int)src[0]) << 6) & 127) | ((((int)src[1]) >> 2) & 63);            dest[2] = ((((int)src[1]) << 5) & 127) | ((((int)src[2]) >> 3) & 31);            dest[3] = ((((int)src[2]) << 4) & 127) | ((((int)src[3]) >> 4) & 15);            dest[4] = ((((int)src[3]) << 3) & 127) | ((((int)src[4]) >> 5) & 7);            dest[5] = ((((int)src[4]) << 2) & 127) | ((((int)src[5]) >> 6) & 3);            dest[6] = ((((int)src[5]) << 1) & 127) | ((((int)src[6]) >> 7) & 1);            dest[7] = ((((int)src[6])) & 127);        }

        private static void Pack8IntValuesBE7(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 127) << 1)                | ((src[1] & 127) >> 6)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 127) << 2)                | ((src[2] & 127) >> 5)) & 255);
                            dest[2] = 
                (byte)((((src[2] & 127) << 3)                | ((src[3] & 127) >> 4)) & 255);
                            dest[3] = 
                (byte)((((src[3] & 127) << 4)                | ((src[4] & 127) >> 3)) & 255);
                            dest[4] = 
                (byte)((((src[4] & 127) << 5)                | ((src[5] & 127) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[5] & 127) << 6)                | ((src[6] & 127) >> 1)) & 255);
                            dest[6] = 
                (byte)((((src[6] & 127) << 7)                | ((src[7] & 127))) & 255);
                        }
        private static void Unpack8IntValuesLE8(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255);            dest[1] = ((((int)src[1])) & 255);            dest[2] = ((((int)src[2])) & 255);            dest[3] = ((((int)src[3])) & 255);            dest[4] = ((((int)src[4])) & 255);            dest[5] = ((((int)src[5])) & 255);            dest[6] = ((((int)src[6])) & 255);            dest[7] = ((((int)src[7])) & 255);        }

        private static void Pack8IntValuesLE8(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 255))) & 255);
                            dest[1] = 
                (byte)((((src[1] & 255))) & 255);
                            dest[2] = 
                (byte)((((src[2] & 255))) & 255);
                            dest[3] = 
                (byte)((((src[3] & 255))) & 255);
                            dest[4] = 
                (byte)((((src[4] & 255))) & 255);
                            dest[5] = 
                (byte)((((src[5] & 255))) & 255);
                            dest[6] = 
                (byte)((((src[6] & 255))) & 255);
                            dest[7] = 
                (byte)((((src[7] & 255))) & 255);
                        }
        private static void Unpack8IntValuesBE8(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255);            dest[1] = ((((int)src[1])) & 255);            dest[2] = ((((int)src[2])) & 255);            dest[3] = ((((int)src[3])) & 255);            dest[4] = ((((int)src[4])) & 255);            dest[5] = ((((int)src[5])) & 255);            dest[6] = ((((int)src[6])) & 255);            dest[7] = ((((int)src[7])) & 255);        }

        private static void Pack8IntValuesBE8(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 255))) & 255);
                            dest[1] = 
                (byte)((((src[1] & 255))) & 255);
                            dest[2] = 
                (byte)((((src[2] & 255))) & 255);
                            dest[3] = 
                (byte)((((src[3] & 255))) & 255);
                            dest[4] = 
                (byte)((((src[4] & 255))) & 255);
                            dest[5] = 
                (byte)((((src[5] & 255))) & 255);
                            dest[6] = 
                (byte)((((src[6] & 255))) & 255);
                            dest[7] = 
                (byte)((((src[7] & 255))) & 255);
                        }
        private static void Unpack8IntValuesLE9(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 511);            dest[1] = ((((int)src[1]) >> 1) & 127) | ((((int)src[2]) << 7) & 511);            dest[2] = ((((int)src[2]) >> 2) & 63) | ((((int)src[3]) << 6) & 511);            dest[3] = ((((int)src[3]) >> 3) & 31) | ((((int)src[4]) << 5) & 511);            dest[4] = ((((int)src[4]) >> 4) & 15) | ((((int)src[5]) << 4) & 511);            dest[5] = ((((int)src[5]) >> 5) & 7) | ((((int)src[6]) << 3) & 511);            dest[6] = ((((int)src[6]) >> 6) & 3) | ((((int)src[7]) << 2) & 511);            dest[7] = ((((int)src[7]) >> 7) & 1) | ((((int)src[8]) << 1) & 511);        }

        private static void Pack8IntValuesLE9(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 511))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 511) >> 8)                | ((src[1] & 511) << 1)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 511) >> 7)                | ((src[2] & 511) << 2)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 511) >> 6)                | ((src[3] & 511) << 3)) & 255);
                            dest[4] = 
                (byte)((((src[3] & 511) >> 5)                | ((src[4] & 511) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[4] & 511) >> 4)                | ((src[5] & 511) << 5)) & 255);
                            dest[6] = 
                (byte)((((src[5] & 511) >> 3)                | ((src[6] & 511) << 6)) & 255);
                            dest[7] = 
                (byte)((((src[6] & 511) >> 2)                | ((src[7] & 511) << 7)) & 255);
                            dest[8] = 
                (byte)((((src[7] & 511) >> 1)) & 255);
                        }
        private static void Unpack8IntValuesBE9(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 1) & 511) | ((((int)src[1]) >> 7) & 1);            dest[1] = ((((int)src[1]) << 2) & 511) | ((((int)src[2]) >> 6) & 3);            dest[2] = ((((int)src[2]) << 3) & 511) | ((((int)src[3]) >> 5) & 7);            dest[3] = ((((int)src[3]) << 4) & 511) | ((((int)src[4]) >> 4) & 15);            dest[4] = ((((int)src[4]) << 5) & 511) | ((((int)src[5]) >> 3) & 31);            dest[5] = ((((int)src[5]) << 6) & 511) | ((((int)src[6]) >> 2) & 63);            dest[6] = ((((int)src[6]) << 7) & 511) | ((((int)src[7]) >> 1) & 127);            dest[7] = ((((int)src[7]) << 8) & 511) | ((((int)src[8])) & 255);        }

        private static void Pack8IntValuesBE9(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 511) >> 1)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 511) << 7)                | ((src[1] & 511) >> 2)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 511) << 6)                | ((src[2] & 511) >> 3)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 511) << 5)                | ((src[3] & 511) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[3] & 511) << 4)                | ((src[4] & 511) >> 5)) & 255);
                            dest[5] = 
                (byte)((((src[4] & 511) << 3)                | ((src[5] & 511) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[5] & 511) << 2)                | ((src[6] & 511) >> 7)) & 255);
                            dest[7] = 
                (byte)((((src[6] & 511) << 1)                | ((src[7] & 511) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[7] & 511))) & 255);
                        }
        private static void Unpack8IntValuesLE10(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 1023);            dest[1] = ((((int)src[1]) >> 2) & 63) | ((((int)src[2]) << 6) & 1023);            dest[2] = ((((int)src[2]) >> 4) & 15) | ((((int)src[3]) << 4) & 1023);            dest[3] = ((((int)src[3]) >> 6) & 3) | ((((int)src[4]) << 2) & 1023);            dest[4] = ((((int)src[5])) & 255) | ((((int)src[6]) << 8) & 1023);            dest[5] = ((((int)src[6]) >> 2) & 63) | ((((int)src[7]) << 6) & 1023);            dest[6] = ((((int)src[7]) >> 4) & 15) | ((((int)src[8]) << 4) & 1023);            dest[7] = ((((int)src[8]) >> 6) & 3) | ((((int)src[9]) << 2) & 1023);        }

        private static void Pack8IntValuesLE10(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1023))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1023) >> 8)                | ((src[1] & 1023) << 2)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 1023) >> 6)                | ((src[2] & 1023) << 4)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 1023) >> 4)                | ((src[3] & 1023) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[3] & 1023) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[4] & 1023))) & 255);
                            dest[6] = 
                (byte)((((src[4] & 1023) >> 8)                | ((src[5] & 1023) << 2)) & 255);
                            dest[7] = 
                (byte)((((src[5] & 1023) >> 6)                | ((src[6] & 1023) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[6] & 1023) >> 4)                | ((src[7] & 1023) << 6)) & 255);
                            dest[9] = 
                (byte)((((src[7] & 1023) >> 2)) & 255);
                        }
        private static void Unpack8IntValuesBE10(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 2) & 1023) | ((((int)src[1]) >> 6) & 3);            dest[1] = ((((int)src[1]) << 4) & 1023) | ((((int)src[2]) >> 4) & 15);            dest[2] = ((((int)src[2]) << 6) & 1023) | ((((int)src[3]) >> 2) & 63);            dest[3] = ((((int)src[3]) << 8) & 1023) | ((((int)src[4])) & 255);            dest[4] = ((((int)src[5]) << 2) & 1023) | ((((int)src[6]) >> 6) & 3);            dest[5] = ((((int)src[6]) << 4) & 1023) | ((((int)src[7]) >> 4) & 15);            dest[6] = ((((int)src[7]) << 6) & 1023) | ((((int)src[8]) >> 2) & 63);            dest[7] = ((((int)src[8]) << 8) & 1023) | ((((int)src[9])) & 255);        }

        private static void Pack8IntValuesBE10(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1023) >> 2)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1023) << 6)                | ((src[1] & 1023) >> 4)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 1023) << 4)                | ((src[2] & 1023) >> 6)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 1023) << 2)                | ((src[3] & 1023) >> 8)) & 255);
                            dest[4] = 
                (byte)((((src[3] & 1023))) & 255);
                            dest[5] = 
                (byte)((((src[4] & 1023) >> 2)) & 255);
                            dest[6] = 
                (byte)((((src[4] & 1023) << 6)                | ((src[5] & 1023) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[5] & 1023) << 4)                | ((src[6] & 1023) >> 6)) & 255);
                            dest[8] = 
                (byte)((((src[6] & 1023) << 2)                | ((src[7] & 1023) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[7] & 1023))) & 255);
                        }
        private static void Unpack8IntValuesLE11(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 2047);            dest[1] = ((((int)src[1]) >> 3) & 31) | ((((int)src[2]) << 5) & 2047);            dest[2] = ((((int)src[2]) >> 6) & 3) | ((((int)src[3]) << 2) & 1023) | ((((int)src[4]) << 10) & 2047);            dest[3] = ((((int)src[4]) >> 1) & 127) | ((((int)src[5]) << 7) & 2047);            dest[4] = ((((int)src[5]) >> 4) & 15) | ((((int)src[6]) << 4) & 2047);            dest[5] = ((((int)src[6]) >> 7) & 1) | ((((int)src[7]) << 1) & 511) | ((((int)src[8]) << 9) & 2047);            dest[6] = ((((int)src[8]) >> 2) & 63) | ((((int)src[9]) << 6) & 2047);            dest[7] = ((((int)src[9]) >> 5) & 7) | ((((int)src[10]) << 3) & 2047);        }

        private static void Pack8IntValuesLE11(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2047))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2047) >> 8)                | ((src[1] & 2047) << 3)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 2047) >> 5)                | ((src[2] & 2047) << 6)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 2047) >> 2)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 2047) >> 10)                | ((src[3] & 2047) << 1)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 2047) >> 7)                | ((src[4] & 2047) << 4)) & 255);
                            dest[6] = 
                (byte)((((src[4] & 2047) >> 4)                | ((src[5] & 2047) << 7)) & 255);
                            dest[7] = 
                (byte)((((src[5] & 2047) >> 1)) & 255);
                            dest[8] = 
                (byte)((((src[5] & 2047) >> 9)                | ((src[6] & 2047) << 2)) & 255);
                            dest[9] = 
                (byte)((((src[6] & 2047) >> 6)                | ((src[7] & 2047) << 5)) & 255);
                            dest[10] = 
                (byte)((((src[7] & 2047) >> 3)) & 255);
                        }
        private static void Unpack8IntValuesBE11(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 3) & 2047) | ((((int)src[1]) >> 5) & 7);            dest[1] = ((((int)src[1]) << 6) & 2047) | ((((int)src[2]) >> 2) & 63);            dest[2] = ((((int)src[2]) << 9) & 2047) | ((((int)src[3]) << 1) & 511) | ((((int)src[4]) >> 7) & 1);            dest[3] = ((((int)src[4]) << 4) & 2047) | ((((int)src[5]) >> 4) & 15);            dest[4] = ((((int)src[5]) << 7) & 2047) | ((((int)src[6]) >> 1) & 127);            dest[5] = ((((int)src[6]) << 10) & 2047) | ((((int)src[7]) << 2) & 1023) | ((((int)src[8]) >> 6) & 3);            dest[6] = ((((int)src[8]) << 5) & 2047) | ((((int)src[9]) >> 3) & 31);            dest[7] = ((((int)src[9]) << 8) & 2047) | ((((int)src[10])) & 255);        }

        private static void Pack8IntValuesBE11(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2047) >> 3)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2047) << 5)                | ((src[1] & 2047) >> 6)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 2047) << 2)                | ((src[2] & 2047) >> 9)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 2047) >> 1)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 2047) << 7)                | ((src[3] & 2047) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 2047) << 4)                | ((src[4] & 2047) >> 7)) & 255);
                            dest[6] = 
                (byte)((((src[4] & 2047) << 1)                | ((src[5] & 2047) >> 10)) & 255);
                            dest[7] = 
                (byte)((((src[5] & 2047) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[5] & 2047) << 6)                | ((src[6] & 2047) >> 5)) & 255);
                            dest[9] = 
                (byte)((((src[6] & 2047) << 3)                | ((src[7] & 2047) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[7] & 2047))) & 255);
                        }
        private static void Unpack8IntValuesLE12(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 4095);            dest[1] = ((((int)src[1]) >> 4) & 15) | ((((int)src[2]) << 4) & 4095);            dest[2] = ((((int)src[3])) & 255) | ((((int)src[4]) << 8) & 4095);            dest[3] = ((((int)src[4]) >> 4) & 15) | ((((int)src[5]) << 4) & 4095);            dest[4] = ((((int)src[6])) & 255) | ((((int)src[7]) << 8) & 4095);            dest[5] = ((((int)src[7]) >> 4) & 15) | ((((int)src[8]) << 4) & 4095);            dest[6] = ((((int)src[9])) & 255) | ((((int)src[10]) << 8) & 4095);            dest[7] = ((((int)src[10]) >> 4) & 15) | ((((int)src[11]) << 4) & 4095);        }

        private static void Pack8IntValuesLE12(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4095))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4095) >> 8)                | ((src[1] & 4095) << 4)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 4095) >> 4)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 4095))) & 255);
                            dest[4] = 
                (byte)((((src[2] & 4095) >> 8)                | ((src[3] & 4095) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 4095) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[4] & 4095))) & 255);
                            dest[7] = 
                (byte)((((src[4] & 4095) >> 8)                | ((src[5] & 4095) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[5] & 4095) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[6] & 4095))) & 255);
                            dest[10] = 
                (byte)((((src[6] & 4095) >> 8)                | ((src[7] & 4095) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[7] & 4095) >> 4)) & 255);
                        }
        private static void Unpack8IntValuesBE12(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 4) & 4095) | ((((int)src[1]) >> 4) & 15);            dest[1] = ((((int)src[1]) << 8) & 4095) | ((((int)src[2])) & 255);            dest[2] = ((((int)src[3]) << 4) & 4095) | ((((int)src[4]) >> 4) & 15);            dest[3] = ((((int)src[4]) << 8) & 4095) | ((((int)src[5])) & 255);            dest[4] = ((((int)src[6]) << 4) & 4095) | ((((int)src[7]) >> 4) & 15);            dest[5] = ((((int)src[7]) << 8) & 4095) | ((((int)src[8])) & 255);            dest[6] = ((((int)src[9]) << 4) & 4095) | ((((int)src[10]) >> 4) & 15);            dest[7] = ((((int)src[10]) << 8) & 4095) | ((((int)src[11])) & 255);        }

        private static void Pack8IntValuesBE12(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4095) >> 4)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4095) << 4)                | ((src[1] & 4095) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 4095))) & 255);
                            dest[3] = 
                (byte)((((src[2] & 4095) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 4095) << 4)                | ((src[3] & 4095) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 4095))) & 255);
                            dest[6] = 
                (byte)((((src[4] & 4095) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[4] & 4095) << 4)                | ((src[5] & 4095) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[5] & 4095))) & 255);
                            dest[9] = 
                (byte)((((src[6] & 4095) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[6] & 4095) << 4)                | ((src[7] & 4095) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[7] & 4095))) & 255);
                        }
        private static void Unpack8IntValuesLE13(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 8191);            dest[1] = ((((int)src[1]) >> 5) & 7) | ((((int)src[2]) << 3) & 2047) | ((((int)src[3]) << 11) & 8191);            dest[2] = ((((int)src[3]) >> 2) & 63) | ((((int)src[4]) << 6) & 8191);            dest[3] = ((((int)src[4]) >> 7) & 1) | ((((int)src[5]) << 1) & 511) | ((((int)src[6]) << 9) & 8191);            dest[4] = ((((int)src[6]) >> 4) & 15) | ((((int)src[7]) << 4) & 4095) | ((((int)src[8]) << 12) & 8191);            dest[5] = ((((int)src[8]) >> 1) & 127) | ((((int)src[9]) << 7) & 8191);            dest[6] = ((((int)src[9]) >> 6) & 3) | ((((int)src[10]) << 2) & 1023) | ((((int)src[11]) << 10) & 8191);            dest[7] = ((((int)src[11]) >> 3) & 31) | ((((int)src[12]) << 5) & 8191);        }

        private static void Pack8IntValuesLE13(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8191))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8191) >> 8)                | ((src[1] & 8191) << 5)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 8191) >> 3)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 8191) >> 11)                | ((src[2] & 8191) << 2)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 8191) >> 6)                | ((src[3] & 8191) << 7)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 8191) >> 1)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 8191) >> 9)                | ((src[4] & 8191) << 4)) & 255);
                            dest[7] = 
                (byte)((((src[4] & 8191) >> 4)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 8191) >> 12)                | ((src[5] & 8191) << 1)) & 255);
                            dest[9] = 
                (byte)((((src[5] & 8191) >> 7)                | ((src[6] & 8191) << 6)) & 255);
                            dest[10] = 
                (byte)((((src[6] & 8191) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[6] & 8191) >> 10)                | ((src[7] & 8191) << 3)) & 255);
                            dest[12] = 
                (byte)((((src[7] & 8191) >> 5)) & 255);
                        }
        private static void Unpack8IntValuesBE13(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 5) & 8191) | ((((int)src[1]) >> 3) & 31);            dest[1] = ((((int)src[1]) << 10) & 8191) | ((((int)src[2]) << 2) & 1023) | ((((int)src[3]) >> 6) & 3);            dest[2] = ((((int)src[3]) << 7) & 8191) | ((((int)src[4]) >> 1) & 127);            dest[3] = ((((int)src[4]) << 12) & 8191) | ((((int)src[5]) << 4) & 4095) | ((((int)src[6]) >> 4) & 15);            dest[4] = ((((int)src[6]) << 9) & 8191) | ((((int)src[7]) << 1) & 511) | ((((int)src[8]) >> 7) & 1);            dest[5] = ((((int)src[8]) << 6) & 8191) | ((((int)src[9]) >> 2) & 63);            dest[6] = ((((int)src[9]) << 11) & 8191) | ((((int)src[10]) << 3) & 2047) | ((((int)src[11]) >> 5) & 7);            dest[7] = ((((int)src[11]) << 8) & 8191) | ((((int)src[12])) & 255);        }

        private static void Pack8IntValuesBE13(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8191) >> 5)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8191) << 3)                | ((src[1] & 8191) >> 10)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 8191) >> 2)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 8191) << 6)                | ((src[2] & 8191) >> 7)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 8191) << 1)                | ((src[3] & 8191) >> 12)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 8191) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 8191) << 4)                | ((src[4] & 8191) >> 9)) & 255);
                            dest[7] = 
                (byte)((((src[4] & 8191) >> 1)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 8191) << 7)                | ((src[5] & 8191) >> 6)) & 255);
                            dest[9] = 
                (byte)((((src[5] & 8191) << 2)                | ((src[6] & 8191) >> 11)) & 255);
                            dest[10] = 
                (byte)((((src[6] & 8191) >> 3)) & 255);
                            dest[11] = 
                (byte)((((src[6] & 8191) << 5)                | ((src[7] & 8191) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[7] & 8191))) & 255);
                        }
        private static void Unpack8IntValuesLE14(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 16383);            dest[1] = ((((int)src[1]) >> 6) & 3) | ((((int)src[2]) << 2) & 1023) | ((((int)src[3]) << 10) & 16383);            dest[2] = ((((int)src[3]) >> 4) & 15) | ((((int)src[4]) << 4) & 4095) | ((((int)src[5]) << 12) & 16383);            dest[3] = ((((int)src[5]) >> 2) & 63) | ((((int)src[6]) << 6) & 16383);            dest[4] = ((((int)src[7])) & 255) | ((((int)src[8]) << 8) & 16383);            dest[5] = ((((int)src[8]) >> 6) & 3) | ((((int)src[9]) << 2) & 1023) | ((((int)src[10]) << 10) & 16383);            dest[6] = ((((int)src[10]) >> 4) & 15) | ((((int)src[11]) << 4) & 4095) | ((((int)src[12]) << 12) & 16383);            dest[7] = ((((int)src[12]) >> 2) & 63) | ((((int)src[13]) << 6) & 16383);        }

        private static void Pack8IntValuesLE14(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 16383))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 16383) >> 8)                | ((src[1] & 16383) << 6)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 16383) >> 2)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 16383) >> 10)                | ((src[2] & 16383) << 4)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 16383) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 16383) >> 12)                | ((src[3] & 16383) << 2)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 16383) >> 6)) & 255);
                            dest[7] = 
                (byte)((((src[4] & 16383))) & 255);
                            dest[8] = 
                (byte)((((src[4] & 16383) >> 8)                | ((src[5] & 16383) << 6)) & 255);
                            dest[9] = 
                (byte)((((src[5] & 16383) >> 2)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 16383) >> 10)                | ((src[6] & 16383) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[6] & 16383) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 16383) >> 12)                | ((src[7] & 16383) << 2)) & 255);
                            dest[13] = 
                (byte)((((src[7] & 16383) >> 6)) & 255);
                        }
        private static void Unpack8IntValuesBE14(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 6) & 16383) | ((((int)src[1]) >> 2) & 63);            dest[1] = ((((int)src[1]) << 12) & 16383) | ((((int)src[2]) << 4) & 4095) | ((((int)src[3]) >> 4) & 15);            dest[2] = ((((int)src[3]) << 10) & 16383) | ((((int)src[4]) << 2) & 1023) | ((((int)src[5]) >> 6) & 3);            dest[3] = ((((int)src[5]) << 8) & 16383) | ((((int)src[6])) & 255);            dest[4] = ((((int)src[7]) << 6) & 16383) | ((((int)src[8]) >> 2) & 63);            dest[5] = ((((int)src[8]) << 12) & 16383) | ((((int)src[9]) << 4) & 4095) | ((((int)src[10]) >> 4) & 15);            dest[6] = ((((int)src[10]) << 10) & 16383) | ((((int)src[11]) << 2) & 1023) | ((((int)src[12]) >> 6) & 3);            dest[7] = ((((int)src[12]) << 8) & 16383) | ((((int)src[13])) & 255);        }

        private static void Pack8IntValuesBE14(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 16383) >> 6)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 16383) << 2)                | ((src[1] & 16383) >> 12)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 16383) >> 4)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 16383) << 4)                | ((src[2] & 16383) >> 10)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 16383) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 16383) << 6)                | ((src[3] & 16383) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 16383))) & 255);
                            dest[7] = 
                (byte)((((src[4] & 16383) >> 6)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 16383) << 2)                | ((src[5] & 16383) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[5] & 16383) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 16383) << 4)                | ((src[6] & 16383) >> 10)) & 255);
                            dest[11] = 
                (byte)((((src[6] & 16383) >> 2)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 16383) << 6)                | ((src[7] & 16383) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[7] & 16383))) & 255);
                        }
        private static void Unpack8IntValuesLE15(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 32767);            dest[1] = ((((int)src[1]) >> 7) & 1) | ((((int)src[2]) << 1) & 511) | ((((int)src[3]) << 9) & 32767);            dest[2] = ((((int)src[3]) >> 6) & 3) | ((((int)src[4]) << 2) & 1023) | ((((int)src[5]) << 10) & 32767);            dest[3] = ((((int)src[5]) >> 5) & 7) | ((((int)src[6]) << 3) & 2047) | ((((int)src[7]) << 11) & 32767);            dest[4] = ((((int)src[7]) >> 4) & 15) | ((((int)src[8]) << 4) & 4095) | ((((int)src[9]) << 12) & 32767);            dest[5] = ((((int)src[9]) >> 3) & 31) | ((((int)src[10]) << 5) & 8191) | ((((int)src[11]) << 13) & 32767);            dest[6] = ((((int)src[11]) >> 2) & 63) | ((((int)src[12]) << 6) & 16383) | ((((int)src[13]) << 14) & 32767);            dest[7] = ((((int)src[13]) >> 1) & 127) | ((((int)src[14]) << 7) & 32767);        }

        private static void Pack8IntValuesLE15(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 32767))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 32767) >> 8)                | ((src[1] & 32767) << 7)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 32767) >> 1)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 32767) >> 9)                | ((src[2] & 32767) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 32767) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 32767) >> 10)                | ((src[3] & 32767) << 5)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 32767) >> 3)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 32767) >> 11)                | ((src[4] & 32767) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 32767) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 32767) >> 12)                | ((src[5] & 32767) << 3)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 32767) >> 5)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 32767) >> 13)                | ((src[6] & 32767) << 2)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 32767) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 32767) >> 14)                | ((src[7] & 32767) << 1)) & 255);
                            dest[14] = 
                (byte)((((src[7] & 32767) >> 7)) & 255);
                        }
        private static void Unpack8IntValuesBE15(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 7) & 32767) | ((((int)src[1]) >> 1) & 127);            dest[1] = ((((int)src[1]) << 14) & 32767) | ((((int)src[2]) << 6) & 16383) | ((((int)src[3]) >> 2) & 63);            dest[2] = ((((int)src[3]) << 13) & 32767) | ((((int)src[4]) << 5) & 8191) | ((((int)src[5]) >> 3) & 31);            dest[3] = ((((int)src[5]) << 12) & 32767) | ((((int)src[6]) << 4) & 4095) | ((((int)src[7]) >> 4) & 15);            dest[4] = ((((int)src[7]) << 11) & 32767) | ((((int)src[8]) << 3) & 2047) | ((((int)src[9]) >> 5) & 7);            dest[5] = ((((int)src[9]) << 10) & 32767) | ((((int)src[10]) << 2) & 1023) | ((((int)src[11]) >> 6) & 3);            dest[6] = ((((int)src[11]) << 9) & 32767) | ((((int)src[12]) << 1) & 511) | ((((int)src[13]) >> 7) & 1);            dest[7] = ((((int)src[13]) << 8) & 32767) | ((((int)src[14])) & 255);        }

        private static void Pack8IntValuesBE15(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 32767) >> 7)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 32767) << 1)                | ((src[1] & 32767) >> 14)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 32767) >> 6)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 32767) << 2)                | ((src[2] & 32767) >> 13)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 32767) >> 5)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 32767) << 3)                | ((src[3] & 32767) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 32767) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 32767) << 4)                | ((src[4] & 32767) >> 11)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 32767) >> 3)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 32767) << 5)                | ((src[5] & 32767) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 32767) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 32767) << 6)                | ((src[6] & 32767) >> 9)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 32767) >> 1)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 32767) << 7)                | ((src[7] & 32767) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[7] & 32767))) & 255);
                        }
        private static void Unpack8IntValuesLE16(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535);            dest[1] = ((((int)src[2])) & 255) | ((((int)src[3]) << 8) & 65535);            dest[2] = ((((int)src[4])) & 255) | ((((int)src[5]) << 8) & 65535);            dest[3] = ((((int)src[6])) & 255) | ((((int)src[7]) << 8) & 65535);            dest[4] = ((((int)src[8])) & 255) | ((((int)src[9]) << 8) & 65535);            dest[5] = ((((int)src[10])) & 255) | ((((int)src[11]) << 8) & 65535);            dest[6] = ((((int)src[12])) & 255) | ((((int)src[13]) << 8) & 65535);            dest[7] = ((((int)src[14])) & 255) | ((((int)src[15]) << 8) & 65535);        }

        private static void Pack8IntValuesLE16(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 65535))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 65535) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 65535))) & 255);
                            dest[3] = 
                (byte)((((src[1] & 65535) >> 8)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 65535))) & 255);
                            dest[5] = 
                (byte)((((src[2] & 65535) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 65535))) & 255);
                            dest[7] = 
                (byte)((((src[3] & 65535) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 65535))) & 255);
                            dest[9] = 
                (byte)((((src[4] & 65535) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 65535))) & 255);
                            dest[11] = 
                (byte)((((src[5] & 65535) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 65535))) & 255);
                            dest[13] = 
                (byte)((((src[6] & 65535) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[7] & 65535))) & 255);
                            dest[15] = 
                (byte)((((src[7] & 65535) >> 8)) & 255);
                        }
        private static void Unpack8IntValuesBE16(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 8) & 65535) | ((((int)src[1])) & 255);            dest[1] = ((((int)src[2]) << 8) & 65535) | ((((int)src[3])) & 255);            dest[2] = ((((int)src[4]) << 8) & 65535) | ((((int)src[5])) & 255);            dest[3] = ((((int)src[6]) << 8) & 65535) | ((((int)src[7])) & 255);            dest[4] = ((((int)src[8]) << 8) & 65535) | ((((int)src[9])) & 255);            dest[5] = ((((int)src[10]) << 8) & 65535) | ((((int)src[11])) & 255);            dest[6] = ((((int)src[12]) << 8) & 65535) | ((((int)src[13])) & 255);            dest[7] = ((((int)src[14]) << 8) & 65535) | ((((int)src[15])) & 255);        }

        private static void Pack8IntValuesBE16(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 65535) >> 8)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 65535))) & 255);
                            dest[2] = 
                (byte)((((src[1] & 65535) >> 8)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 65535))) & 255);
                            dest[4] = 
                (byte)((((src[2] & 65535) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 65535))) & 255);
                            dest[6] = 
                (byte)((((src[3] & 65535) >> 8)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 65535))) & 255);
                            dest[8] = 
                (byte)((((src[4] & 65535) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 65535))) & 255);
                            dest[10] = 
                (byte)((((src[5] & 65535) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 65535))) & 255);
                            dest[12] = 
                (byte)((((src[6] & 65535) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 65535))) & 255);
                            dest[14] = 
                (byte)((((src[7] & 65535) >> 8)) & 255);
                            dest[15] = 
                (byte)((((src[7] & 65535))) & 255);
                        }
        private static void Unpack8IntValuesLE17(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 131071);            dest[1] = ((((int)src[2]) >> 1) & 127) | ((((int)src[3]) << 7) & 32767) | ((((int)src[4]) << 15) & 131071);            dest[2] = ((((int)src[4]) >> 2) & 63) | ((((int)src[5]) << 6) & 16383) | ((((int)src[6]) << 14) & 131071);            dest[3] = ((((int)src[6]) >> 3) & 31) | ((((int)src[7]) << 5) & 8191) | ((((int)src[8]) << 13) & 131071);            dest[4] = ((((int)src[8]) >> 4) & 15) | ((((int)src[9]) << 4) & 4095) | ((((int)src[10]) << 12) & 131071);            dest[5] = ((((int)src[10]) >> 5) & 7) | ((((int)src[11]) << 3) & 2047) | ((((int)src[12]) << 11) & 131071);            dest[6] = ((((int)src[12]) >> 6) & 3) | ((((int)src[13]) << 2) & 1023) | ((((int)src[14]) << 10) & 131071);            dest[7] = ((((int)src[14]) >> 7) & 1) | ((((int)src[15]) << 1) & 511) | ((((int)src[16]) << 9) & 131071);        }

        private static void Pack8IntValuesLE17(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 131071))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 131071) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 131071) >> 16)                | ((src[1] & 131071) << 1)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 131071) >> 7)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 131071) >> 15)                | ((src[2] & 131071) << 2)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 131071) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 131071) >> 14)                | ((src[3] & 131071) << 3)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 131071) >> 5)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 131071) >> 13)                | ((src[4] & 131071) << 4)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 131071) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 131071) >> 12)                | ((src[5] & 131071) << 5)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 131071) >> 3)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 131071) >> 11)                | ((src[6] & 131071) << 6)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 131071) >> 2)) & 255);
                            dest[14] = 
                (byte)((((src[6] & 131071) >> 10)                | ((src[7] & 131071) << 7)) & 255);
                            dest[15] = 
                (byte)((((src[7] & 131071) >> 1)) & 255);
                            dest[16] = 
                (byte)((((src[7] & 131071) >> 9)) & 255);
                        }
        private static void Unpack8IntValuesBE17(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 9) & 131071) | ((((int)src[1]) << 1) & 511) | ((((int)src[2]) >> 7) & 1);            dest[1] = ((((int)src[2]) << 10) & 131071) | ((((int)src[3]) << 2) & 1023) | ((((int)src[4]) >> 6) & 3);            dest[2] = ((((int)src[4]) << 11) & 131071) | ((((int)src[5]) << 3) & 2047) | ((((int)src[6]) >> 5) & 7);            dest[3] = ((((int)src[6]) << 12) & 131071) | ((((int)src[7]) << 4) & 4095) | ((((int)src[8]) >> 4) & 15);            dest[4] = ((((int)src[8]) << 13) & 131071) | ((((int)src[9]) << 5) & 8191) | ((((int)src[10]) >> 3) & 31);            dest[5] = ((((int)src[10]) << 14) & 131071) | ((((int)src[11]) << 6) & 16383) | ((((int)src[12]) >> 2) & 63);            dest[6] = ((((int)src[12]) << 15) & 131071) | ((((int)src[13]) << 7) & 32767) | ((((int)src[14]) >> 1) & 127);            dest[7] = ((((int)src[14]) << 16) & 131071) | ((((int)src[15]) << 8) & 65535) | ((((int)src[16])) & 255);        }

        private static void Pack8IntValuesBE17(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 131071) >> 9)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 131071) >> 1)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 131071) << 7)                | ((src[1] & 131071) >> 10)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 131071) >> 2)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 131071) << 6)                | ((src[2] & 131071) >> 11)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 131071) >> 3)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 131071) << 5)                | ((src[3] & 131071) >> 12)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 131071) >> 4)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 131071) << 4)                | ((src[4] & 131071) >> 13)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 131071) >> 5)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 131071) << 3)                | ((src[5] & 131071) >> 14)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 131071) >> 6)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 131071) << 2)                | ((src[6] & 131071) >> 15)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 131071) >> 7)) & 255);
                            dest[14] = 
                (byte)((((src[6] & 131071) << 1)                | ((src[7] & 131071) >> 16)) & 255);
                            dest[15] = 
                (byte)((((src[7] & 131071) >> 8)) & 255);
                            dest[16] = 
                (byte)((((src[7] & 131071))) & 255);
                        }
        private static void Unpack8IntValuesLE18(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 262143);            dest[1] = ((((int)src[2]) >> 2) & 63) | ((((int)src[3]) << 6) & 16383) | ((((int)src[4]) << 14) & 262143);            dest[2] = ((((int)src[4]) >> 4) & 15) | ((((int)src[5]) << 4) & 4095) | ((((int)src[6]) << 12) & 262143);            dest[3] = ((((int)src[6]) >> 6) & 3) | ((((int)src[7]) << 2) & 1023) | ((((int)src[8]) << 10) & 262143);            dest[4] = ((((int)src[9])) & 255) | ((((int)src[10]) << 8) & 65535) | ((((int)src[11]) << 16) & 262143);            dest[5] = ((((int)src[11]) >> 2) & 63) | ((((int)src[12]) << 6) & 16383) | ((((int)src[13]) << 14) & 262143);            dest[6] = ((((int)src[13]) >> 4) & 15) | ((((int)src[14]) << 4) & 4095) | ((((int)src[15]) << 12) & 262143);            dest[7] = ((((int)src[15]) >> 6) & 3) | ((((int)src[16]) << 2) & 1023) | ((((int)src[17]) << 10) & 262143);        }

        private static void Pack8IntValuesLE18(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 262143))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 262143) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 262143) >> 16)                | ((src[1] & 262143) << 2)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 262143) >> 6)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 262143) >> 14)                | ((src[2] & 262143) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 262143) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 262143) >> 12)                | ((src[3] & 262143) << 6)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 262143) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 262143) >> 10)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 262143))) & 255);
                            dest[10] = 
                (byte)((((src[4] & 262143) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 262143) >> 16)                | ((src[5] & 262143) << 2)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 262143) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 262143) >> 14)                | ((src[6] & 262143) << 4)) & 255);
                            dest[14] = 
                (byte)((((src[6] & 262143) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 262143) >> 12)                | ((src[7] & 262143) << 6)) & 255);
                            dest[16] = 
                (byte)((((src[7] & 262143) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[7] & 262143) >> 10)) & 255);
                        }
        private static void Unpack8IntValuesBE18(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 10) & 262143) | ((((int)src[1]) << 2) & 1023) | ((((int)src[2]) >> 6) & 3);            dest[1] = ((((int)src[2]) << 12) & 262143) | ((((int)src[3]) << 4) & 4095) | ((((int)src[4]) >> 4) & 15);            dest[2] = ((((int)src[4]) << 14) & 262143) | ((((int)src[5]) << 6) & 16383) | ((((int)src[6]) >> 2) & 63);            dest[3] = ((((int)src[6]) << 16) & 262143) | ((((int)src[7]) << 8) & 65535) | ((((int)src[8])) & 255);            dest[4] = ((((int)src[9]) << 10) & 262143) | ((((int)src[10]) << 2) & 1023) | ((((int)src[11]) >> 6) & 3);            dest[5] = ((((int)src[11]) << 12) & 262143) | ((((int)src[12]) << 4) & 4095) | ((((int)src[13]) >> 4) & 15);            dest[6] = ((((int)src[13]) << 14) & 262143) | ((((int)src[14]) << 6) & 16383) | ((((int)src[15]) >> 2) & 63);            dest[7] = ((((int)src[15]) << 16) & 262143) | ((((int)src[16]) << 8) & 65535) | ((((int)src[17])) & 255);        }

        private static void Pack8IntValuesBE18(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 262143) >> 10)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 262143) >> 2)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 262143) << 6)                | ((src[1] & 262143) >> 12)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 262143) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 262143) << 4)                | ((src[2] & 262143) >> 14)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 262143) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 262143) << 2)                | ((src[3] & 262143) >> 16)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 262143) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 262143))) & 255);
                            dest[9] = 
                (byte)((((src[4] & 262143) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 262143) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 262143) << 6)                | ((src[5] & 262143) >> 12)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 262143) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 262143) << 4)                | ((src[6] & 262143) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[6] & 262143) >> 6)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 262143) << 2)                | ((src[7] & 262143) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[7] & 262143) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[7] & 262143))) & 255);
                        }
        private static void Unpack8IntValuesLE19(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 524287);            dest[1] = ((((int)src[2]) >> 3) & 31) | ((((int)src[3]) << 5) & 8191) | ((((int)src[4]) << 13) & 524287);            dest[2] = ((((int)src[4]) >> 6) & 3) | ((((int)src[5]) << 2) & 1023) | ((((int)src[6]) << 10) & 262143) | ((((int)src[7]) << 18) & 524287);            dest[3] = ((((int)src[7]) >> 1) & 127) | ((((int)src[8]) << 7) & 32767) | ((((int)src[9]) << 15) & 524287);            dest[4] = ((((int)src[9]) >> 4) & 15) | ((((int)src[10]) << 4) & 4095) | ((((int)src[11]) << 12) & 524287);            dest[5] = ((((int)src[11]) >> 7) & 1) | ((((int)src[12]) << 1) & 511) | ((((int)src[13]) << 9) & 131071) | ((((int)src[14]) << 17) & 524287);            dest[6] = ((((int)src[14]) >> 2) & 63) | ((((int)src[15]) << 6) & 16383) | ((((int)src[16]) << 14) & 524287);            dest[7] = ((((int)src[16]) >> 5) & 7) | ((((int)src[17]) << 3) & 2047) | ((((int)src[18]) << 11) & 524287);        }

        private static void Pack8IntValuesLE19(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 524287))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 524287) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 524287) >> 16)                | ((src[1] & 524287) << 3)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 524287) >> 5)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 524287) >> 13)                | ((src[2] & 524287) << 6)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 524287) >> 2)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 524287) >> 10)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 524287) >> 18)                | ((src[3] & 524287) << 1)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 524287) >> 7)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 524287) >> 15)                | ((src[4] & 524287) << 4)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 524287) >> 4)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 524287) >> 12)                | ((src[5] & 524287) << 7)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 524287) >> 1)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 524287) >> 9)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 524287) >> 17)                | ((src[6] & 524287) << 2)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 524287) >> 6)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 524287) >> 14)                | ((src[7] & 524287) << 5)) & 255);
                            dest[17] = 
                (byte)((((src[7] & 524287) >> 3)) & 255);
                            dest[18] = 
                (byte)((((src[7] & 524287) >> 11)) & 255);
                        }
        private static void Unpack8IntValuesBE19(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 11) & 524287) | ((((int)src[1]) << 3) & 2047) | ((((int)src[2]) >> 5) & 7);            dest[1] = ((((int)src[2]) << 14) & 524287) | ((((int)src[3]) << 6) & 16383) | ((((int)src[4]) >> 2) & 63);            dest[2] = ((((int)src[4]) << 17) & 524287) | ((((int)src[5]) << 9) & 131071) | ((((int)src[6]) << 1) & 511) | ((((int)src[7]) >> 7) & 1);            dest[3] = ((((int)src[7]) << 12) & 524287) | ((((int)src[8]) << 4) & 4095) | ((((int)src[9]) >> 4) & 15);            dest[4] = ((((int)src[9]) << 15) & 524287) | ((((int)src[10]) << 7) & 32767) | ((((int)src[11]) >> 1) & 127);            dest[5] = ((((int)src[11]) << 18) & 524287) | ((((int)src[12]) << 10) & 262143) | ((((int)src[13]) << 2) & 1023) | ((((int)src[14]) >> 6) & 3);            dest[6] = ((((int)src[14]) << 13) & 524287) | ((((int)src[15]) << 5) & 8191) | ((((int)src[16]) >> 3) & 31);            dest[7] = ((((int)src[16]) << 16) & 524287) | ((((int)src[17]) << 8) & 65535) | ((((int)src[18])) & 255);        }

        private static void Pack8IntValuesBE19(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 524287) >> 11)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 524287) >> 3)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 524287) << 5)                | ((src[1] & 524287) >> 14)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 524287) >> 6)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 524287) << 2)                | ((src[2] & 524287) >> 17)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 524287) >> 9)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 524287) >> 1)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 524287) << 7)                | ((src[3] & 524287) >> 12)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 524287) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 524287) << 4)                | ((src[4] & 524287) >> 15)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 524287) >> 7)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 524287) << 1)                | ((src[5] & 524287) >> 18)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 524287) >> 10)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 524287) >> 2)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 524287) << 6)                | ((src[6] & 524287) >> 13)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 524287) >> 5)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 524287) << 3)                | ((src[7] & 524287) >> 16)) & 255);
                            dest[17] = 
                (byte)((((src[7] & 524287) >> 8)) & 255);
                            dest[18] = 
                (byte)((((src[7] & 524287))) & 255);
                        }
        private static void Unpack8IntValuesLE20(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 1048575);            dest[1] = ((((int)src[2]) >> 4) & 15) | ((((int)src[3]) << 4) & 4095) | ((((int)src[4]) << 12) & 1048575);            dest[2] = ((((int)src[5])) & 255) | ((((int)src[6]) << 8) & 65535) | ((((int)src[7]) << 16) & 1048575);            dest[3] = ((((int)src[7]) >> 4) & 15) | ((((int)src[8]) << 4) & 4095) | ((((int)src[9]) << 12) & 1048575);            dest[4] = ((((int)src[10])) & 255) | ((((int)src[11]) << 8) & 65535) | ((((int)src[12]) << 16) & 1048575);            dest[5] = ((((int)src[12]) >> 4) & 15) | ((((int)src[13]) << 4) & 4095) | ((((int)src[14]) << 12) & 1048575);            dest[6] = ((((int)src[15])) & 255) | ((((int)src[16]) << 8) & 65535) | ((((int)src[17]) << 16) & 1048575);            dest[7] = ((((int)src[17]) >> 4) & 15) | ((((int)src[18]) << 4) & 4095) | ((((int)src[19]) << 12) & 1048575);        }

        private static void Pack8IntValuesLE20(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1048575))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1048575) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1048575) >> 16)                | ((src[1] & 1048575) << 4)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 1048575) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 1048575) >> 12)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 1048575))) & 255);
                            dest[6] = 
                (byte)((((src[2] & 1048575) >> 8)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 1048575) >> 16)                | ((src[3] & 1048575) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 1048575) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 1048575) >> 12)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 1048575))) & 255);
                            dest[11] = 
                (byte)((((src[4] & 1048575) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 1048575) >> 16)                | ((src[5] & 1048575) << 4)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 1048575) >> 4)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 1048575) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 1048575))) & 255);
                            dest[16] = 
                (byte)((((src[6] & 1048575) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 1048575) >> 16)                | ((src[7] & 1048575) << 4)) & 255);
                            dest[18] = 
                (byte)((((src[7] & 1048575) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[7] & 1048575) >> 12)) & 255);
                        }
        private static void Unpack8IntValuesBE20(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 12) & 1048575) | ((((int)src[1]) << 4) & 4095) | ((((int)src[2]) >> 4) & 15);            dest[1] = ((((int)src[2]) << 16) & 1048575) | ((((int)src[3]) << 8) & 65535) | ((((int)src[4])) & 255);            dest[2] = ((((int)src[5]) << 12) & 1048575) | ((((int)src[6]) << 4) & 4095) | ((((int)src[7]) >> 4) & 15);            dest[3] = ((((int)src[7]) << 16) & 1048575) | ((((int)src[8]) << 8) & 65535) | ((((int)src[9])) & 255);            dest[4] = ((((int)src[10]) << 12) & 1048575) | ((((int)src[11]) << 4) & 4095) | ((((int)src[12]) >> 4) & 15);            dest[5] = ((((int)src[12]) << 16) & 1048575) | ((((int)src[13]) << 8) & 65535) | ((((int)src[14])) & 255);            dest[6] = ((((int)src[15]) << 12) & 1048575) | ((((int)src[16]) << 4) & 4095) | ((((int)src[17]) >> 4) & 15);            dest[7] = ((((int)src[17]) << 16) & 1048575) | ((((int)src[18]) << 8) & 65535) | ((((int)src[19])) & 255);        }

        private static void Pack8IntValuesBE20(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1048575) >> 12)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1048575) >> 4)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1048575) << 4)                | ((src[1] & 1048575) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 1048575) >> 8)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 1048575))) & 255);
                            dest[5] = 
                (byte)((((src[2] & 1048575) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 1048575) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 1048575) << 4)                | ((src[3] & 1048575) >> 16)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 1048575) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 1048575))) & 255);
                            dest[10] = 
                (byte)((((src[4] & 1048575) >> 12)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 1048575) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 1048575) << 4)                | ((src[5] & 1048575) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 1048575) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 1048575))) & 255);
                            dest[15] = 
                (byte)((((src[6] & 1048575) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 1048575) >> 4)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 1048575) << 4)                | ((src[7] & 1048575) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[7] & 1048575) >> 8)) & 255);
                            dest[19] = 
                (byte)((((src[7] & 1048575))) & 255);
                        }
        private static void Unpack8IntValuesLE21(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 2097151);            dest[1] = ((((int)src[2]) >> 5) & 7) | ((((int)src[3]) << 3) & 2047) | ((((int)src[4]) << 11) & 524287) | ((((int)src[5]) << 19) & 2097151);            dest[2] = ((((int)src[5]) >> 2) & 63) | ((((int)src[6]) << 6) & 16383) | ((((int)src[7]) << 14) & 2097151);            dest[3] = ((((int)src[7]) >> 7) & 1) | ((((int)src[8]) << 1) & 511) | ((((int)src[9]) << 9) & 131071) | ((((int)src[10]) << 17) & 2097151);            dest[4] = ((((int)src[10]) >> 4) & 15) | ((((int)src[11]) << 4) & 4095) | ((((int)src[12]) << 12) & 1048575) | ((((int)src[13]) << 20) & 2097151);            dest[5] = ((((int)src[13]) >> 1) & 127) | ((((int)src[14]) << 7) & 32767) | ((((int)src[15]) << 15) & 2097151);            dest[6] = ((((int)src[15]) >> 6) & 3) | ((((int)src[16]) << 2) & 1023) | ((((int)src[17]) << 10) & 262143) | ((((int)src[18]) << 18) & 2097151);            dest[7] = ((((int)src[18]) >> 3) & 31) | ((((int)src[19]) << 5) & 8191) | ((((int)src[20]) << 13) & 2097151);        }

        private static void Pack8IntValuesLE21(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2097151))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2097151) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2097151) >> 16)                | ((src[1] & 2097151) << 5)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 2097151) >> 3)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 2097151) >> 11)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 2097151) >> 19)                | ((src[2] & 2097151) << 2)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 2097151) >> 6)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 2097151) >> 14)                | ((src[3] & 2097151) << 7)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 2097151) >> 1)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 2097151) >> 9)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 2097151) >> 17)                | ((src[4] & 2097151) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 2097151) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 2097151) >> 12)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 2097151) >> 20)                | ((src[5] & 2097151) << 1)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 2097151) >> 7)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 2097151) >> 15)                | ((src[6] & 2097151) << 6)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 2097151) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 2097151) >> 10)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 2097151) >> 18)                | ((src[7] & 2097151) << 3)) & 255);
                            dest[19] = 
                (byte)((((src[7] & 2097151) >> 5)) & 255);
                            dest[20] = 
                (byte)((((src[7] & 2097151) >> 13)) & 255);
                        }
        private static void Unpack8IntValuesBE21(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 13) & 2097151) | ((((int)src[1]) << 5) & 8191) | ((((int)src[2]) >> 3) & 31);            dest[1] = ((((int)src[2]) << 18) & 2097151) | ((((int)src[3]) << 10) & 262143) | ((((int)src[4]) << 2) & 1023) | ((((int)src[5]) >> 6) & 3);            dest[2] = ((((int)src[5]) << 15) & 2097151) | ((((int)src[6]) << 7) & 32767) | ((((int)src[7]) >> 1) & 127);            dest[3] = ((((int)src[7]) << 20) & 2097151) | ((((int)src[8]) << 12) & 1048575) | ((((int)src[9]) << 4) & 4095) | ((((int)src[10]) >> 4) & 15);            dest[4] = ((((int)src[10]) << 17) & 2097151) | ((((int)src[11]) << 9) & 131071) | ((((int)src[12]) << 1) & 511) | ((((int)src[13]) >> 7) & 1);            dest[5] = ((((int)src[13]) << 14) & 2097151) | ((((int)src[14]) << 6) & 16383) | ((((int)src[15]) >> 2) & 63);            dest[6] = ((((int)src[15]) << 19) & 2097151) | ((((int)src[16]) << 11) & 524287) | ((((int)src[17]) << 3) & 2047) | ((((int)src[18]) >> 5) & 7);            dest[7] = ((((int)src[18]) << 16) & 2097151) | ((((int)src[19]) << 8) & 65535) | ((((int)src[20])) & 255);        }

        private static void Pack8IntValuesBE21(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2097151) >> 13)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2097151) >> 5)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2097151) << 3)                | ((src[1] & 2097151) >> 18)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 2097151) >> 10)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 2097151) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 2097151) << 6)                | ((src[2] & 2097151) >> 15)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 2097151) >> 7)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 2097151) << 1)                | ((src[3] & 2097151) >> 20)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 2097151) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 2097151) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 2097151) << 4)                | ((src[4] & 2097151) >> 17)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 2097151) >> 9)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 2097151) >> 1)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 2097151) << 7)                | ((src[5] & 2097151) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 2097151) >> 6)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 2097151) << 2)                | ((src[6] & 2097151) >> 19)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 2097151) >> 11)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 2097151) >> 3)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 2097151) << 5)                | ((src[7] & 2097151) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[7] & 2097151) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[7] & 2097151))) & 255);
                        }
        private static void Unpack8IntValuesLE22(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 4194303);            dest[1] = ((((int)src[2]) >> 6) & 3) | ((((int)src[3]) << 2) & 1023) | ((((int)src[4]) << 10) & 262143) | ((((int)src[5]) << 18) & 4194303);            dest[2] = ((((int)src[5]) >> 4) & 15) | ((((int)src[6]) << 4) & 4095) | ((((int)src[7]) << 12) & 1048575) | ((((int)src[8]) << 20) & 4194303);            dest[3] = ((((int)src[8]) >> 2) & 63) | ((((int)src[9]) << 6) & 16383) | ((((int)src[10]) << 14) & 4194303);            dest[4] = ((((int)src[11])) & 255) | ((((int)src[12]) << 8) & 65535) | ((((int)src[13]) << 16) & 4194303);            dest[5] = ((((int)src[13]) >> 6) & 3) | ((((int)src[14]) << 2) & 1023) | ((((int)src[15]) << 10) & 262143) | ((((int)src[16]) << 18) & 4194303);            dest[6] = ((((int)src[16]) >> 4) & 15) | ((((int)src[17]) << 4) & 4095) | ((((int)src[18]) << 12) & 1048575) | ((((int)src[19]) << 20) & 4194303);            dest[7] = ((((int)src[19]) >> 2) & 63) | ((((int)src[20]) << 6) & 16383) | ((((int)src[21]) << 14) & 4194303);        }

        private static void Pack8IntValuesLE22(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4194303))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4194303) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4194303) >> 16)                | ((src[1] & 4194303) << 6)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 4194303) >> 2)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 4194303) >> 10)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 4194303) >> 18)                | ((src[2] & 4194303) << 4)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 4194303) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 4194303) >> 12)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 4194303) >> 20)                | ((src[3] & 4194303) << 2)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 4194303) >> 6)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 4194303) >> 14)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 4194303))) & 255);
                            dest[12] = 
                (byte)((((src[4] & 4194303) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 4194303) >> 16)                | ((src[5] & 4194303) << 6)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 4194303) >> 2)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 4194303) >> 10)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 4194303) >> 18)                | ((src[6] & 4194303) << 4)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 4194303) >> 4)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 4194303) >> 12)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 4194303) >> 20)                | ((src[7] & 4194303) << 2)) & 255);
                            dest[20] = 
                (byte)((((src[7] & 4194303) >> 6)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 4194303) >> 14)) & 255);
                        }
        private static void Unpack8IntValuesBE22(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 14) & 4194303) | ((((int)src[1]) << 6) & 16383) | ((((int)src[2]) >> 2) & 63);            dest[1] = ((((int)src[2]) << 20) & 4194303) | ((((int)src[3]) << 12) & 1048575) | ((((int)src[4]) << 4) & 4095) | ((((int)src[5]) >> 4) & 15);            dest[2] = ((((int)src[5]) << 18) & 4194303) | ((((int)src[6]) << 10) & 262143) | ((((int)src[7]) << 2) & 1023) | ((((int)src[8]) >> 6) & 3);            dest[3] = ((((int)src[8]) << 16) & 4194303) | ((((int)src[9]) << 8) & 65535) | ((((int)src[10])) & 255);            dest[4] = ((((int)src[11]) << 14) & 4194303) | ((((int)src[12]) << 6) & 16383) | ((((int)src[13]) >> 2) & 63);            dest[5] = ((((int)src[13]) << 20) & 4194303) | ((((int)src[14]) << 12) & 1048575) | ((((int)src[15]) << 4) & 4095) | ((((int)src[16]) >> 4) & 15);            dest[6] = ((((int)src[16]) << 18) & 4194303) | ((((int)src[17]) << 10) & 262143) | ((((int)src[18]) << 2) & 1023) | ((((int)src[19]) >> 6) & 3);            dest[7] = ((((int)src[19]) << 16) & 4194303) | ((((int)src[20]) << 8) & 65535) | ((((int)src[21])) & 255);        }

        private static void Pack8IntValuesBE22(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4194303) >> 14)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4194303) >> 6)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4194303) << 2)                | ((src[1] & 4194303) >> 20)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 4194303) >> 12)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 4194303) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 4194303) << 4)                | ((src[2] & 4194303) >> 18)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 4194303) >> 10)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 4194303) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 4194303) << 6)                | ((src[3] & 4194303) >> 16)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 4194303) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 4194303))) & 255);
                            dest[11] = 
                (byte)((((src[4] & 4194303) >> 14)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 4194303) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 4194303) << 2)                | ((src[5] & 4194303) >> 20)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 4194303) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 4194303) >> 4)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 4194303) << 4)                | ((src[6] & 4194303) >> 18)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 4194303) >> 10)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 4194303) >> 2)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 4194303) << 6)                | ((src[7] & 4194303) >> 16)) & 255);
                            dest[20] = 
                (byte)((((src[7] & 4194303) >> 8)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 4194303))) & 255);
                        }
        private static void Unpack8IntValuesLE23(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 8388607);            dest[1] = ((((int)src[2]) >> 7) & 1) | ((((int)src[3]) << 1) & 511) | ((((int)src[4]) << 9) & 131071) | ((((int)src[5]) << 17) & 8388607);            dest[2] = ((((int)src[5]) >> 6) & 3) | ((((int)src[6]) << 2) & 1023) | ((((int)src[7]) << 10) & 262143) | ((((int)src[8]) << 18) & 8388607);            dest[3] = ((((int)src[8]) >> 5) & 7) | ((((int)src[9]) << 3) & 2047) | ((((int)src[10]) << 11) & 524287) | ((((int)src[11]) << 19) & 8388607);            dest[4] = ((((int)src[11]) >> 4) & 15) | ((((int)src[12]) << 4) & 4095) | ((((int)src[13]) << 12) & 1048575) | ((((int)src[14]) << 20) & 8388607);            dest[5] = ((((int)src[14]) >> 3) & 31) | ((((int)src[15]) << 5) & 8191) | ((((int)src[16]) << 13) & 2097151) | ((((int)src[17]) << 21) & 8388607);            dest[6] = ((((int)src[17]) >> 2) & 63) | ((((int)src[18]) << 6) & 16383) | ((((int)src[19]) << 14) & 4194303) | ((((int)src[20]) << 22) & 8388607);            dest[7] = ((((int)src[20]) >> 1) & 127) | ((((int)src[21]) << 7) & 32767) | ((((int)src[22]) << 15) & 8388607);        }

        private static void Pack8IntValuesLE23(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8388607))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8388607) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 8388607) >> 16)                | ((src[1] & 8388607) << 7)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 8388607) >> 1)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 8388607) >> 9)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 8388607) >> 17)                | ((src[2] & 8388607) << 6)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 8388607) >> 2)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 8388607) >> 10)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 8388607) >> 18)                | ((src[3] & 8388607) << 5)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 8388607) >> 3)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 8388607) >> 11)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 8388607) >> 19)                | ((src[4] & 8388607) << 4)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 8388607) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 8388607) >> 12)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 8388607) >> 20)                | ((src[5] & 8388607) << 3)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 8388607) >> 5)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 8388607) >> 13)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 8388607) >> 21)                | ((src[6] & 8388607) << 2)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 8388607) >> 6)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 8388607) >> 14)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 8388607) >> 22)                | ((src[7] & 8388607) << 1)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 8388607) >> 7)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 8388607) >> 15)) & 255);
                        }
        private static void Unpack8IntValuesBE23(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 15) & 8388607) | ((((int)src[1]) << 7) & 32767) | ((((int)src[2]) >> 1) & 127);            dest[1] = ((((int)src[2]) << 22) & 8388607) | ((((int)src[3]) << 14) & 4194303) | ((((int)src[4]) << 6) & 16383) | ((((int)src[5]) >> 2) & 63);            dest[2] = ((((int)src[5]) << 21) & 8388607) | ((((int)src[6]) << 13) & 2097151) | ((((int)src[7]) << 5) & 8191) | ((((int)src[8]) >> 3) & 31);            dest[3] = ((((int)src[8]) << 20) & 8388607) | ((((int)src[9]) << 12) & 1048575) | ((((int)src[10]) << 4) & 4095) | ((((int)src[11]) >> 4) & 15);            dest[4] = ((((int)src[11]) << 19) & 8388607) | ((((int)src[12]) << 11) & 524287) | ((((int)src[13]) << 3) & 2047) | ((((int)src[14]) >> 5) & 7);            dest[5] = ((((int)src[14]) << 18) & 8388607) | ((((int)src[15]) << 10) & 262143) | ((((int)src[16]) << 2) & 1023) | ((((int)src[17]) >> 6) & 3);            dest[6] = ((((int)src[17]) << 17) & 8388607) | ((((int)src[18]) << 9) & 131071) | ((((int)src[19]) << 1) & 511) | ((((int)src[20]) >> 7) & 1);            dest[7] = ((((int)src[20]) << 16) & 8388607) | ((((int)src[21]) << 8) & 65535) | ((((int)src[22])) & 255);        }

        private static void Pack8IntValuesBE23(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8388607) >> 15)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8388607) >> 7)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 8388607) << 1)                | ((src[1] & 8388607) >> 22)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 8388607) >> 14)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 8388607) >> 6)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 8388607) << 2)                | ((src[2] & 8388607) >> 21)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 8388607) >> 13)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 8388607) >> 5)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 8388607) << 3)                | ((src[3] & 8388607) >> 20)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 8388607) >> 12)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 8388607) >> 4)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 8388607) << 4)                | ((src[4] & 8388607) >> 19)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 8388607) >> 11)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 8388607) >> 3)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 8388607) << 5)                | ((src[5] & 8388607) >> 18)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 8388607) >> 10)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 8388607) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 8388607) << 6)                | ((src[6] & 8388607) >> 17)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 8388607) >> 9)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 8388607) >> 1)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 8388607) << 7)                | ((src[7] & 8388607) >> 16)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 8388607) >> 8)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 8388607))) & 255);
                        }
        private static void Unpack8IntValuesLE24(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215);            dest[1] = ((((int)src[3])) & 255) | ((((int)src[4]) << 8) & 65535) | ((((int)src[5]) << 16) & 16777215);            dest[2] = ((((int)src[6])) & 255) | ((((int)src[7]) << 8) & 65535) | ((((int)src[8]) << 16) & 16777215);            dest[3] = ((((int)src[9])) & 255) | ((((int)src[10]) << 8) & 65535) | ((((int)src[11]) << 16) & 16777215);            dest[4] = ((((int)src[12])) & 255) | ((((int)src[13]) << 8) & 65535) | ((((int)src[14]) << 16) & 16777215);            dest[5] = ((((int)src[15])) & 255) | ((((int)src[16]) << 8) & 65535) | ((((int)src[17]) << 16) & 16777215);            dest[6] = ((((int)src[18])) & 255) | ((((int)src[19]) << 8) & 65535) | ((((int)src[20]) << 16) & 16777215);            dest[7] = ((((int)src[21])) & 255) | ((((int)src[22]) << 8) & 65535) | ((((int)src[23]) << 16) & 16777215);        }

        private static void Pack8IntValuesLE24(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 16777215))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 16777215) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 16777215) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 16777215))) & 255);
                            dest[4] = 
                (byte)((((src[1] & 16777215) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 16777215) >> 16)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 16777215))) & 255);
                            dest[7] = 
                (byte)((((src[2] & 16777215) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 16777215) >> 16)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 16777215))) & 255);
                            dest[10] = 
                (byte)((((src[3] & 16777215) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 16777215) >> 16)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 16777215))) & 255);
                            dest[13] = 
                (byte)((((src[4] & 16777215) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 16777215) >> 16)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 16777215))) & 255);
                            dest[16] = 
                (byte)((((src[5] & 16777215) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 16777215) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 16777215))) & 255);
                            dest[19] = 
                (byte)((((src[6] & 16777215) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 16777215) >> 16)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 16777215))) & 255);
                            dest[22] = 
                (byte)((((src[7] & 16777215) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 16777215) >> 16)) & 255);
                        }
        private static void Unpack8IntValuesBE24(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 16) & 16777215) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2])) & 255);            dest[1] = ((((int)src[3]) << 16) & 16777215) | ((((int)src[4]) << 8) & 65535) | ((((int)src[5])) & 255);            dest[2] = ((((int)src[6]) << 16) & 16777215) | ((((int)src[7]) << 8) & 65535) | ((((int)src[8])) & 255);            dest[3] = ((((int)src[9]) << 16) & 16777215) | ((((int)src[10]) << 8) & 65535) | ((((int)src[11])) & 255);            dest[4] = ((((int)src[12]) << 16) & 16777215) | ((((int)src[13]) << 8) & 65535) | ((((int)src[14])) & 255);            dest[5] = ((((int)src[15]) << 16) & 16777215) | ((((int)src[16]) << 8) & 65535) | ((((int)src[17])) & 255);            dest[6] = ((((int)src[18]) << 16) & 16777215) | ((((int)src[19]) << 8) & 65535) | ((((int)src[20])) & 255);            dest[7] = ((((int)src[21]) << 16) & 16777215) | ((((int)src[22]) << 8) & 65535) | ((((int)src[23])) & 255);        }

        private static void Pack8IntValuesBE24(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 16777215) >> 16)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 16777215) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 16777215))) & 255);
                            dest[3] = 
                (byte)((((src[1] & 16777215) >> 16)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 16777215) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 16777215))) & 255);
                            dest[6] = 
                (byte)((((src[2] & 16777215) >> 16)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 16777215) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 16777215))) & 255);
                            dest[9] = 
                (byte)((((src[3] & 16777215) >> 16)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 16777215) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 16777215))) & 255);
                            dest[12] = 
                (byte)((((src[4] & 16777215) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 16777215) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 16777215))) & 255);
                            dest[15] = 
                (byte)((((src[5] & 16777215) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 16777215) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 16777215))) & 255);
                            dest[18] = 
                (byte)((((src[6] & 16777215) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 16777215) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 16777215))) & 255);
                            dest[21] = 
                (byte)((((src[7] & 16777215) >> 16)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 16777215) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 16777215))) & 255);
                        }
        private static void Unpack8IntValuesLE25(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215) | ((((int)src[3]) << 24) & 33554431);            dest[1] = ((((int)src[3]) >> 1) & 127) | ((((int)src[4]) << 7) & 32767) | ((((int)src[5]) << 15) & 8388607) | ((((int)src[6]) << 23) & 33554431);            dest[2] = ((((int)src[6]) >> 2) & 63) | ((((int)src[7]) << 6) & 16383) | ((((int)src[8]) << 14) & 4194303) | ((((int)src[9]) << 22) & 33554431);            dest[3] = ((((int)src[9]) >> 3) & 31) | ((((int)src[10]) << 5) & 8191) | ((((int)src[11]) << 13) & 2097151) | ((((int)src[12]) << 21) & 33554431);            dest[4] = ((((int)src[12]) >> 4) & 15) | ((((int)src[13]) << 4) & 4095) | ((((int)src[14]) << 12) & 1048575) | ((((int)src[15]) << 20) & 33554431);            dest[5] = ((((int)src[15]) >> 5) & 7) | ((((int)src[16]) << 3) & 2047) | ((((int)src[17]) << 11) & 524287) | ((((int)src[18]) << 19) & 33554431);            dest[6] = ((((int)src[18]) >> 6) & 3) | ((((int)src[19]) << 2) & 1023) | ((((int)src[20]) << 10) & 262143) | ((((int)src[21]) << 18) & 33554431);            dest[7] = ((((int)src[21]) >> 7) & 1) | ((((int)src[22]) << 1) & 511) | ((((int)src[23]) << 9) & 131071) | ((((int)src[24]) << 17) & 33554431);        }

        private static void Pack8IntValuesLE25(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 33554431))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 33554431) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 33554431) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 33554431) >> 24)                | ((src[1] & 33554431) << 1)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 33554431) >> 7)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 33554431) >> 15)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 33554431) >> 23)                | ((src[2] & 33554431) << 2)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 33554431) >> 6)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 33554431) >> 14)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 33554431) >> 22)                | ((src[3] & 33554431) << 3)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 33554431) >> 5)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 33554431) >> 13)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 33554431) >> 21)                | ((src[4] & 33554431) << 4)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 33554431) >> 4)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 33554431) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 33554431) >> 20)                | ((src[5] & 33554431) << 5)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 33554431) >> 3)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 33554431) >> 11)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 33554431) >> 19)                | ((src[6] & 33554431) << 6)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 33554431) >> 2)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 33554431) >> 10)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 33554431) >> 18)                | ((src[7] & 33554431) << 7)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 33554431) >> 1)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 33554431) >> 9)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 33554431) >> 17)) & 255);
                        }
        private static void Unpack8IntValuesBE25(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 17) & 33554431) | ((((int)src[1]) << 9) & 131071) | ((((int)src[2]) << 1) & 511) | ((((int)src[3]) >> 7) & 1);            dest[1] = ((((int)src[3]) << 18) & 33554431) | ((((int)src[4]) << 10) & 262143) | ((((int)src[5]) << 2) & 1023) | ((((int)src[6]) >> 6) & 3);            dest[2] = ((((int)src[6]) << 19) & 33554431) | ((((int)src[7]) << 11) & 524287) | ((((int)src[8]) << 3) & 2047) | ((((int)src[9]) >> 5) & 7);            dest[3] = ((((int)src[9]) << 20) & 33554431) | ((((int)src[10]) << 12) & 1048575) | ((((int)src[11]) << 4) & 4095) | ((((int)src[12]) >> 4) & 15);            dest[4] = ((((int)src[12]) << 21) & 33554431) | ((((int)src[13]) << 13) & 2097151) | ((((int)src[14]) << 5) & 8191) | ((((int)src[15]) >> 3) & 31);            dest[5] = ((((int)src[15]) << 22) & 33554431) | ((((int)src[16]) << 14) & 4194303) | ((((int)src[17]) << 6) & 16383) | ((((int)src[18]) >> 2) & 63);            dest[6] = ((((int)src[18]) << 23) & 33554431) | ((((int)src[19]) << 15) & 8388607) | ((((int)src[20]) << 7) & 32767) | ((((int)src[21]) >> 1) & 127);            dest[7] = ((((int)src[21]) << 24) & 33554431) | ((((int)src[22]) << 16) & 16777215) | ((((int)src[23]) << 8) & 65535) | ((((int)src[24])) & 255);        }

        private static void Pack8IntValuesBE25(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 33554431) >> 17)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 33554431) >> 9)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 33554431) >> 1)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 33554431) << 7)                | ((src[1] & 33554431) >> 18)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 33554431) >> 10)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 33554431) >> 2)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 33554431) << 6)                | ((src[2] & 33554431) >> 19)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 33554431) >> 11)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 33554431) >> 3)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 33554431) << 5)                | ((src[3] & 33554431) >> 20)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 33554431) >> 12)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 33554431) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 33554431) << 4)                | ((src[4] & 33554431) >> 21)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 33554431) >> 13)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 33554431) >> 5)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 33554431) << 3)                | ((src[5] & 33554431) >> 22)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 33554431) >> 14)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 33554431) >> 6)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 33554431) << 2)                | ((src[6] & 33554431) >> 23)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 33554431) >> 15)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 33554431) >> 7)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 33554431) << 1)                | ((src[7] & 33554431) >> 24)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 33554431) >> 16)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 33554431) >> 8)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 33554431))) & 255);
                        }
        private static void Unpack8IntValuesLE26(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215) | ((((int)src[3]) << 24) & 67108863);            dest[1] = ((((int)src[3]) >> 2) & 63) | ((((int)src[4]) << 6) & 16383) | ((((int)src[5]) << 14) & 4194303) | ((((int)src[6]) << 22) & 67108863);            dest[2] = ((((int)src[6]) >> 4) & 15) | ((((int)src[7]) << 4) & 4095) | ((((int)src[8]) << 12) & 1048575) | ((((int)src[9]) << 20) & 67108863);            dest[3] = ((((int)src[9]) >> 6) & 3) | ((((int)src[10]) << 2) & 1023) | ((((int)src[11]) << 10) & 262143) | ((((int)src[12]) << 18) & 67108863);            dest[4] = ((((int)src[13])) & 255) | ((((int)src[14]) << 8) & 65535) | ((((int)src[15]) << 16) & 16777215) | ((((int)src[16]) << 24) & 67108863);            dest[5] = ((((int)src[16]) >> 2) & 63) | ((((int)src[17]) << 6) & 16383) | ((((int)src[18]) << 14) & 4194303) | ((((int)src[19]) << 22) & 67108863);            dest[6] = ((((int)src[19]) >> 4) & 15) | ((((int)src[20]) << 4) & 4095) | ((((int)src[21]) << 12) & 1048575) | ((((int)src[22]) << 20) & 67108863);            dest[7] = ((((int)src[22]) >> 6) & 3) | ((((int)src[23]) << 2) & 1023) | ((((int)src[24]) << 10) & 262143) | ((((int)src[25]) << 18) & 67108863);        }

        private static void Pack8IntValuesLE26(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 67108863))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 67108863) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 67108863) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 67108863) >> 24)                | ((src[1] & 67108863) << 2)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 67108863) >> 6)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 67108863) >> 14)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 67108863) >> 22)                | ((src[2] & 67108863) << 4)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 67108863) >> 4)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 67108863) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 67108863) >> 20)                | ((src[3] & 67108863) << 6)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 67108863) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 67108863) >> 10)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 67108863) >> 18)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 67108863))) & 255);
                            dest[14] = 
                (byte)((((src[4] & 67108863) >> 8)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 67108863) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 67108863) >> 24)                | ((src[5] & 67108863) << 2)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 67108863) >> 6)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 67108863) >> 14)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 67108863) >> 22)                | ((src[6] & 67108863) << 4)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 67108863) >> 4)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 67108863) >> 12)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 67108863) >> 20)                | ((src[7] & 67108863) << 6)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 67108863) >> 2)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 67108863) >> 10)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 67108863) >> 18)) & 255);
                        }
        private static void Unpack8IntValuesBE26(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 18) & 67108863) | ((((int)src[1]) << 10) & 262143) | ((((int)src[2]) << 2) & 1023) | ((((int)src[3]) >> 6) & 3);            dest[1] = ((((int)src[3]) << 20) & 67108863) | ((((int)src[4]) << 12) & 1048575) | ((((int)src[5]) << 4) & 4095) | ((((int)src[6]) >> 4) & 15);            dest[2] = ((((int)src[6]) << 22) & 67108863) | ((((int)src[7]) << 14) & 4194303) | ((((int)src[8]) << 6) & 16383) | ((((int)src[9]) >> 2) & 63);            dest[3] = ((((int)src[9]) << 24) & 67108863) | ((((int)src[10]) << 16) & 16777215) | ((((int)src[11]) << 8) & 65535) | ((((int)src[12])) & 255);            dest[4] = ((((int)src[13]) << 18) & 67108863) | ((((int)src[14]) << 10) & 262143) | ((((int)src[15]) << 2) & 1023) | ((((int)src[16]) >> 6) & 3);            dest[5] = ((((int)src[16]) << 20) & 67108863) | ((((int)src[17]) << 12) & 1048575) | ((((int)src[18]) << 4) & 4095) | ((((int)src[19]) >> 4) & 15);            dest[6] = ((((int)src[19]) << 22) & 67108863) | ((((int)src[20]) << 14) & 4194303) | ((((int)src[21]) << 6) & 16383) | ((((int)src[22]) >> 2) & 63);            dest[7] = ((((int)src[22]) << 24) & 67108863) | ((((int)src[23]) << 16) & 16777215) | ((((int)src[24]) << 8) & 65535) | ((((int)src[25])) & 255);        }

        private static void Pack8IntValuesBE26(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 67108863) >> 18)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 67108863) >> 10)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 67108863) >> 2)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 67108863) << 6)                | ((src[1] & 67108863) >> 20)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 67108863) >> 12)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 67108863) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 67108863) << 4)                | ((src[2] & 67108863) >> 22)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 67108863) >> 14)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 67108863) >> 6)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 67108863) << 2)                | ((src[3] & 67108863) >> 24)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 67108863) >> 16)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 67108863) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 67108863))) & 255);
                            dest[13] = 
                (byte)((((src[4] & 67108863) >> 18)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 67108863) >> 10)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 67108863) >> 2)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 67108863) << 6)                | ((src[5] & 67108863) >> 20)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 67108863) >> 12)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 67108863) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 67108863) << 4)                | ((src[6] & 67108863) >> 22)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 67108863) >> 14)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 67108863) >> 6)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 67108863) << 2)                | ((src[7] & 67108863) >> 24)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 67108863) >> 16)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 67108863) >> 8)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 67108863))) & 255);
                        }
        private static void Unpack8IntValuesLE27(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215) | ((((int)src[3]) << 24) & 134217727);            dest[1] = ((((int)src[3]) >> 3) & 31) | ((((int)src[4]) << 5) & 8191) | ((((int)src[5]) << 13) & 2097151) | ((((int)src[6]) << 21) & 134217727);            dest[2] = ((((int)src[6]) >> 6) & 3) | ((((int)src[7]) << 2) & 1023) | ((((int)src[8]) << 10) & 262143) | ((((int)src[9]) << 18) & 67108863) | ((((int)src[10]) << 26) & 134217727);            dest[3] = ((((int)src[10]) >> 1) & 127) | ((((int)src[11]) << 7) & 32767) | ((((int)src[12]) << 15) & 8388607) | ((((int)src[13]) << 23) & 134217727);            dest[4] = ((((int)src[13]) >> 4) & 15) | ((((int)src[14]) << 4) & 4095) | ((((int)src[15]) << 12) & 1048575) | ((((int)src[16]) << 20) & 134217727);            dest[5] = ((((int)src[16]) >> 7) & 1) | ((((int)src[17]) << 1) & 511) | ((((int)src[18]) << 9) & 131071) | ((((int)src[19]) << 17) & 33554431) | ((((int)src[20]) << 25) & 134217727);            dest[6] = ((((int)src[20]) >> 2) & 63) | ((((int)src[21]) << 6) & 16383) | ((((int)src[22]) << 14) & 4194303) | ((((int)src[23]) << 22) & 134217727);            dest[7] = ((((int)src[23]) >> 5) & 7) | ((((int)src[24]) << 3) & 2047) | ((((int)src[25]) << 11) & 524287) | ((((int)src[26]) << 19) & 134217727);        }

        private static void Pack8IntValuesLE27(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 134217727))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 134217727) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 134217727) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 134217727) >> 24)                | ((src[1] & 134217727) << 3)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 134217727) >> 5)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 134217727) >> 13)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 134217727) >> 21)                | ((src[2] & 134217727) << 6)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 134217727) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 134217727) >> 10)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 134217727) >> 18)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 134217727) >> 26)                | ((src[3] & 134217727) << 1)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 134217727) >> 7)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 134217727) >> 15)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 134217727) >> 23)                | ((src[4] & 134217727) << 4)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 134217727) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 134217727) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 134217727) >> 20)                | ((src[5] & 134217727) << 7)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 134217727) >> 1)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 134217727) >> 9)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 134217727) >> 17)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 134217727) >> 25)                | ((src[6] & 134217727) << 2)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 134217727) >> 6)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 134217727) >> 14)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 134217727) >> 22)                | ((src[7] & 134217727) << 5)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 134217727) >> 3)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 134217727) >> 11)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 134217727) >> 19)) & 255);
                        }
        private static void Unpack8IntValuesBE27(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 19) & 134217727) | ((((int)src[1]) << 11) & 524287) | ((((int)src[2]) << 3) & 2047) | ((((int)src[3]) >> 5) & 7);            dest[1] = ((((int)src[3]) << 22) & 134217727) | ((((int)src[4]) << 14) & 4194303) | ((((int)src[5]) << 6) & 16383) | ((((int)src[6]) >> 2) & 63);            dest[2] = ((((int)src[6]) << 25) & 134217727) | ((((int)src[7]) << 17) & 33554431) | ((((int)src[8]) << 9) & 131071) | ((((int)src[9]) << 1) & 511) | ((((int)src[10]) >> 7) & 1);            dest[3] = ((((int)src[10]) << 20) & 134217727) | ((((int)src[11]) << 12) & 1048575) | ((((int)src[12]) << 4) & 4095) | ((((int)src[13]) >> 4) & 15);            dest[4] = ((((int)src[13]) << 23) & 134217727) | ((((int)src[14]) << 15) & 8388607) | ((((int)src[15]) << 7) & 32767) | ((((int)src[16]) >> 1) & 127);            dest[5] = ((((int)src[16]) << 26) & 134217727) | ((((int)src[17]) << 18) & 67108863) | ((((int)src[18]) << 10) & 262143) | ((((int)src[19]) << 2) & 1023) | ((((int)src[20]) >> 6) & 3);            dest[6] = ((((int)src[20]) << 21) & 134217727) | ((((int)src[21]) << 13) & 2097151) | ((((int)src[22]) << 5) & 8191) | ((((int)src[23]) >> 3) & 31);            dest[7] = ((((int)src[23]) << 24) & 134217727) | ((((int)src[24]) << 16) & 16777215) | ((((int)src[25]) << 8) & 65535) | ((((int)src[26])) & 255);        }

        private static void Pack8IntValuesBE27(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 134217727) >> 19)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 134217727) >> 11)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 134217727) >> 3)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 134217727) << 5)                | ((src[1] & 134217727) >> 22)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 134217727) >> 14)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 134217727) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 134217727) << 2)                | ((src[2] & 134217727) >> 25)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 134217727) >> 17)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 134217727) >> 9)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 134217727) >> 1)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 134217727) << 7)                | ((src[3] & 134217727) >> 20)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 134217727) >> 12)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 134217727) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 134217727) << 4)                | ((src[4] & 134217727) >> 23)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 134217727) >> 15)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 134217727) >> 7)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 134217727) << 1)                | ((src[5] & 134217727) >> 26)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 134217727) >> 18)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 134217727) >> 10)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 134217727) >> 2)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 134217727) << 6)                | ((src[6] & 134217727) >> 21)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 134217727) >> 13)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 134217727) >> 5)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 134217727) << 3)                | ((src[7] & 134217727) >> 24)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 134217727) >> 16)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 134217727) >> 8)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 134217727))) & 255);
                        }
        private static void Unpack8IntValuesLE28(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215) | ((((int)src[3]) << 24) & 268435455);            dest[1] = ((((int)src[3]) >> 4) & 15) | ((((int)src[4]) << 4) & 4095) | ((((int)src[5]) << 12) & 1048575) | ((((int)src[6]) << 20) & 268435455);            dest[2] = ((((int)src[7])) & 255) | ((((int)src[8]) << 8) & 65535) | ((((int)src[9]) << 16) & 16777215) | ((((int)src[10]) << 24) & 268435455);            dest[3] = ((((int)src[10]) >> 4) & 15) | ((((int)src[11]) << 4) & 4095) | ((((int)src[12]) << 12) & 1048575) | ((((int)src[13]) << 20) & 268435455);            dest[4] = ((((int)src[14])) & 255) | ((((int)src[15]) << 8) & 65535) | ((((int)src[16]) << 16) & 16777215) | ((((int)src[17]) << 24) & 268435455);            dest[5] = ((((int)src[17]) >> 4) & 15) | ((((int)src[18]) << 4) & 4095) | ((((int)src[19]) << 12) & 1048575) | ((((int)src[20]) << 20) & 268435455);            dest[6] = ((((int)src[21])) & 255) | ((((int)src[22]) << 8) & 65535) | ((((int)src[23]) << 16) & 16777215) | ((((int)src[24]) << 24) & 268435455);            dest[7] = ((((int)src[24]) >> 4) & 15) | ((((int)src[25]) << 4) & 4095) | ((((int)src[26]) << 12) & 1048575) | ((((int)src[27]) << 20) & 268435455);        }

        private static void Pack8IntValuesLE28(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 268435455))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 268435455) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 268435455) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 268435455) >> 24)                | ((src[1] & 268435455) << 4)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 268435455) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 268435455) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 268435455) >> 20)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 268435455))) & 255);
                            dest[8] = 
                (byte)((((src[2] & 268435455) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 268435455) >> 16)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 268435455) >> 24)                | ((src[3] & 268435455) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 268435455) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 268435455) >> 12)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 268435455) >> 20)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 268435455))) & 255);
                            dest[15] = 
                (byte)((((src[4] & 268435455) >> 8)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 268435455) >> 16)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 268435455) >> 24)                | ((src[5] & 268435455) << 4)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 268435455) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 268435455) >> 12)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 268435455) >> 20)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 268435455))) & 255);
                            dest[22] = 
                (byte)((((src[6] & 268435455) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 268435455) >> 16)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 268435455) >> 24)                | ((src[7] & 268435455) << 4)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 268435455) >> 4)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 268435455) >> 12)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 268435455) >> 20)) & 255);
                        }
        private static void Unpack8IntValuesBE28(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 20) & 268435455) | ((((int)src[1]) << 12) & 1048575) | ((((int)src[2]) << 4) & 4095) | ((((int)src[3]) >> 4) & 15);            dest[1] = ((((int)src[3]) << 24) & 268435455) | ((((int)src[4]) << 16) & 16777215) | ((((int)src[5]) << 8) & 65535) | ((((int)src[6])) & 255);            dest[2] = ((((int)src[7]) << 20) & 268435455) | ((((int)src[8]) << 12) & 1048575) | ((((int)src[9]) << 4) & 4095) | ((((int)src[10]) >> 4) & 15);            dest[3] = ((((int)src[10]) << 24) & 268435455) | ((((int)src[11]) << 16) & 16777215) | ((((int)src[12]) << 8) & 65535) | ((((int)src[13])) & 255);            dest[4] = ((((int)src[14]) << 20) & 268435455) | ((((int)src[15]) << 12) & 1048575) | ((((int)src[16]) << 4) & 4095) | ((((int)src[17]) >> 4) & 15);            dest[5] = ((((int)src[17]) << 24) & 268435455) | ((((int)src[18]) << 16) & 16777215) | ((((int)src[19]) << 8) & 65535) | ((((int)src[20])) & 255);            dest[6] = ((((int)src[21]) << 20) & 268435455) | ((((int)src[22]) << 12) & 1048575) | ((((int)src[23]) << 4) & 4095) | ((((int)src[24]) >> 4) & 15);            dest[7] = ((((int)src[24]) << 24) & 268435455) | ((((int)src[25]) << 16) & 16777215) | ((((int)src[26]) << 8) & 65535) | ((((int)src[27])) & 255);        }

        private static void Pack8IntValuesBE28(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 268435455) >> 20)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 268435455) >> 12)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 268435455) >> 4)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 268435455) << 4)                | ((src[1] & 268435455) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 268435455) >> 16)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 268435455) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 268435455))) & 255);
                            dest[7] = 
                (byte)((((src[2] & 268435455) >> 20)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 268435455) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 268435455) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 268435455) << 4)                | ((src[3] & 268435455) >> 24)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 268435455) >> 16)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 268435455) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 268435455))) & 255);
                            dest[14] = 
                (byte)((((src[4] & 268435455) >> 20)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 268435455) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 268435455) >> 4)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 268435455) << 4)                | ((src[5] & 268435455) >> 24)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 268435455) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 268435455) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 268435455))) & 255);
                            dest[21] = 
                (byte)((((src[6] & 268435455) >> 20)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 268435455) >> 12)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 268435455) >> 4)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 268435455) << 4)                | ((src[7] & 268435455) >> 24)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 268435455) >> 16)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 268435455) >> 8)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 268435455))) & 255);
                        }
        private static void Unpack8IntValuesLE29(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215) | ((((int)src[3]) << 24) & 536870911);            dest[1] = ((((int)src[3]) >> 5) & 7) | ((((int)src[4]) << 3) & 2047) | ((((int)src[5]) << 11) & 524287) | ((((int)src[6]) << 19) & 134217727) | ((((int)src[7]) << 27) & 536870911);            dest[2] = ((((int)src[7]) >> 2) & 63) | ((((int)src[8]) << 6) & 16383) | ((((int)src[9]) << 14) & 4194303) | ((((int)src[10]) << 22) & 536870911);            dest[3] = ((((int)src[10]) >> 7) & 1) | ((((int)src[11]) << 1) & 511) | ((((int)src[12]) << 9) & 131071) | ((((int)src[13]) << 17) & 33554431) | ((((int)src[14]) << 25) & 536870911);            dest[4] = ((((int)src[14]) >> 4) & 15) | ((((int)src[15]) << 4) & 4095) | ((((int)src[16]) << 12) & 1048575) | ((((int)src[17]) << 20) & 268435455) | ((((int)src[18]) << 28) & 536870911);            dest[5] = ((((int)src[18]) >> 1) & 127) | ((((int)src[19]) << 7) & 32767) | ((((int)src[20]) << 15) & 8388607) | ((((int)src[21]) << 23) & 536870911);            dest[6] = ((((int)src[21]) >> 6) & 3) | ((((int)src[22]) << 2) & 1023) | ((((int)src[23]) << 10) & 262143) | ((((int)src[24]) << 18) & 67108863) | ((((int)src[25]) << 26) & 536870911);            dest[7] = ((((int)src[25]) >> 3) & 31) | ((((int)src[26]) << 5) & 8191) | ((((int)src[27]) << 13) & 2097151) | ((((int)src[28]) << 21) & 536870911);        }

        private static void Pack8IntValuesLE29(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 536870911))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 536870911) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 536870911) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 536870911) >> 24)                | ((src[1] & 536870911) << 5)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 536870911) >> 3)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 536870911) >> 11)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 536870911) >> 19)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 536870911) >> 27)                | ((src[2] & 536870911) << 2)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 536870911) >> 6)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 536870911) >> 14)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 536870911) >> 22)                | ((src[3] & 536870911) << 7)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 536870911) >> 1)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 536870911) >> 9)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 536870911) >> 17)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 536870911) >> 25)                | ((src[4] & 536870911) << 4)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 536870911) >> 4)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 536870911) >> 12)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 536870911) >> 20)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 536870911) >> 28)                | ((src[5] & 536870911) << 1)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 536870911) >> 7)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 536870911) >> 15)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 536870911) >> 23)                | ((src[6] & 536870911) << 6)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 536870911) >> 2)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 536870911) >> 10)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 536870911) >> 18)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 536870911) >> 26)                | ((src[7] & 536870911) << 3)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 536870911) >> 5)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 536870911) >> 13)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 536870911) >> 21)) & 255);
                        }
        private static void Unpack8IntValuesBE29(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 21) & 536870911) | ((((int)src[1]) << 13) & 2097151) | ((((int)src[2]) << 5) & 8191) | ((((int)src[3]) >> 3) & 31);            dest[1] = ((((int)src[3]) << 26) & 536870911) | ((((int)src[4]) << 18) & 67108863) | ((((int)src[5]) << 10) & 262143) | ((((int)src[6]) << 2) & 1023) | ((((int)src[7]) >> 6) & 3);            dest[2] = ((((int)src[7]) << 23) & 536870911) | ((((int)src[8]) << 15) & 8388607) | ((((int)src[9]) << 7) & 32767) | ((((int)src[10]) >> 1) & 127);            dest[3] = ((((int)src[10]) << 28) & 536870911) | ((((int)src[11]) << 20) & 268435455) | ((((int)src[12]) << 12) & 1048575) | ((((int)src[13]) << 4) & 4095) | ((((int)src[14]) >> 4) & 15);            dest[4] = ((((int)src[14]) << 25) & 536870911) | ((((int)src[15]) << 17) & 33554431) | ((((int)src[16]) << 9) & 131071) | ((((int)src[17]) << 1) & 511) | ((((int)src[18]) >> 7) & 1);            dest[5] = ((((int)src[18]) << 22) & 536870911) | ((((int)src[19]) << 14) & 4194303) | ((((int)src[20]) << 6) & 16383) | ((((int)src[21]) >> 2) & 63);            dest[6] = ((((int)src[21]) << 27) & 536870911) | ((((int)src[22]) << 19) & 134217727) | ((((int)src[23]) << 11) & 524287) | ((((int)src[24]) << 3) & 2047) | ((((int)src[25]) >> 5) & 7);            dest[7] = ((((int)src[25]) << 24) & 536870911) | ((((int)src[26]) << 16) & 16777215) | ((((int)src[27]) << 8) & 65535) | ((((int)src[28])) & 255);        }

        private static void Pack8IntValuesBE29(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 536870911) >> 21)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 536870911) >> 13)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 536870911) >> 5)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 536870911) << 3)                | ((src[1] & 536870911) >> 26)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 536870911) >> 18)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 536870911) >> 10)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 536870911) >> 2)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 536870911) << 6)                | ((src[2] & 536870911) >> 23)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 536870911) >> 15)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 536870911) >> 7)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 536870911) << 1)                | ((src[3] & 536870911) >> 28)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 536870911) >> 20)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 536870911) >> 12)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 536870911) >> 4)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 536870911) << 4)                | ((src[4] & 536870911) >> 25)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 536870911) >> 17)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 536870911) >> 9)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 536870911) >> 1)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 536870911) << 7)                | ((src[5] & 536870911) >> 22)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 536870911) >> 14)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 536870911) >> 6)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 536870911) << 2)                | ((src[6] & 536870911) >> 27)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 536870911) >> 19)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 536870911) >> 11)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 536870911) >> 3)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 536870911) << 5)                | ((src[7] & 536870911) >> 24)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 536870911) >> 16)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 536870911) >> 8)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 536870911))) & 255);
                        }
        private static void Unpack8IntValuesLE30(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215) | ((((int)src[3]) << 24) & 1073741823);            dest[1] = ((((int)src[3]) >> 6) & 3) | ((((int)src[4]) << 2) & 1023) | ((((int)src[5]) << 10) & 262143) | ((((int)src[6]) << 18) & 67108863) | ((((int)src[7]) << 26) & 1073741823);            dest[2] = ((((int)src[7]) >> 4) & 15) | ((((int)src[8]) << 4) & 4095) | ((((int)src[9]) << 12) & 1048575) | ((((int)src[10]) << 20) & 268435455) | ((((int)src[11]) << 28) & 1073741823);            dest[3] = ((((int)src[11]) >> 2) & 63) | ((((int)src[12]) << 6) & 16383) | ((((int)src[13]) << 14) & 4194303) | ((((int)src[14]) << 22) & 1073741823);            dest[4] = ((((int)src[15])) & 255) | ((((int)src[16]) << 8) & 65535) | ((((int)src[17]) << 16) & 16777215) | ((((int)src[18]) << 24) & 1073741823);            dest[5] = ((((int)src[18]) >> 6) & 3) | ((((int)src[19]) << 2) & 1023) | ((((int)src[20]) << 10) & 262143) | ((((int)src[21]) << 18) & 67108863) | ((((int)src[22]) << 26) & 1073741823);            dest[6] = ((((int)src[22]) >> 4) & 15) | ((((int)src[23]) << 4) & 4095) | ((((int)src[24]) << 12) & 1048575) | ((((int)src[25]) << 20) & 268435455) | ((((int)src[26]) << 28) & 1073741823);            dest[7] = ((((int)src[26]) >> 2) & 63) | ((((int)src[27]) << 6) & 16383) | ((((int)src[28]) << 14) & 4194303) | ((((int)src[29]) << 22) & 1073741823);        }

        private static void Pack8IntValuesLE30(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1073741823))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1073741823) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1073741823) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1073741823) >> 24)                | ((src[1] & 1073741823) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 1073741823) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 1073741823) >> 10)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 1073741823) >> 18)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 1073741823) >> 26)                | ((src[2] & 1073741823) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 1073741823) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 1073741823) >> 12)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 1073741823) >> 20)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 1073741823) >> 28)                | ((src[3] & 1073741823) << 2)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 1073741823) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 1073741823) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 1073741823) >> 22)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 1073741823))) & 255);
                            dest[16] = 
                (byte)((((src[4] & 1073741823) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 1073741823) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 1073741823) >> 24)                | ((src[5] & 1073741823) << 6)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 1073741823) >> 2)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 1073741823) >> 10)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 1073741823) >> 18)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 1073741823) >> 26)                | ((src[6] & 1073741823) << 4)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 1073741823) >> 4)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 1073741823) >> 12)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 1073741823) >> 20)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 1073741823) >> 28)                | ((src[7] & 1073741823) << 2)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 1073741823) >> 6)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 1073741823) >> 14)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 1073741823) >> 22)) & 255);
                        }
        private static void Unpack8IntValuesBE30(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 22) & 1073741823) | ((((int)src[1]) << 14) & 4194303) | ((((int)src[2]) << 6) & 16383) | ((((int)src[3]) >> 2) & 63);            dest[1] = ((((int)src[3]) << 28) & 1073741823) | ((((int)src[4]) << 20) & 268435455) | ((((int)src[5]) << 12) & 1048575) | ((((int)src[6]) << 4) & 4095) | ((((int)src[7]) >> 4) & 15);            dest[2] = ((((int)src[7]) << 26) & 1073741823) | ((((int)src[8]) << 18) & 67108863) | ((((int)src[9]) << 10) & 262143) | ((((int)src[10]) << 2) & 1023) | ((((int)src[11]) >> 6) & 3);            dest[3] = ((((int)src[11]) << 24) & 1073741823) | ((((int)src[12]) << 16) & 16777215) | ((((int)src[13]) << 8) & 65535) | ((((int)src[14])) & 255);            dest[4] = ((((int)src[15]) << 22) & 1073741823) | ((((int)src[16]) << 14) & 4194303) | ((((int)src[17]) << 6) & 16383) | ((((int)src[18]) >> 2) & 63);            dest[5] = ((((int)src[18]) << 28) & 1073741823) | ((((int)src[19]) << 20) & 268435455) | ((((int)src[20]) << 12) & 1048575) | ((((int)src[21]) << 4) & 4095) | ((((int)src[22]) >> 4) & 15);            dest[6] = ((((int)src[22]) << 26) & 1073741823) | ((((int)src[23]) << 18) & 67108863) | ((((int)src[24]) << 10) & 262143) | ((((int)src[25]) << 2) & 1023) | ((((int)src[26]) >> 6) & 3);            dest[7] = ((((int)src[26]) << 24) & 1073741823) | ((((int)src[27]) << 16) & 16777215) | ((((int)src[28]) << 8) & 65535) | ((((int)src[29])) & 255);        }

        private static void Pack8IntValuesBE30(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1073741823) >> 22)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1073741823) >> 14)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1073741823) >> 6)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1073741823) << 2)                | ((src[1] & 1073741823) >> 28)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 1073741823) >> 20)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 1073741823) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 1073741823) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 1073741823) << 4)                | ((src[2] & 1073741823) >> 26)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 1073741823) >> 18)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 1073741823) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 1073741823) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 1073741823) << 6)                | ((src[3] & 1073741823) >> 24)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 1073741823) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 1073741823) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 1073741823))) & 255);
                            dest[15] = 
                (byte)((((src[4] & 1073741823) >> 22)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 1073741823) >> 14)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 1073741823) >> 6)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 1073741823) << 2)                | ((src[5] & 1073741823) >> 28)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 1073741823) >> 20)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 1073741823) >> 12)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 1073741823) >> 4)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 1073741823) << 4)                | ((src[6] & 1073741823) >> 26)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 1073741823) >> 18)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 1073741823) >> 10)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 1073741823) >> 2)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 1073741823) << 6)                | ((src[7] & 1073741823) >> 24)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 1073741823) >> 16)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 1073741823) >> 8)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 1073741823))) & 255);
                        }
        private static void Unpack8IntValuesLE31(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215) | ((((int)src[3]) << 24) & 2147483647);            dest[1] = ((((int)src[3]) >> 7) & 1) | ((((int)src[4]) << 1) & 511) | ((((int)src[5]) << 9) & 131071) | ((((int)src[6]) << 17) & 33554431) | ((((int)src[7]) << 25) & 2147483647);            dest[2] = ((((int)src[7]) >> 6) & 3) | ((((int)src[8]) << 2) & 1023) | ((((int)src[9]) << 10) & 262143) | ((((int)src[10]) << 18) & 67108863) | ((((int)src[11]) << 26) & 2147483647);            dest[3] = ((((int)src[11]) >> 5) & 7) | ((((int)src[12]) << 3) & 2047) | ((((int)src[13]) << 11) & 524287) | ((((int)src[14]) << 19) & 134217727) | ((((int)src[15]) << 27) & 2147483647);            dest[4] = ((((int)src[15]) >> 4) & 15) | ((((int)src[16]) << 4) & 4095) | ((((int)src[17]) << 12) & 1048575) | ((((int)src[18]) << 20) & 268435455) | ((((int)src[19]) << 28) & 2147483647);            dest[5] = ((((int)src[19]) >> 3) & 31) | ((((int)src[20]) << 5) & 8191) | ((((int)src[21]) << 13) & 2097151) | ((((int)src[22]) << 21) & 536870911) | ((((int)src[23]) << 29) & 2147483647);            dest[6] = ((((int)src[23]) >> 2) & 63) | ((((int)src[24]) << 6) & 16383) | ((((int)src[25]) << 14) & 4194303) | ((((int)src[26]) << 22) & 1073741823) | ((((int)src[27]) << 30) & 2147483647);            dest[7] = ((((int)src[27]) >> 1) & 127) | ((((int)src[28]) << 7) & 32767) | ((((int)src[29]) << 15) & 8388607) | ((((int)src[30]) << 23) & 2147483647);        }

        private static void Pack8IntValuesLE31(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2147483647))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2147483647) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2147483647) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2147483647) >> 24)                | ((src[1] & 2147483647) << 7)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 2147483647) >> 1)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 2147483647) >> 9)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 2147483647) >> 17)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 2147483647) >> 25)                | ((src[2] & 2147483647) << 6)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 2147483647) >> 2)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 2147483647) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 2147483647) >> 18)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 2147483647) >> 26)                | ((src[3] & 2147483647) << 5)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 2147483647) >> 3)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 2147483647) >> 11)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 2147483647) >> 19)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 2147483647) >> 27)                | ((src[4] & 2147483647) << 4)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 2147483647) >> 4)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 2147483647) >> 12)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 2147483647) >> 20)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 2147483647) >> 28)                | ((src[5] & 2147483647) << 3)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 2147483647) >> 5)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 2147483647) >> 13)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 2147483647) >> 21)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 2147483647) >> 29)                | ((src[6] & 2147483647) << 2)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 2147483647) >> 6)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 2147483647) >> 14)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 2147483647) >> 22)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 2147483647) >> 30)                | ((src[7] & 2147483647) << 1)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 2147483647) >> 7)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 2147483647) >> 15)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 2147483647) >> 23)) & 255);
                        }
        private static void Unpack8IntValuesBE31(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 23) & 2147483647) | ((((int)src[1]) << 15) & 8388607) | ((((int)src[2]) << 7) & 32767) | ((((int)src[3]) >> 1) & 127);            dest[1] = ((((int)src[3]) << 30) & 2147483647) | ((((int)src[4]) << 22) & 1073741823) | ((((int)src[5]) << 14) & 4194303) | ((((int)src[6]) << 6) & 16383) | ((((int)src[7]) >> 2) & 63);            dest[2] = ((((int)src[7]) << 29) & 2147483647) | ((((int)src[8]) << 21) & 536870911) | ((((int)src[9]) << 13) & 2097151) | ((((int)src[10]) << 5) & 8191) | ((((int)src[11]) >> 3) & 31);            dest[3] = ((((int)src[11]) << 28) & 2147483647) | ((((int)src[12]) << 20) & 268435455) | ((((int)src[13]) << 12) & 1048575) | ((((int)src[14]) << 4) & 4095) | ((((int)src[15]) >> 4) & 15);            dest[4] = ((((int)src[15]) << 27) & 2147483647) | ((((int)src[16]) << 19) & 134217727) | ((((int)src[17]) << 11) & 524287) | ((((int)src[18]) << 3) & 2047) | ((((int)src[19]) >> 5) & 7);            dest[5] = ((((int)src[19]) << 26) & 2147483647) | ((((int)src[20]) << 18) & 67108863) | ((((int)src[21]) << 10) & 262143) | ((((int)src[22]) << 2) & 1023) | ((((int)src[23]) >> 6) & 3);            dest[6] = ((((int)src[23]) << 25) & 2147483647) | ((((int)src[24]) << 17) & 33554431) | ((((int)src[25]) << 9) & 131071) | ((((int)src[26]) << 1) & 511) | ((((int)src[27]) >> 7) & 1);            dest[7] = ((((int)src[27]) << 24) & 2147483647) | ((((int)src[28]) << 16) & 16777215) | ((((int)src[29]) << 8) & 65535) | ((((int)src[30])) & 255);        }

        private static void Pack8IntValuesBE31(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2147483647) >> 23)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2147483647) >> 15)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2147483647) >> 7)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2147483647) << 1)                | ((src[1] & 2147483647) >> 30)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 2147483647) >> 22)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 2147483647) >> 14)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 2147483647) >> 6)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 2147483647) << 2)                | ((src[2] & 2147483647) >> 29)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 2147483647) >> 21)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 2147483647) >> 13)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 2147483647) >> 5)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 2147483647) << 3)                | ((src[3] & 2147483647) >> 28)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 2147483647) >> 20)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 2147483647) >> 12)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 2147483647) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 2147483647) << 4)                | ((src[4] & 2147483647) >> 27)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 2147483647) >> 19)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 2147483647) >> 11)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 2147483647) >> 3)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 2147483647) << 5)                | ((src[5] & 2147483647) >> 26)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 2147483647) >> 18)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 2147483647) >> 10)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 2147483647) >> 2)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 2147483647) << 6)                | ((src[6] & 2147483647) >> 25)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 2147483647) >> 17)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 2147483647) >> 9)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 2147483647) >> 1)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 2147483647) << 7)                | ((src[7] & 2147483647) >> 24)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 2147483647) >> 16)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 2147483647) >> 8)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 2147483647))) & 255);
                        }
        private static void Unpack8IntValuesLE32(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0])) & 255) | ((((int)src[1]) << 8) & 65535) | ((((int)src[2]) << 16) & 16777215) | ((((int)src[3]) << 24) & -1);            dest[1] = ((((int)src[4])) & 255) | ((((int)src[5]) << 8) & 65535) | ((((int)src[6]) << 16) & 16777215) | ((((int)src[7]) << 24) & -1);            dest[2] = ((((int)src[8])) & 255) | ((((int)src[9]) << 8) & 65535) | ((((int)src[10]) << 16) & 16777215) | ((((int)src[11]) << 24) & -1);            dest[3] = ((((int)src[12])) & 255) | ((((int)src[13]) << 8) & 65535) | ((((int)src[14]) << 16) & 16777215) | ((((int)src[15]) << 24) & -1);            dest[4] = ((((int)src[16])) & 255) | ((((int)src[17]) << 8) & 65535) | ((((int)src[18]) << 16) & 16777215) | ((((int)src[19]) << 24) & -1);            dest[5] = ((((int)src[20])) & 255) | ((((int)src[21]) << 8) & 65535) | ((((int)src[22]) << 16) & 16777215) | ((((int)src[23]) << 24) & -1);            dest[6] = ((((int)src[24])) & 255) | ((((int)src[25]) << 8) & 65535) | ((((int)src[26]) << 16) & 16777215) | ((((int)src[27]) << 24) & -1);            dest[7] = ((((int)src[28])) & 255) | ((((int)src[29]) << 8) & 65535) | ((((int)src[30]) << 16) & 16777215) | ((((int)src[31]) << 24) & -1);        }

        private static void Pack8IntValuesLE32(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4294967295))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4294967295) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4294967295) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4294967295) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 4294967295))) & 255);
                            dest[5] = 
                (byte)((((src[1] & 4294967295) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 4294967295) >> 16)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 4294967295) >> 24)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 4294967295))) & 255);
                            dest[9] = 
                (byte)((((src[2] & 4294967295) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 4294967295) >> 16)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 4294967295) >> 24)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 4294967295))) & 255);
                            dest[13] = 
                (byte)((((src[3] & 4294967295) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 4294967295) >> 16)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 4294967295) >> 24)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 4294967295))) & 255);
                            dest[17] = 
                (byte)((((src[4] & 4294967295) >> 8)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 4294967295) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 4294967295) >> 24)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 4294967295))) & 255);
                            dest[21] = 
                (byte)((((src[5] & 4294967295) >> 8)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 4294967295) >> 16)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 4294967295) >> 24)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 4294967295))) & 255);
                            dest[25] = 
                (byte)((((src[6] & 4294967295) >> 8)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 4294967295) >> 16)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 4294967295) >> 24)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 4294967295))) & 255);
                            dest[29] = 
                (byte)((((src[7] & 4294967295) >> 8)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 4294967295) >> 16)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 4294967295) >> 24)) & 255);
                        }
        private static void Unpack8IntValuesBE32(Span<byte> src, Span<int> dest) {
                    dest[0] = ((((int)src[0]) << 24) & -1) | ((((int)src[1]) << 16) & 16777215) | ((((int)src[2]) << 8) & 65535) | ((((int)src[3])) & 255);            dest[1] = ((((int)src[4]) << 24) & -1) | ((((int)src[5]) << 16) & 16777215) | ((((int)src[6]) << 8) & 65535) | ((((int)src[7])) & 255);            dest[2] = ((((int)src[8]) << 24) & -1) | ((((int)src[9]) << 16) & 16777215) | ((((int)src[10]) << 8) & 65535) | ((((int)src[11])) & 255);            dest[3] = ((((int)src[12]) << 24) & -1) | ((((int)src[13]) << 16) & 16777215) | ((((int)src[14]) << 8) & 65535) | ((((int)src[15])) & 255);            dest[4] = ((((int)src[16]) << 24) & -1) | ((((int)src[17]) << 16) & 16777215) | ((((int)src[18]) << 8) & 65535) | ((((int)src[19])) & 255);            dest[5] = ((((int)src[20]) << 24) & -1) | ((((int)src[21]) << 16) & 16777215) | ((((int)src[22]) << 8) & 65535) | ((((int)src[23])) & 255);            dest[6] = ((((int)src[24]) << 24) & -1) | ((((int)src[25]) << 16) & 16777215) | ((((int)src[26]) << 8) & 65535) | ((((int)src[27])) & 255);            dest[7] = ((((int)src[28]) << 24) & -1) | ((((int)src[29]) << 16) & 16777215) | ((((int)src[30]) << 8) & 65535) | ((((int)src[31])) & 255);        }

        private static void Pack8IntValuesBE32(Span<int> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4294967295) >> 24)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4294967295) >> 16)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4294967295) >> 8)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4294967295))) & 255);
                            dest[4] = 
                (byte)((((src[1] & 4294967295) >> 24)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 4294967295) >> 16)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 4294967295) >> 8)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 4294967295))) & 255);
                            dest[8] = 
                (byte)((((src[2] & 4294967295) >> 24)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 4294967295) >> 16)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 4294967295) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 4294967295))) & 255);
                            dest[12] = 
                (byte)((((src[3] & 4294967295) >> 24)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 4294967295) >> 16)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 4294967295) >> 8)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 4294967295))) & 255);
                            dest[16] = 
                (byte)((((src[4] & 4294967295) >> 24)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 4294967295) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 4294967295) >> 8)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 4294967295))) & 255);
                            dest[20] = 
                (byte)((((src[5] & 4294967295) >> 24)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 4294967295) >> 16)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 4294967295) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 4294967295))) & 255);
                            dest[24] = 
                (byte)((((src[6] & 4294967295) >> 24)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 4294967295) >> 16)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 4294967295) >> 8)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 4294967295))) & 255);
                            dest[28] = 
                (byte)((((src[7] & 4294967295) >> 24)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 4294967295) >> 16)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 4294967295) >> 8)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 4294967295))) & 255);
                        }
        private static void Unpack8LongValuesLE0(Span<byte> src, Span<long> dest) {
                 }

        private static void Pack8LongValuesLE0(Span<long> src, Span<byte> dest) {
                }
        private static void Unpack8LongValuesBE0(Span<byte> src, Span<long> dest) {
                 }

        private static void Pack8LongValuesBE0(Span<long> src, Span<byte> dest) {
                }
        private static void Unpack8LongValuesLE1(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 1);            dest[1] = ((((long)src[0]) >> 1) & 1);            dest[2] = ((((long)src[0]) >> 2) & 1);            dest[3] = ((((long)src[0]) >> 3) & 1);            dest[4] = ((((long)src[0]) >> 4) & 1);            dest[5] = ((((long)src[0]) >> 5) & 1);            dest[6] = ((((long)src[0]) >> 6) & 1);            dest[7] = ((((long)src[0]) >> 7) & 1);        }

        private static void Pack8LongValuesLE1(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1))                | ((src[1] & 1) << 1)                | ((src[2] & 1) << 2)                | ((src[3] & 1) << 3)                | ((src[4] & 1) << 4)                | ((src[5] & 1) << 5)                | ((src[6] & 1) << 6)                | ((src[7] & 1) << 7)) & 255);
                        }
        private static void Unpack8LongValuesBE1(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) >> 7) & 1);            dest[1] = ((((long)src[0]) >> 6) & 1);            dest[2] = ((((long)src[0]) >> 5) & 1);            dest[3] = ((((long)src[0]) >> 4) & 1);            dest[4] = ((((long)src[0]) >> 3) & 1);            dest[5] = ((((long)src[0]) >> 2) & 1);            dest[6] = ((((long)src[0]) >> 1) & 1);            dest[7] = ((((long)src[0])) & 1);        }

        private static void Pack8LongValuesBE1(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1) << 7)                | ((src[1] & 1) << 6)                | ((src[2] & 1) << 5)                | ((src[3] & 1) << 4)                | ((src[4] & 1) << 3)                | ((src[5] & 1) << 2)                | ((src[6] & 1) << 1)                | ((src[7] & 1))) & 255);
                        }
        private static void Unpack8LongValuesLE2(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 3);            dest[1] = ((((long)src[0]) >> 2) & 3);            dest[2] = ((((long)src[0]) >> 4) & 3);            dest[3] = ((((long)src[0]) >> 6) & 3);            dest[4] = ((((long)src[1])) & 3);            dest[5] = ((((long)src[1]) >> 2) & 3);            dest[6] = ((((long)src[1]) >> 4) & 3);            dest[7] = ((((long)src[1]) >> 6) & 3);        }

        private static void Pack8LongValuesLE2(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 3))                | ((src[1] & 3) << 2)                | ((src[2] & 3) << 4)                | ((src[3] & 3) << 6)) & 255);
                            dest[1] = 
                (byte)((((src[4] & 3))                | ((src[5] & 3) << 2)                | ((src[6] & 3) << 4)                | ((src[7] & 3) << 6)) & 255);
                        }
        private static void Unpack8LongValuesBE2(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) >> 6) & 3);            dest[1] = ((((long)src[0]) >> 4) & 3);            dest[2] = ((((long)src[0]) >> 2) & 3);            dest[3] = ((((long)src[0])) & 3);            dest[4] = ((((long)src[1]) >> 6) & 3);            dest[5] = ((((long)src[1]) >> 4) & 3);            dest[6] = ((((long)src[1]) >> 2) & 3);            dest[7] = ((((long)src[1])) & 3);        }

        private static void Pack8LongValuesBE2(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 3) << 6)                | ((src[1] & 3) << 4)                | ((src[2] & 3) << 2)                | ((src[3] & 3))) & 255);
                            dest[1] = 
                (byte)((((src[4] & 3) << 6)                | ((src[5] & 3) << 4)                | ((src[6] & 3) << 2)                | ((src[7] & 3))) & 255);
                        }
        private static void Unpack8LongValuesLE3(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 7);            dest[1] = ((((long)src[0]) >> 3) & 7);            dest[2] = ((((long)src[0]) >> 6) & 3) | ((((long)src[1]) << 2) & 7);            dest[3] = ((((long)src[1]) >> 1) & 7);            dest[4] = ((((long)src[1]) >> 4) & 7);            dest[5] = ((((long)src[1]) >> 7) & 1) | ((((long)src[2]) << 1) & 7);            dest[6] = ((((long)src[2]) >> 2) & 7);            dest[7] = ((((long)src[2]) >> 5) & 7);        }

        private static void Pack8LongValuesLE3(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 7))                | ((src[1] & 7) << 3)                | ((src[2] & 7) << 6)) & 255);
                            dest[1] = 
                (byte)((((src[2] & 7) >> 2)                | ((src[3] & 7) << 1)                | ((src[4] & 7) << 4)                | ((src[5] & 7) << 7)) & 255);
                            dest[2] = 
                (byte)((((src[5] & 7) >> 1)                | ((src[6] & 7) << 2)                | ((src[7] & 7) << 5)) & 255);
                        }
        private static void Unpack8LongValuesBE3(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) >> 5) & 7);            dest[1] = ((((long)src[0]) >> 2) & 7);            dest[2] = ((((long)src[0]) << 1) & 7) | ((((long)src[1]) >> 7) & 1);            dest[3] = ((((long)src[1]) >> 4) & 7);            dest[4] = ((((long)src[1]) >> 1) & 7);            dest[5] = ((((long)src[1]) << 2) & 7) | ((((long)src[2]) >> 6) & 3);            dest[6] = ((((long)src[2]) >> 3) & 7);            dest[7] = ((((long)src[2])) & 7);        }

        private static void Pack8LongValuesBE3(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 7) << 5)                | ((src[1] & 7) << 2)                | ((src[2] & 7) >> 1)) & 255);
                            dest[1] = 
                (byte)((((src[2] & 7) << 7)                | ((src[3] & 7) << 4)                | ((src[4] & 7) << 1)                | ((src[5] & 7) >> 2)) & 255);
                            dest[2] = 
                (byte)((((src[5] & 7) << 6)                | ((src[6] & 7) << 3)                | ((src[7] & 7))) & 255);
                        }
        private static void Unpack8LongValuesLE4(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 15);            dest[1] = ((((long)src[0]) >> 4) & 15);            dest[2] = ((((long)src[1])) & 15);            dest[3] = ((((long)src[1]) >> 4) & 15);            dest[4] = ((((long)src[2])) & 15);            dest[5] = ((((long)src[2]) >> 4) & 15);            dest[6] = ((((long)src[3])) & 15);            dest[7] = ((((long)src[3]) >> 4) & 15);        }

        private static void Pack8LongValuesLE4(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 15))                | ((src[1] & 15) << 4)) & 255);
                            dest[1] = 
                (byte)((((src[2] & 15))                | ((src[3] & 15) << 4)) & 255);
                            dest[2] = 
                (byte)((((src[4] & 15))                | ((src[5] & 15) << 4)) & 255);
                            dest[3] = 
                (byte)((((src[6] & 15))                | ((src[7] & 15) << 4)) & 255);
                        }
        private static void Unpack8LongValuesBE4(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) >> 4) & 15);            dest[1] = ((((long)src[0])) & 15);            dest[2] = ((((long)src[1]) >> 4) & 15);            dest[3] = ((((long)src[1])) & 15);            dest[4] = ((((long)src[2]) >> 4) & 15);            dest[5] = ((((long)src[2])) & 15);            dest[6] = ((((long)src[3]) >> 4) & 15);            dest[7] = ((((long)src[3])) & 15);        }

        private static void Pack8LongValuesBE4(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 15) << 4)                | ((src[1] & 15))) & 255);
                            dest[1] = 
                (byte)((((src[2] & 15) << 4)                | ((src[3] & 15))) & 255);
                            dest[2] = 
                (byte)((((src[4] & 15) << 4)                | ((src[5] & 15))) & 255);
                            dest[3] = 
                (byte)((((src[6] & 15) << 4)                | ((src[7] & 15))) & 255);
                        }
        private static void Unpack8LongValuesLE5(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 31);            dest[1] = ((((long)src[0]) >> 5) & 7) | ((((long)src[1]) << 3) & 31);            dest[2] = ((((long)src[1]) >> 2) & 31);            dest[3] = ((((long)src[1]) >> 7) & 1) | ((((long)src[2]) << 1) & 31);            dest[4] = ((((long)src[2]) >> 4) & 15) | ((((long)src[3]) << 4) & 31);            dest[5] = ((((long)src[3]) >> 1) & 31);            dest[6] = ((((long)src[3]) >> 6) & 3) | ((((long)src[4]) << 2) & 31);            dest[7] = ((((long)src[4]) >> 3) & 31);        }

        private static void Pack8LongValuesLE5(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 31))                | ((src[1] & 31) << 5)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 31) >> 3)                | ((src[2] & 31) << 2)                | ((src[3] & 31) << 7)) & 255);
                            dest[2] = 
                (byte)((((src[3] & 31) >> 1)                | ((src[4] & 31) << 4)) & 255);
                            dest[3] = 
                (byte)((((src[4] & 31) >> 4)                | ((src[5] & 31) << 1)                | ((src[6] & 31) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[6] & 31) >> 2)                | ((src[7] & 31) << 3)) & 255);
                        }
        private static void Unpack8LongValuesBE5(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) >> 3) & 31);            dest[1] = ((((long)src[0]) << 2) & 31) | ((((long)src[1]) >> 6) & 3);            dest[2] = ((((long)src[1]) >> 1) & 31);            dest[3] = ((((long)src[1]) << 4) & 31) | ((((long)src[2]) >> 4) & 15);            dest[4] = ((((long)src[2]) << 1) & 31) | ((((long)src[3]) >> 7) & 1);            dest[5] = ((((long)src[3]) >> 2) & 31);            dest[6] = ((((long)src[3]) << 3) & 31) | ((((long)src[4]) >> 5) & 7);            dest[7] = ((((long)src[4])) & 31);        }

        private static void Pack8LongValuesBE5(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 31) << 3)                | ((src[1] & 31) >> 2)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 31) << 6)                | ((src[2] & 31) << 1)                | ((src[3] & 31) >> 4)) & 255);
                            dest[2] = 
                (byte)((((src[3] & 31) << 4)                | ((src[4] & 31) >> 1)) & 255);
                            dest[3] = 
                (byte)((((src[4] & 31) << 7)                | ((src[5] & 31) << 2)                | ((src[6] & 31) >> 3)) & 255);
                            dest[4] = 
                (byte)((((src[6] & 31) << 5)                | ((src[7] & 31))) & 255);
                        }
        private static void Unpack8LongValuesLE6(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 63);            dest[1] = ((((long)src[0]) >> 6) & 3) | ((((long)src[1]) << 2) & 63);            dest[2] = ((((long)src[1]) >> 4) & 15) | ((((long)src[2]) << 4) & 63);            dest[3] = ((((long)src[2]) >> 2) & 63);            dest[4] = ((((long)src[3])) & 63);            dest[5] = ((((long)src[3]) >> 6) & 3) | ((((long)src[4]) << 2) & 63);            dest[6] = ((((long)src[4]) >> 4) & 15) | ((((long)src[5]) << 4) & 63);            dest[7] = ((((long)src[5]) >> 2) & 63);        }

        private static void Pack8LongValuesLE6(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 63))                | ((src[1] & 63) << 6)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 63) >> 2)                | ((src[2] & 63) << 4)) & 255);
                            dest[2] = 
                (byte)((((src[2] & 63) >> 4)                | ((src[3] & 63) << 2)) & 255);
                            dest[3] = 
                (byte)((((src[4] & 63))                | ((src[5] & 63) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[5] & 63) >> 2)                | ((src[6] & 63) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[6] & 63) >> 4)                | ((src[7] & 63) << 2)) & 255);
                        }
        private static void Unpack8LongValuesBE6(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) >> 2) & 63);            dest[1] = ((((long)src[0]) << 4) & 63) | ((((long)src[1]) >> 4) & 15);            dest[2] = ((((long)src[1]) << 2) & 63) | ((((long)src[2]) >> 6) & 3);            dest[3] = ((((long)src[2])) & 63);            dest[4] = ((((long)src[3]) >> 2) & 63);            dest[5] = ((((long)src[3]) << 4) & 63) | ((((long)src[4]) >> 4) & 15);            dest[6] = ((((long)src[4]) << 2) & 63) | ((((long)src[5]) >> 6) & 3);            dest[7] = ((((long)src[5])) & 63);        }

        private static void Pack8LongValuesBE6(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 63) << 2)                | ((src[1] & 63) >> 4)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 63) << 4)                | ((src[2] & 63) >> 2)) & 255);
                            dest[2] = 
                (byte)((((src[2] & 63) << 6)                | ((src[3] & 63))) & 255);
                            dest[3] = 
                (byte)((((src[4] & 63) << 2)                | ((src[5] & 63) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[5] & 63) << 4)                | ((src[6] & 63) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[6] & 63) << 6)                | ((src[7] & 63))) & 255);
                        }
        private static void Unpack8LongValuesLE7(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 127);            dest[1] = ((((long)src[0]) >> 7) & 1) | ((((long)src[1]) << 1) & 127);            dest[2] = ((((long)src[1]) >> 6) & 3) | ((((long)src[2]) << 2) & 127);            dest[3] = ((((long)src[2]) >> 5) & 7) | ((((long)src[3]) << 3) & 127);            dest[4] = ((((long)src[3]) >> 4) & 15) | ((((long)src[4]) << 4) & 127);            dest[5] = ((((long)src[4]) >> 3) & 31) | ((((long)src[5]) << 5) & 127);            dest[6] = ((((long)src[5]) >> 2) & 63) | ((((long)src[6]) << 6) & 127);            dest[7] = ((((long)src[6]) >> 1) & 127);        }

        private static void Pack8LongValuesLE7(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 127))                | ((src[1] & 127) << 7)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 127) >> 1)                | ((src[2] & 127) << 6)) & 255);
                            dest[2] = 
                (byte)((((src[2] & 127) >> 2)                | ((src[3] & 127) << 5)) & 255);
                            dest[3] = 
                (byte)((((src[3] & 127) >> 3)                | ((src[4] & 127) << 4)) & 255);
                            dest[4] = 
                (byte)((((src[4] & 127) >> 4)                | ((src[5] & 127) << 3)) & 255);
                            dest[5] = 
                (byte)((((src[5] & 127) >> 5)                | ((src[6] & 127) << 2)) & 255);
                            dest[6] = 
                (byte)((((src[6] & 127) >> 6)                | ((src[7] & 127) << 1)) & 255);
                        }
        private static void Unpack8LongValuesBE7(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) >> 1) & 127);            dest[1] = ((((long)src[0]) << 6) & 127) | ((((long)src[1]) >> 2) & 63);            dest[2] = ((((long)src[1]) << 5) & 127) | ((((long)src[2]) >> 3) & 31);            dest[3] = ((((long)src[2]) << 4) & 127) | ((((long)src[3]) >> 4) & 15);            dest[4] = ((((long)src[3]) << 3) & 127) | ((((long)src[4]) >> 5) & 7);            dest[5] = ((((long)src[4]) << 2) & 127) | ((((long)src[5]) >> 6) & 3);            dest[6] = ((((long)src[5]) << 1) & 127) | ((((long)src[6]) >> 7) & 1);            dest[7] = ((((long)src[6])) & 127);        }

        private static void Pack8LongValuesBE7(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 127) << 1)                | ((src[1] & 127) >> 6)) & 255);
                            dest[1] = 
                (byte)((((src[1] & 127) << 2)                | ((src[2] & 127) >> 5)) & 255);
                            dest[2] = 
                (byte)((((src[2] & 127) << 3)                | ((src[3] & 127) >> 4)) & 255);
                            dest[3] = 
                (byte)((((src[3] & 127) << 4)                | ((src[4] & 127) >> 3)) & 255);
                            dest[4] = 
                (byte)((((src[4] & 127) << 5)                | ((src[5] & 127) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[5] & 127) << 6)                | ((src[6] & 127) >> 1)) & 255);
                            dest[6] = 
                (byte)((((src[6] & 127) << 7)                | ((src[7] & 127))) & 255);
                        }
        private static void Unpack8LongValuesLE8(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255);            dest[1] = ((((long)src[1])) & 255);            dest[2] = ((((long)src[2])) & 255);            dest[3] = ((((long)src[3])) & 255);            dest[4] = ((((long)src[4])) & 255);            dest[5] = ((((long)src[5])) & 255);            dest[6] = ((((long)src[6])) & 255);            dest[7] = ((((long)src[7])) & 255);        }

        private static void Pack8LongValuesLE8(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 255))) & 255);
                            dest[1] = 
                (byte)((((src[1] & 255))) & 255);
                            dest[2] = 
                (byte)((((src[2] & 255))) & 255);
                            dest[3] = 
                (byte)((((src[3] & 255))) & 255);
                            dest[4] = 
                (byte)((((src[4] & 255))) & 255);
                            dest[5] = 
                (byte)((((src[5] & 255))) & 255);
                            dest[6] = 
                (byte)((((src[6] & 255))) & 255);
                            dest[7] = 
                (byte)((((src[7] & 255))) & 255);
                        }
        private static void Unpack8LongValuesBE8(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255);            dest[1] = ((((long)src[1])) & 255);            dest[2] = ((((long)src[2])) & 255);            dest[3] = ((((long)src[3])) & 255);            dest[4] = ((((long)src[4])) & 255);            dest[5] = ((((long)src[5])) & 255);            dest[6] = ((((long)src[6])) & 255);            dest[7] = ((((long)src[7])) & 255);        }

        private static void Pack8LongValuesBE8(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 255))) & 255);
                            dest[1] = 
                (byte)((((src[1] & 255))) & 255);
                            dest[2] = 
                (byte)((((src[2] & 255))) & 255);
                            dest[3] = 
                (byte)((((src[3] & 255))) & 255);
                            dest[4] = 
                (byte)((((src[4] & 255))) & 255);
                            dest[5] = 
                (byte)((((src[5] & 255))) & 255);
                            dest[6] = 
                (byte)((((src[6] & 255))) & 255);
                            dest[7] = 
                (byte)((((src[7] & 255))) & 255);
                        }
        private static void Unpack8LongValuesLE9(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 511);            dest[1] = ((((long)src[1]) >> 1) & 127) | ((((long)src[2]) << 7) & 511);            dest[2] = ((((long)src[2]) >> 2) & 63) | ((((long)src[3]) << 6) & 511);            dest[3] = ((((long)src[3]) >> 3) & 31) | ((((long)src[4]) << 5) & 511);            dest[4] = ((((long)src[4]) >> 4) & 15) | ((((long)src[5]) << 4) & 511);            dest[5] = ((((long)src[5]) >> 5) & 7) | ((((long)src[6]) << 3) & 511);            dest[6] = ((((long)src[6]) >> 6) & 3) | ((((long)src[7]) << 2) & 511);            dest[7] = ((((long)src[7]) >> 7) & 1) | ((((long)src[8]) << 1) & 511);        }

        private static void Pack8LongValuesLE9(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 511))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 511) >> 8)                | ((src[1] & 511) << 1)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 511) >> 7)                | ((src[2] & 511) << 2)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 511) >> 6)                | ((src[3] & 511) << 3)) & 255);
                            dest[4] = 
                (byte)((((src[3] & 511) >> 5)                | ((src[4] & 511) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[4] & 511) >> 4)                | ((src[5] & 511) << 5)) & 255);
                            dest[6] = 
                (byte)((((src[5] & 511) >> 3)                | ((src[6] & 511) << 6)) & 255);
                            dest[7] = 
                (byte)((((src[6] & 511) >> 2)                | ((src[7] & 511) << 7)) & 255);
                            dest[8] = 
                (byte)((((src[7] & 511) >> 1)) & 255);
                        }
        private static void Unpack8LongValuesBE9(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 1) & 511) | ((((long)src[1]) >> 7) & 1);            dest[1] = ((((long)src[1]) << 2) & 511) | ((((long)src[2]) >> 6) & 3);            dest[2] = ((((long)src[2]) << 3) & 511) | ((((long)src[3]) >> 5) & 7);            dest[3] = ((((long)src[3]) << 4) & 511) | ((((long)src[4]) >> 4) & 15);            dest[4] = ((((long)src[4]) << 5) & 511) | ((((long)src[5]) >> 3) & 31);            dest[5] = ((((long)src[5]) << 6) & 511) | ((((long)src[6]) >> 2) & 63);            dest[6] = ((((long)src[6]) << 7) & 511) | ((((long)src[7]) >> 1) & 127);            dest[7] = ((((long)src[7]) << 8) & 511) | ((((long)src[8])) & 255);        }

        private static void Pack8LongValuesBE9(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 511) >> 1)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 511) << 7)                | ((src[1] & 511) >> 2)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 511) << 6)                | ((src[2] & 511) >> 3)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 511) << 5)                | ((src[3] & 511) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[3] & 511) << 4)                | ((src[4] & 511) >> 5)) & 255);
                            dest[5] = 
                (byte)((((src[4] & 511) << 3)                | ((src[5] & 511) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[5] & 511) << 2)                | ((src[6] & 511) >> 7)) & 255);
                            dest[7] = 
                (byte)((((src[6] & 511) << 1)                | ((src[7] & 511) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[7] & 511))) & 255);
                        }
        private static void Unpack8LongValuesLE10(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 1023);            dest[1] = ((((long)src[1]) >> 2) & 63) | ((((long)src[2]) << 6) & 1023);            dest[2] = ((((long)src[2]) >> 4) & 15) | ((((long)src[3]) << 4) & 1023);            dest[3] = ((((long)src[3]) >> 6) & 3) | ((((long)src[4]) << 2) & 1023);            dest[4] = ((((long)src[5])) & 255) | ((((long)src[6]) << 8) & 1023);            dest[5] = ((((long)src[6]) >> 2) & 63) | ((((long)src[7]) << 6) & 1023);            dest[6] = ((((long)src[7]) >> 4) & 15) | ((((long)src[8]) << 4) & 1023);            dest[7] = ((((long)src[8]) >> 6) & 3) | ((((long)src[9]) << 2) & 1023);        }

        private static void Pack8LongValuesLE10(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1023))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1023) >> 8)                | ((src[1] & 1023) << 2)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 1023) >> 6)                | ((src[2] & 1023) << 4)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 1023) >> 4)                | ((src[3] & 1023) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[3] & 1023) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[4] & 1023))) & 255);
                            dest[6] = 
                (byte)((((src[4] & 1023) >> 8)                | ((src[5] & 1023) << 2)) & 255);
                            dest[7] = 
                (byte)((((src[5] & 1023) >> 6)                | ((src[6] & 1023) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[6] & 1023) >> 4)                | ((src[7] & 1023) << 6)) & 255);
                            dest[9] = 
                (byte)((((src[7] & 1023) >> 2)) & 255);
                        }
        private static void Unpack8LongValuesBE10(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 2) & 1023) | ((((long)src[1]) >> 6) & 3);            dest[1] = ((((long)src[1]) << 4) & 1023) | ((((long)src[2]) >> 4) & 15);            dest[2] = ((((long)src[2]) << 6) & 1023) | ((((long)src[3]) >> 2) & 63);            dest[3] = ((((long)src[3]) << 8) & 1023) | ((((long)src[4])) & 255);            dest[4] = ((((long)src[5]) << 2) & 1023) | ((((long)src[6]) >> 6) & 3);            dest[5] = ((((long)src[6]) << 4) & 1023) | ((((long)src[7]) >> 4) & 15);            dest[6] = ((((long)src[7]) << 6) & 1023) | ((((long)src[8]) >> 2) & 63);            dest[7] = ((((long)src[8]) << 8) & 1023) | ((((long)src[9])) & 255);        }

        private static void Pack8LongValuesBE10(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1023) >> 2)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1023) << 6)                | ((src[1] & 1023) >> 4)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 1023) << 4)                | ((src[2] & 1023) >> 6)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 1023) << 2)                | ((src[3] & 1023) >> 8)) & 255);
                            dest[4] = 
                (byte)((((src[3] & 1023))) & 255);
                            dest[5] = 
                (byte)((((src[4] & 1023) >> 2)) & 255);
                            dest[6] = 
                (byte)((((src[4] & 1023) << 6)                | ((src[5] & 1023) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[5] & 1023) << 4)                | ((src[6] & 1023) >> 6)) & 255);
                            dest[8] = 
                (byte)((((src[6] & 1023) << 2)                | ((src[7] & 1023) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[7] & 1023))) & 255);
                        }
        private static void Unpack8LongValuesLE11(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 2047);            dest[1] = ((((long)src[1]) >> 3) & 31) | ((((long)src[2]) << 5) & 2047);            dest[2] = ((((long)src[2]) >> 6) & 3) | ((((long)src[3]) << 2) & 1023) | ((((long)src[4]) << 10) & 2047);            dest[3] = ((((long)src[4]) >> 1) & 127) | ((((long)src[5]) << 7) & 2047);            dest[4] = ((((long)src[5]) >> 4) & 15) | ((((long)src[6]) << 4) & 2047);            dest[5] = ((((long)src[6]) >> 7) & 1) | ((((long)src[7]) << 1) & 511) | ((((long)src[8]) << 9) & 2047);            dest[6] = ((((long)src[8]) >> 2) & 63) | ((((long)src[9]) << 6) & 2047);            dest[7] = ((((long)src[9]) >> 5) & 7) | ((((long)src[10]) << 3) & 2047);        }

        private static void Pack8LongValuesLE11(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2047))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2047) >> 8)                | ((src[1] & 2047) << 3)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 2047) >> 5)                | ((src[2] & 2047) << 6)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 2047) >> 2)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 2047) >> 10)                | ((src[3] & 2047) << 1)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 2047) >> 7)                | ((src[4] & 2047) << 4)) & 255);
                            dest[6] = 
                (byte)((((src[4] & 2047) >> 4)                | ((src[5] & 2047) << 7)) & 255);
                            dest[7] = 
                (byte)((((src[5] & 2047) >> 1)) & 255);
                            dest[8] = 
                (byte)((((src[5] & 2047) >> 9)                | ((src[6] & 2047) << 2)) & 255);
                            dest[9] = 
                (byte)((((src[6] & 2047) >> 6)                | ((src[7] & 2047) << 5)) & 255);
                            dest[10] = 
                (byte)((((src[7] & 2047) >> 3)) & 255);
                        }
        private static void Unpack8LongValuesBE11(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 3) & 2047) | ((((long)src[1]) >> 5) & 7);            dest[1] = ((((long)src[1]) << 6) & 2047) | ((((long)src[2]) >> 2) & 63);            dest[2] = ((((long)src[2]) << 9) & 2047) | ((((long)src[3]) << 1) & 511) | ((((long)src[4]) >> 7) & 1);            dest[3] = ((((long)src[4]) << 4) & 2047) | ((((long)src[5]) >> 4) & 15);            dest[4] = ((((long)src[5]) << 7) & 2047) | ((((long)src[6]) >> 1) & 127);            dest[5] = ((((long)src[6]) << 10) & 2047) | ((((long)src[7]) << 2) & 1023) | ((((long)src[8]) >> 6) & 3);            dest[6] = ((((long)src[8]) << 5) & 2047) | ((((long)src[9]) >> 3) & 31);            dest[7] = ((((long)src[9]) << 8) & 2047) | ((((long)src[10])) & 255);        }

        private static void Pack8LongValuesBE11(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2047) >> 3)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2047) << 5)                | ((src[1] & 2047) >> 6)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 2047) << 2)                | ((src[2] & 2047) >> 9)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 2047) >> 1)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 2047) << 7)                | ((src[3] & 2047) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 2047) << 4)                | ((src[4] & 2047) >> 7)) & 255);
                            dest[6] = 
                (byte)((((src[4] & 2047) << 1)                | ((src[5] & 2047) >> 10)) & 255);
                            dest[7] = 
                (byte)((((src[5] & 2047) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[5] & 2047) << 6)                | ((src[6] & 2047) >> 5)) & 255);
                            dest[9] = 
                (byte)((((src[6] & 2047) << 3)                | ((src[7] & 2047) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[7] & 2047))) & 255);
                        }
        private static void Unpack8LongValuesLE12(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 4095);            dest[1] = ((((long)src[1]) >> 4) & 15) | ((((long)src[2]) << 4) & 4095);            dest[2] = ((((long)src[3])) & 255) | ((((long)src[4]) << 8) & 4095);            dest[3] = ((((long)src[4]) >> 4) & 15) | ((((long)src[5]) << 4) & 4095);            dest[4] = ((((long)src[6])) & 255) | ((((long)src[7]) << 8) & 4095);            dest[5] = ((((long)src[7]) >> 4) & 15) | ((((long)src[8]) << 4) & 4095);            dest[6] = ((((long)src[9])) & 255) | ((((long)src[10]) << 8) & 4095);            dest[7] = ((((long)src[10]) >> 4) & 15) | ((((long)src[11]) << 4) & 4095);        }

        private static void Pack8LongValuesLE12(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4095))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4095) >> 8)                | ((src[1] & 4095) << 4)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 4095) >> 4)) & 255);
                            dest[3] = 
                (byte)((((src[2] & 4095))) & 255);
                            dest[4] = 
                (byte)((((src[2] & 4095) >> 8)                | ((src[3] & 4095) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 4095) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[4] & 4095))) & 255);
                            dest[7] = 
                (byte)((((src[4] & 4095) >> 8)                | ((src[5] & 4095) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[5] & 4095) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[6] & 4095))) & 255);
                            dest[10] = 
                (byte)((((src[6] & 4095) >> 8)                | ((src[7] & 4095) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[7] & 4095) >> 4)) & 255);
                        }
        private static void Unpack8LongValuesBE12(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 4) & 4095) | ((((long)src[1]) >> 4) & 15);            dest[1] = ((((long)src[1]) << 8) & 4095) | ((((long)src[2])) & 255);            dest[2] = ((((long)src[3]) << 4) & 4095) | ((((long)src[4]) >> 4) & 15);            dest[3] = ((((long)src[4]) << 8) & 4095) | ((((long)src[5])) & 255);            dest[4] = ((((long)src[6]) << 4) & 4095) | ((((long)src[7]) >> 4) & 15);            dest[5] = ((((long)src[7]) << 8) & 4095) | ((((long)src[8])) & 255);            dest[6] = ((((long)src[9]) << 4) & 4095) | ((((long)src[10]) >> 4) & 15);            dest[7] = ((((long)src[10]) << 8) & 4095) | ((((long)src[11])) & 255);        }

        private static void Pack8LongValuesBE12(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4095) >> 4)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4095) << 4)                | ((src[1] & 4095) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 4095))) & 255);
                            dest[3] = 
                (byte)((((src[2] & 4095) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 4095) << 4)                | ((src[3] & 4095) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 4095))) & 255);
                            dest[6] = 
                (byte)((((src[4] & 4095) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[4] & 4095) << 4)                | ((src[5] & 4095) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[5] & 4095))) & 255);
                            dest[9] = 
                (byte)((((src[6] & 4095) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[6] & 4095) << 4)                | ((src[7] & 4095) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[7] & 4095))) & 255);
                        }
        private static void Unpack8LongValuesLE13(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 8191);            dest[1] = ((((long)src[1]) >> 5) & 7) | ((((long)src[2]) << 3) & 2047) | ((((long)src[3]) << 11) & 8191);            dest[2] = ((((long)src[3]) >> 2) & 63) | ((((long)src[4]) << 6) & 8191);            dest[3] = ((((long)src[4]) >> 7) & 1) | ((((long)src[5]) << 1) & 511) | ((((long)src[6]) << 9) & 8191);            dest[4] = ((((long)src[6]) >> 4) & 15) | ((((long)src[7]) << 4) & 4095) | ((((long)src[8]) << 12) & 8191);            dest[5] = ((((long)src[8]) >> 1) & 127) | ((((long)src[9]) << 7) & 8191);            dest[6] = ((((long)src[9]) >> 6) & 3) | ((((long)src[10]) << 2) & 1023) | ((((long)src[11]) << 10) & 8191);            dest[7] = ((((long)src[11]) >> 3) & 31) | ((((long)src[12]) << 5) & 8191);        }

        private static void Pack8LongValuesLE13(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8191))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8191) >> 8)                | ((src[1] & 8191) << 5)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 8191) >> 3)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 8191) >> 11)                | ((src[2] & 8191) << 2)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 8191) >> 6)                | ((src[3] & 8191) << 7)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 8191) >> 1)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 8191) >> 9)                | ((src[4] & 8191) << 4)) & 255);
                            dest[7] = 
                (byte)((((src[4] & 8191) >> 4)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 8191) >> 12)                | ((src[5] & 8191) << 1)) & 255);
                            dest[9] = 
                (byte)((((src[5] & 8191) >> 7)                | ((src[6] & 8191) << 6)) & 255);
                            dest[10] = 
                (byte)((((src[6] & 8191) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[6] & 8191) >> 10)                | ((src[7] & 8191) << 3)) & 255);
                            dest[12] = 
                (byte)((((src[7] & 8191) >> 5)) & 255);
                        }
        private static void Unpack8LongValuesBE13(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 5) & 8191) | ((((long)src[1]) >> 3) & 31);            dest[1] = ((((long)src[1]) << 10) & 8191) | ((((long)src[2]) << 2) & 1023) | ((((long)src[3]) >> 6) & 3);            dest[2] = ((((long)src[3]) << 7) & 8191) | ((((long)src[4]) >> 1) & 127);            dest[3] = ((((long)src[4]) << 12) & 8191) | ((((long)src[5]) << 4) & 4095) | ((((long)src[6]) >> 4) & 15);            dest[4] = ((((long)src[6]) << 9) & 8191) | ((((long)src[7]) << 1) & 511) | ((((long)src[8]) >> 7) & 1);            dest[5] = ((((long)src[8]) << 6) & 8191) | ((((long)src[9]) >> 2) & 63);            dest[6] = ((((long)src[9]) << 11) & 8191) | ((((long)src[10]) << 3) & 2047) | ((((long)src[11]) >> 5) & 7);            dest[7] = ((((long)src[11]) << 8) & 8191) | ((((long)src[12])) & 255);        }

        private static void Pack8LongValuesBE13(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8191) >> 5)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8191) << 3)                | ((src[1] & 8191) >> 10)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 8191) >> 2)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 8191) << 6)                | ((src[2] & 8191) >> 7)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 8191) << 1)                | ((src[3] & 8191) >> 12)) & 255);
                            dest[5] = 
                (byte)((((src[3] & 8191) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 8191) << 4)                | ((src[4] & 8191) >> 9)) & 255);
                            dest[7] = 
                (byte)((((src[4] & 8191) >> 1)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 8191) << 7)                | ((src[5] & 8191) >> 6)) & 255);
                            dest[9] = 
                (byte)((((src[5] & 8191) << 2)                | ((src[6] & 8191) >> 11)) & 255);
                            dest[10] = 
                (byte)((((src[6] & 8191) >> 3)) & 255);
                            dest[11] = 
                (byte)((((src[6] & 8191) << 5)                | ((src[7] & 8191) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[7] & 8191))) & 255);
                        }
        private static void Unpack8LongValuesLE14(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 16383);            dest[1] = ((((long)src[1]) >> 6) & 3) | ((((long)src[2]) << 2) & 1023) | ((((long)src[3]) << 10) & 16383);            dest[2] = ((((long)src[3]) >> 4) & 15) | ((((long)src[4]) << 4) & 4095) | ((((long)src[5]) << 12) & 16383);            dest[3] = ((((long)src[5]) >> 2) & 63) | ((((long)src[6]) << 6) & 16383);            dest[4] = ((((long)src[7])) & 255) | ((((long)src[8]) << 8) & 16383);            dest[5] = ((((long)src[8]) >> 6) & 3) | ((((long)src[9]) << 2) & 1023) | ((((long)src[10]) << 10) & 16383);            dest[6] = ((((long)src[10]) >> 4) & 15) | ((((long)src[11]) << 4) & 4095) | ((((long)src[12]) << 12) & 16383);            dest[7] = ((((long)src[12]) >> 2) & 63) | ((((long)src[13]) << 6) & 16383);        }

        private static void Pack8LongValuesLE14(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 16383))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 16383) >> 8)                | ((src[1] & 16383) << 6)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 16383) >> 2)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 16383) >> 10)                | ((src[2] & 16383) << 4)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 16383) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 16383) >> 12)                | ((src[3] & 16383) << 2)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 16383) >> 6)) & 255);
                            dest[7] = 
                (byte)((((src[4] & 16383))) & 255);
                            dest[8] = 
                (byte)((((src[4] & 16383) >> 8)                | ((src[5] & 16383) << 6)) & 255);
                            dest[9] = 
                (byte)((((src[5] & 16383) >> 2)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 16383) >> 10)                | ((src[6] & 16383) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[6] & 16383) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 16383) >> 12)                | ((src[7] & 16383) << 2)) & 255);
                            dest[13] = 
                (byte)((((src[7] & 16383) >> 6)) & 255);
                        }
        private static void Unpack8LongValuesBE14(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 6) & 16383) | ((((long)src[1]) >> 2) & 63);            dest[1] = ((((long)src[1]) << 12) & 16383) | ((((long)src[2]) << 4) & 4095) | ((((long)src[3]) >> 4) & 15);            dest[2] = ((((long)src[3]) << 10) & 16383) | ((((long)src[4]) << 2) & 1023) | ((((long)src[5]) >> 6) & 3);            dest[3] = ((((long)src[5]) << 8) & 16383) | ((((long)src[6])) & 255);            dest[4] = ((((long)src[7]) << 6) & 16383) | ((((long)src[8]) >> 2) & 63);            dest[5] = ((((long)src[8]) << 12) & 16383) | ((((long)src[9]) << 4) & 4095) | ((((long)src[10]) >> 4) & 15);            dest[6] = ((((long)src[10]) << 10) & 16383) | ((((long)src[11]) << 2) & 1023) | ((((long)src[12]) >> 6) & 3);            dest[7] = ((((long)src[12]) << 8) & 16383) | ((((long)src[13])) & 255);        }

        private static void Pack8LongValuesBE14(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 16383) >> 6)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 16383) << 2)                | ((src[1] & 16383) >> 12)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 16383) >> 4)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 16383) << 4)                | ((src[2] & 16383) >> 10)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 16383) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 16383) << 6)                | ((src[3] & 16383) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 16383))) & 255);
                            dest[7] = 
                (byte)((((src[4] & 16383) >> 6)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 16383) << 2)                | ((src[5] & 16383) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[5] & 16383) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 16383) << 4)                | ((src[6] & 16383) >> 10)) & 255);
                            dest[11] = 
                (byte)((((src[6] & 16383) >> 2)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 16383) << 6)                | ((src[7] & 16383) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[7] & 16383))) & 255);
                        }
        private static void Unpack8LongValuesLE15(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 32767);            dest[1] = ((((long)src[1]) >> 7) & 1) | ((((long)src[2]) << 1) & 511) | ((((long)src[3]) << 9) & 32767);            dest[2] = ((((long)src[3]) >> 6) & 3) | ((((long)src[4]) << 2) & 1023) | ((((long)src[5]) << 10) & 32767);            dest[3] = ((((long)src[5]) >> 5) & 7) | ((((long)src[6]) << 3) & 2047) | ((((long)src[7]) << 11) & 32767);            dest[4] = ((((long)src[7]) >> 4) & 15) | ((((long)src[8]) << 4) & 4095) | ((((long)src[9]) << 12) & 32767);            dest[5] = ((((long)src[9]) >> 3) & 31) | ((((long)src[10]) << 5) & 8191) | ((((long)src[11]) << 13) & 32767);            dest[6] = ((((long)src[11]) >> 2) & 63) | ((((long)src[12]) << 6) & 16383) | ((((long)src[13]) << 14) & 32767);            dest[7] = ((((long)src[13]) >> 1) & 127) | ((((long)src[14]) << 7) & 32767);        }

        private static void Pack8LongValuesLE15(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 32767))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 32767) >> 8)                | ((src[1] & 32767) << 7)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 32767) >> 1)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 32767) >> 9)                | ((src[2] & 32767) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 32767) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 32767) >> 10)                | ((src[3] & 32767) << 5)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 32767) >> 3)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 32767) >> 11)                | ((src[4] & 32767) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 32767) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 32767) >> 12)                | ((src[5] & 32767) << 3)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 32767) >> 5)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 32767) >> 13)                | ((src[6] & 32767) << 2)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 32767) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 32767) >> 14)                | ((src[7] & 32767) << 1)) & 255);
                            dest[14] = 
                (byte)((((src[7] & 32767) >> 7)) & 255);
                        }
        private static void Unpack8LongValuesBE15(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 7) & 32767) | ((((long)src[1]) >> 1) & 127);            dest[1] = ((((long)src[1]) << 14) & 32767) | ((((long)src[2]) << 6) & 16383) | ((((long)src[3]) >> 2) & 63);            dest[2] = ((((long)src[3]) << 13) & 32767) | ((((long)src[4]) << 5) & 8191) | ((((long)src[5]) >> 3) & 31);            dest[3] = ((((long)src[5]) << 12) & 32767) | ((((long)src[6]) << 4) & 4095) | ((((long)src[7]) >> 4) & 15);            dest[4] = ((((long)src[7]) << 11) & 32767) | ((((long)src[8]) << 3) & 2047) | ((((long)src[9]) >> 5) & 7);            dest[5] = ((((long)src[9]) << 10) & 32767) | ((((long)src[10]) << 2) & 1023) | ((((long)src[11]) >> 6) & 3);            dest[6] = ((((long)src[11]) << 9) & 32767) | ((((long)src[12]) << 1) & 511) | ((((long)src[13]) >> 7) & 1);            dest[7] = ((((long)src[13]) << 8) & 32767) | ((((long)src[14])) & 255);        }

        private static void Pack8LongValuesBE15(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 32767) >> 7)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 32767) << 1)                | ((src[1] & 32767) >> 14)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 32767) >> 6)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 32767) << 2)                | ((src[2] & 32767) >> 13)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 32767) >> 5)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 32767) << 3)                | ((src[3] & 32767) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 32767) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 32767) << 4)                | ((src[4] & 32767) >> 11)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 32767) >> 3)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 32767) << 5)                | ((src[5] & 32767) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 32767) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 32767) << 6)                | ((src[6] & 32767) >> 9)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 32767) >> 1)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 32767) << 7)                | ((src[7] & 32767) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[7] & 32767))) & 255);
                        }
        private static void Unpack8LongValuesLE16(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535);            dest[1] = ((((long)src[2])) & 255) | ((((long)src[3]) << 8) & 65535);            dest[2] = ((((long)src[4])) & 255) | ((((long)src[5]) << 8) & 65535);            dest[3] = ((((long)src[6])) & 255) | ((((long)src[7]) << 8) & 65535);            dest[4] = ((((long)src[8])) & 255) | ((((long)src[9]) << 8) & 65535);            dest[5] = ((((long)src[10])) & 255) | ((((long)src[11]) << 8) & 65535);            dest[6] = ((((long)src[12])) & 255) | ((((long)src[13]) << 8) & 65535);            dest[7] = ((((long)src[14])) & 255) | ((((long)src[15]) << 8) & 65535);        }

        private static void Pack8LongValuesLE16(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 65535))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 65535) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[1] & 65535))) & 255);
                            dest[3] = 
                (byte)((((src[1] & 65535) >> 8)) & 255);
                            dest[4] = 
                (byte)((((src[2] & 65535))) & 255);
                            dest[5] = 
                (byte)((((src[2] & 65535) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[3] & 65535))) & 255);
                            dest[7] = 
                (byte)((((src[3] & 65535) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[4] & 65535))) & 255);
                            dest[9] = 
                (byte)((((src[4] & 65535) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[5] & 65535))) & 255);
                            dest[11] = 
                (byte)((((src[5] & 65535) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[6] & 65535))) & 255);
                            dest[13] = 
                (byte)((((src[6] & 65535) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[7] & 65535))) & 255);
                            dest[15] = 
                (byte)((((src[7] & 65535) >> 8)) & 255);
                        }
        private static void Unpack8LongValuesBE16(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 8) & 65535) | ((((long)src[1])) & 255);            dest[1] = ((((long)src[2]) << 8) & 65535) | ((((long)src[3])) & 255);            dest[2] = ((((long)src[4]) << 8) & 65535) | ((((long)src[5])) & 255);            dest[3] = ((((long)src[6]) << 8) & 65535) | ((((long)src[7])) & 255);            dest[4] = ((((long)src[8]) << 8) & 65535) | ((((long)src[9])) & 255);            dest[5] = ((((long)src[10]) << 8) & 65535) | ((((long)src[11])) & 255);            dest[6] = ((((long)src[12]) << 8) & 65535) | ((((long)src[13])) & 255);            dest[7] = ((((long)src[14]) << 8) & 65535) | ((((long)src[15])) & 255);        }

        private static void Pack8LongValuesBE16(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 65535) >> 8)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 65535))) & 255);
                            dest[2] = 
                (byte)((((src[1] & 65535) >> 8)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 65535))) & 255);
                            dest[4] = 
                (byte)((((src[2] & 65535) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 65535))) & 255);
                            dest[6] = 
                (byte)((((src[3] & 65535) >> 8)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 65535))) & 255);
                            dest[8] = 
                (byte)((((src[4] & 65535) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 65535))) & 255);
                            dest[10] = 
                (byte)((((src[5] & 65535) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 65535))) & 255);
                            dest[12] = 
                (byte)((((src[6] & 65535) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 65535))) & 255);
                            dest[14] = 
                (byte)((((src[7] & 65535) >> 8)) & 255);
                            dest[15] = 
                (byte)((((src[7] & 65535))) & 255);
                        }
        private static void Unpack8LongValuesLE17(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 131071);            dest[1] = ((((long)src[2]) >> 1) & 127) | ((((long)src[3]) << 7) & 32767) | ((((long)src[4]) << 15) & 131071);            dest[2] = ((((long)src[4]) >> 2) & 63) | ((((long)src[5]) << 6) & 16383) | ((((long)src[6]) << 14) & 131071);            dest[3] = ((((long)src[6]) >> 3) & 31) | ((((long)src[7]) << 5) & 8191) | ((((long)src[8]) << 13) & 131071);            dest[4] = ((((long)src[8]) >> 4) & 15) | ((((long)src[9]) << 4) & 4095) | ((((long)src[10]) << 12) & 131071);            dest[5] = ((((long)src[10]) >> 5) & 7) | ((((long)src[11]) << 3) & 2047) | ((((long)src[12]) << 11) & 131071);            dest[6] = ((((long)src[12]) >> 6) & 3) | ((((long)src[13]) << 2) & 1023) | ((((long)src[14]) << 10) & 131071);            dest[7] = ((((long)src[14]) >> 7) & 1) | ((((long)src[15]) << 1) & 511) | ((((long)src[16]) << 9) & 131071);        }

        private static void Pack8LongValuesLE17(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 131071))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 131071) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 131071) >> 16)                | ((src[1] & 131071) << 1)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 131071) >> 7)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 131071) >> 15)                | ((src[2] & 131071) << 2)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 131071) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 131071) >> 14)                | ((src[3] & 131071) << 3)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 131071) >> 5)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 131071) >> 13)                | ((src[4] & 131071) << 4)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 131071) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 131071) >> 12)                | ((src[5] & 131071) << 5)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 131071) >> 3)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 131071) >> 11)                | ((src[6] & 131071) << 6)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 131071) >> 2)) & 255);
                            dest[14] = 
                (byte)((((src[6] & 131071) >> 10)                | ((src[7] & 131071) << 7)) & 255);
                            dest[15] = 
                (byte)((((src[7] & 131071) >> 1)) & 255);
                            dest[16] = 
                (byte)((((src[7] & 131071) >> 9)) & 255);
                        }
        private static void Unpack8LongValuesBE17(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 9) & 131071) | ((((long)src[1]) << 1) & 511) | ((((long)src[2]) >> 7) & 1);            dest[1] = ((((long)src[2]) << 10) & 131071) | ((((long)src[3]) << 2) & 1023) | ((((long)src[4]) >> 6) & 3);            dest[2] = ((((long)src[4]) << 11) & 131071) | ((((long)src[5]) << 3) & 2047) | ((((long)src[6]) >> 5) & 7);            dest[3] = ((((long)src[6]) << 12) & 131071) | ((((long)src[7]) << 4) & 4095) | ((((long)src[8]) >> 4) & 15);            dest[4] = ((((long)src[8]) << 13) & 131071) | ((((long)src[9]) << 5) & 8191) | ((((long)src[10]) >> 3) & 31);            dest[5] = ((((long)src[10]) << 14) & 131071) | ((((long)src[11]) << 6) & 16383) | ((((long)src[12]) >> 2) & 63);            dest[6] = ((((long)src[12]) << 15) & 131071) | ((((long)src[13]) << 7) & 32767) | ((((long)src[14]) >> 1) & 127);            dest[7] = ((((long)src[14]) << 16) & 131071) | ((((long)src[15]) << 8) & 65535) | ((((long)src[16])) & 255);        }

        private static void Pack8LongValuesBE17(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 131071) >> 9)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 131071) >> 1)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 131071) << 7)                | ((src[1] & 131071) >> 10)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 131071) >> 2)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 131071) << 6)                | ((src[2] & 131071) >> 11)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 131071) >> 3)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 131071) << 5)                | ((src[3] & 131071) >> 12)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 131071) >> 4)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 131071) << 4)                | ((src[4] & 131071) >> 13)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 131071) >> 5)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 131071) << 3)                | ((src[5] & 131071) >> 14)) & 255);
                            dest[11] = 
                (byte)((((src[5] & 131071) >> 6)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 131071) << 2)                | ((src[6] & 131071) >> 15)) & 255);
                            dest[13] = 
                (byte)((((src[6] & 131071) >> 7)) & 255);
                            dest[14] = 
                (byte)((((src[6] & 131071) << 1)                | ((src[7] & 131071) >> 16)) & 255);
                            dest[15] = 
                (byte)((((src[7] & 131071) >> 8)) & 255);
                            dest[16] = 
                (byte)((((src[7] & 131071))) & 255);
                        }
        private static void Unpack8LongValuesLE18(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 262143);            dest[1] = ((((long)src[2]) >> 2) & 63) | ((((long)src[3]) << 6) & 16383) | ((((long)src[4]) << 14) & 262143);            dest[2] = ((((long)src[4]) >> 4) & 15) | ((((long)src[5]) << 4) & 4095) | ((((long)src[6]) << 12) & 262143);            dest[3] = ((((long)src[6]) >> 6) & 3) | ((((long)src[7]) << 2) & 1023) | ((((long)src[8]) << 10) & 262143);            dest[4] = ((((long)src[9])) & 255) | ((((long)src[10]) << 8) & 65535) | ((((long)src[11]) << 16) & 262143);            dest[5] = ((((long)src[11]) >> 2) & 63) | ((((long)src[12]) << 6) & 16383) | ((((long)src[13]) << 14) & 262143);            dest[6] = ((((long)src[13]) >> 4) & 15) | ((((long)src[14]) << 4) & 4095) | ((((long)src[15]) << 12) & 262143);            dest[7] = ((((long)src[15]) >> 6) & 3) | ((((long)src[16]) << 2) & 1023) | ((((long)src[17]) << 10) & 262143);        }

        private static void Pack8LongValuesLE18(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 262143))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 262143) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 262143) >> 16)                | ((src[1] & 262143) << 2)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 262143) >> 6)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 262143) >> 14)                | ((src[2] & 262143) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 262143) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 262143) >> 12)                | ((src[3] & 262143) << 6)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 262143) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 262143) >> 10)) & 255);
                            dest[9] = 
                (byte)((((src[4] & 262143))) & 255);
                            dest[10] = 
                (byte)((((src[4] & 262143) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 262143) >> 16)                | ((src[5] & 262143) << 2)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 262143) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 262143) >> 14)                | ((src[6] & 262143) << 4)) & 255);
                            dest[14] = 
                (byte)((((src[6] & 262143) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 262143) >> 12)                | ((src[7] & 262143) << 6)) & 255);
                            dest[16] = 
                (byte)((((src[7] & 262143) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[7] & 262143) >> 10)) & 255);
                        }
        private static void Unpack8LongValuesBE18(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 10) & 262143) | ((((long)src[1]) << 2) & 1023) | ((((long)src[2]) >> 6) & 3);            dest[1] = ((((long)src[2]) << 12) & 262143) | ((((long)src[3]) << 4) & 4095) | ((((long)src[4]) >> 4) & 15);            dest[2] = ((((long)src[4]) << 14) & 262143) | ((((long)src[5]) << 6) & 16383) | ((((long)src[6]) >> 2) & 63);            dest[3] = ((((long)src[6]) << 16) & 262143) | ((((long)src[7]) << 8) & 65535) | ((((long)src[8])) & 255);            dest[4] = ((((long)src[9]) << 10) & 262143) | ((((long)src[10]) << 2) & 1023) | ((((long)src[11]) >> 6) & 3);            dest[5] = ((((long)src[11]) << 12) & 262143) | ((((long)src[12]) << 4) & 4095) | ((((long)src[13]) >> 4) & 15);            dest[6] = ((((long)src[13]) << 14) & 262143) | ((((long)src[14]) << 6) & 16383) | ((((long)src[15]) >> 2) & 63);            dest[7] = ((((long)src[15]) << 16) & 262143) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17])) & 255);        }

        private static void Pack8LongValuesBE18(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 262143) >> 10)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 262143) >> 2)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 262143) << 6)                | ((src[1] & 262143) >> 12)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 262143) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 262143) << 4)                | ((src[2] & 262143) >> 14)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 262143) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 262143) << 2)                | ((src[3] & 262143) >> 16)) & 255);
                            dest[7] = 
                (byte)((((src[3] & 262143) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 262143))) & 255);
                            dest[9] = 
                (byte)((((src[4] & 262143) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 262143) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 262143) << 6)                | ((src[5] & 262143) >> 12)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 262143) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 262143) << 4)                | ((src[6] & 262143) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[6] & 262143) >> 6)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 262143) << 2)                | ((src[7] & 262143) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[7] & 262143) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[7] & 262143))) & 255);
                        }
        private static void Unpack8LongValuesLE19(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 524287);            dest[1] = ((((long)src[2]) >> 3) & 31) | ((((long)src[3]) << 5) & 8191) | ((((long)src[4]) << 13) & 524287);            dest[2] = ((((long)src[4]) >> 6) & 3) | ((((long)src[5]) << 2) & 1023) | ((((long)src[6]) << 10) & 262143) | ((((long)src[7]) << 18) & 524287);            dest[3] = ((((long)src[7]) >> 1) & 127) | ((((long)src[8]) << 7) & 32767) | ((((long)src[9]) << 15) & 524287);            dest[4] = ((((long)src[9]) >> 4) & 15) | ((((long)src[10]) << 4) & 4095) | ((((long)src[11]) << 12) & 524287);            dest[5] = ((((long)src[11]) >> 7) & 1) | ((((long)src[12]) << 1) & 511) | ((((long)src[13]) << 9) & 131071) | ((((long)src[14]) << 17) & 524287);            dest[6] = ((((long)src[14]) >> 2) & 63) | ((((long)src[15]) << 6) & 16383) | ((((long)src[16]) << 14) & 524287);            dest[7] = ((((long)src[16]) >> 5) & 7) | ((((long)src[17]) << 3) & 2047) | ((((long)src[18]) << 11) & 524287);        }

        private static void Pack8LongValuesLE19(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 524287))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 524287) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 524287) >> 16)                | ((src[1] & 524287) << 3)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 524287) >> 5)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 524287) >> 13)                | ((src[2] & 524287) << 6)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 524287) >> 2)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 524287) >> 10)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 524287) >> 18)                | ((src[3] & 524287) << 1)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 524287) >> 7)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 524287) >> 15)                | ((src[4] & 524287) << 4)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 524287) >> 4)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 524287) >> 12)                | ((src[5] & 524287) << 7)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 524287) >> 1)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 524287) >> 9)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 524287) >> 17)                | ((src[6] & 524287) << 2)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 524287) >> 6)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 524287) >> 14)                | ((src[7] & 524287) << 5)) & 255);
                            dest[17] = 
                (byte)((((src[7] & 524287) >> 3)) & 255);
                            dest[18] = 
                (byte)((((src[7] & 524287) >> 11)) & 255);
                        }
        private static void Unpack8LongValuesBE19(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 11) & 524287) | ((((long)src[1]) << 3) & 2047) | ((((long)src[2]) >> 5) & 7);            dest[1] = ((((long)src[2]) << 14) & 524287) | ((((long)src[3]) << 6) & 16383) | ((((long)src[4]) >> 2) & 63);            dest[2] = ((((long)src[4]) << 17) & 524287) | ((((long)src[5]) << 9) & 131071) | ((((long)src[6]) << 1) & 511) | ((((long)src[7]) >> 7) & 1);            dest[3] = ((((long)src[7]) << 12) & 524287) | ((((long)src[8]) << 4) & 4095) | ((((long)src[9]) >> 4) & 15);            dest[4] = ((((long)src[9]) << 15) & 524287) | ((((long)src[10]) << 7) & 32767) | ((((long)src[11]) >> 1) & 127);            dest[5] = ((((long)src[11]) << 18) & 524287) | ((((long)src[12]) << 10) & 262143) | ((((long)src[13]) << 2) & 1023) | ((((long)src[14]) >> 6) & 3);            dest[6] = ((((long)src[14]) << 13) & 524287) | ((((long)src[15]) << 5) & 8191) | ((((long)src[16]) >> 3) & 31);            dest[7] = ((((long)src[16]) << 16) & 524287) | ((((long)src[17]) << 8) & 65535) | ((((long)src[18])) & 255);        }

        private static void Pack8LongValuesBE19(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 524287) >> 11)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 524287) >> 3)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 524287) << 5)                | ((src[1] & 524287) >> 14)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 524287) >> 6)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 524287) << 2)                | ((src[2] & 524287) >> 17)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 524287) >> 9)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 524287) >> 1)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 524287) << 7)                | ((src[3] & 524287) >> 12)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 524287) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 524287) << 4)                | ((src[4] & 524287) >> 15)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 524287) >> 7)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 524287) << 1)                | ((src[5] & 524287) >> 18)) & 255);
                            dest[12] = 
                (byte)((((src[5] & 524287) >> 10)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 524287) >> 2)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 524287) << 6)                | ((src[6] & 524287) >> 13)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 524287) >> 5)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 524287) << 3)                | ((src[7] & 524287) >> 16)) & 255);
                            dest[17] = 
                (byte)((((src[7] & 524287) >> 8)) & 255);
                            dest[18] = 
                (byte)((((src[7] & 524287))) & 255);
                        }
        private static void Unpack8LongValuesLE20(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 1048575);            dest[1] = ((((long)src[2]) >> 4) & 15) | ((((long)src[3]) << 4) & 4095) | ((((long)src[4]) << 12) & 1048575);            dest[2] = ((((long)src[5])) & 255) | ((((long)src[6]) << 8) & 65535) | ((((long)src[7]) << 16) & 1048575);            dest[3] = ((((long)src[7]) >> 4) & 15) | ((((long)src[8]) << 4) & 4095) | ((((long)src[9]) << 12) & 1048575);            dest[4] = ((((long)src[10])) & 255) | ((((long)src[11]) << 8) & 65535) | ((((long)src[12]) << 16) & 1048575);            dest[5] = ((((long)src[12]) >> 4) & 15) | ((((long)src[13]) << 4) & 4095) | ((((long)src[14]) << 12) & 1048575);            dest[6] = ((((long)src[15])) & 255) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17]) << 16) & 1048575);            dest[7] = ((((long)src[17]) >> 4) & 15) | ((((long)src[18]) << 4) & 4095) | ((((long)src[19]) << 12) & 1048575);        }

        private static void Pack8LongValuesLE20(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1048575))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1048575) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1048575) >> 16)                | ((src[1] & 1048575) << 4)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 1048575) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 1048575) >> 12)) & 255);
                            dest[5] = 
                (byte)((((src[2] & 1048575))) & 255);
                            dest[6] = 
                (byte)((((src[2] & 1048575) >> 8)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 1048575) >> 16)                | ((src[3] & 1048575) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 1048575) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 1048575) >> 12)) & 255);
                            dest[10] = 
                (byte)((((src[4] & 1048575))) & 255);
                            dest[11] = 
                (byte)((((src[4] & 1048575) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 1048575) >> 16)                | ((src[5] & 1048575) << 4)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 1048575) >> 4)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 1048575) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[6] & 1048575))) & 255);
                            dest[16] = 
                (byte)((((src[6] & 1048575) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 1048575) >> 16)                | ((src[7] & 1048575) << 4)) & 255);
                            dest[18] = 
                (byte)((((src[7] & 1048575) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[7] & 1048575) >> 12)) & 255);
                        }
        private static void Unpack8LongValuesBE20(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 12) & 1048575) | ((((long)src[1]) << 4) & 4095) | ((((long)src[2]) >> 4) & 15);            dest[1] = ((((long)src[2]) << 16) & 1048575) | ((((long)src[3]) << 8) & 65535) | ((((long)src[4])) & 255);            dest[2] = ((((long)src[5]) << 12) & 1048575) | ((((long)src[6]) << 4) & 4095) | ((((long)src[7]) >> 4) & 15);            dest[3] = ((((long)src[7]) << 16) & 1048575) | ((((long)src[8]) << 8) & 65535) | ((((long)src[9])) & 255);            dest[4] = ((((long)src[10]) << 12) & 1048575) | ((((long)src[11]) << 4) & 4095) | ((((long)src[12]) >> 4) & 15);            dest[5] = ((((long)src[12]) << 16) & 1048575) | ((((long)src[13]) << 8) & 65535) | ((((long)src[14])) & 255);            dest[6] = ((((long)src[15]) << 12) & 1048575) | ((((long)src[16]) << 4) & 4095) | ((((long)src[17]) >> 4) & 15);            dest[7] = ((((long)src[17]) << 16) & 1048575) | ((((long)src[18]) << 8) & 65535) | ((((long)src[19])) & 255);        }

        private static void Pack8LongValuesBE20(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1048575) >> 12)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1048575) >> 4)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1048575) << 4)                | ((src[1] & 1048575) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 1048575) >> 8)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 1048575))) & 255);
                            dest[5] = 
                (byte)((((src[2] & 1048575) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 1048575) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 1048575) << 4)                | ((src[3] & 1048575) >> 16)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 1048575) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 1048575))) & 255);
                            dest[10] = 
                (byte)((((src[4] & 1048575) >> 12)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 1048575) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 1048575) << 4)                | ((src[5] & 1048575) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[5] & 1048575) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 1048575))) & 255);
                            dest[15] = 
                (byte)((((src[6] & 1048575) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 1048575) >> 4)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 1048575) << 4)                | ((src[7] & 1048575) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[7] & 1048575) >> 8)) & 255);
                            dest[19] = 
                (byte)((((src[7] & 1048575))) & 255);
                        }
        private static void Unpack8LongValuesLE21(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 2097151);            dest[1] = ((((long)src[2]) >> 5) & 7) | ((((long)src[3]) << 3) & 2047) | ((((long)src[4]) << 11) & 524287) | ((((long)src[5]) << 19) & 2097151);            dest[2] = ((((long)src[5]) >> 2) & 63) | ((((long)src[6]) << 6) & 16383) | ((((long)src[7]) << 14) & 2097151);            dest[3] = ((((long)src[7]) >> 7) & 1) | ((((long)src[8]) << 1) & 511) | ((((long)src[9]) << 9) & 131071) | ((((long)src[10]) << 17) & 2097151);            dest[4] = ((((long)src[10]) >> 4) & 15) | ((((long)src[11]) << 4) & 4095) | ((((long)src[12]) << 12) & 1048575) | ((((long)src[13]) << 20) & 2097151);            dest[5] = ((((long)src[13]) >> 1) & 127) | ((((long)src[14]) << 7) & 32767) | ((((long)src[15]) << 15) & 2097151);            dest[6] = ((((long)src[15]) >> 6) & 3) | ((((long)src[16]) << 2) & 1023) | ((((long)src[17]) << 10) & 262143) | ((((long)src[18]) << 18) & 2097151);            dest[7] = ((((long)src[18]) >> 3) & 31) | ((((long)src[19]) << 5) & 8191) | ((((long)src[20]) << 13) & 2097151);        }

        private static void Pack8LongValuesLE21(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2097151))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2097151) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2097151) >> 16)                | ((src[1] & 2097151) << 5)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 2097151) >> 3)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 2097151) >> 11)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 2097151) >> 19)                | ((src[2] & 2097151) << 2)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 2097151) >> 6)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 2097151) >> 14)                | ((src[3] & 2097151) << 7)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 2097151) >> 1)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 2097151) >> 9)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 2097151) >> 17)                | ((src[4] & 2097151) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 2097151) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 2097151) >> 12)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 2097151) >> 20)                | ((src[5] & 2097151) << 1)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 2097151) >> 7)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 2097151) >> 15)                | ((src[6] & 2097151) << 6)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 2097151) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 2097151) >> 10)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 2097151) >> 18)                | ((src[7] & 2097151) << 3)) & 255);
                            dest[19] = 
                (byte)((((src[7] & 2097151) >> 5)) & 255);
                            dest[20] = 
                (byte)((((src[7] & 2097151) >> 13)) & 255);
                        }
        private static void Unpack8LongValuesBE21(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 13) & 2097151) | ((((long)src[1]) << 5) & 8191) | ((((long)src[2]) >> 3) & 31);            dest[1] = ((((long)src[2]) << 18) & 2097151) | ((((long)src[3]) << 10) & 262143) | ((((long)src[4]) << 2) & 1023) | ((((long)src[5]) >> 6) & 3);            dest[2] = ((((long)src[5]) << 15) & 2097151) | ((((long)src[6]) << 7) & 32767) | ((((long)src[7]) >> 1) & 127);            dest[3] = ((((long)src[7]) << 20) & 2097151) | ((((long)src[8]) << 12) & 1048575) | ((((long)src[9]) << 4) & 4095) | ((((long)src[10]) >> 4) & 15);            dest[4] = ((((long)src[10]) << 17) & 2097151) | ((((long)src[11]) << 9) & 131071) | ((((long)src[12]) << 1) & 511) | ((((long)src[13]) >> 7) & 1);            dest[5] = ((((long)src[13]) << 14) & 2097151) | ((((long)src[14]) << 6) & 16383) | ((((long)src[15]) >> 2) & 63);            dest[6] = ((((long)src[15]) << 19) & 2097151) | ((((long)src[16]) << 11) & 524287) | ((((long)src[17]) << 3) & 2047) | ((((long)src[18]) >> 5) & 7);            dest[7] = ((((long)src[18]) << 16) & 2097151) | ((((long)src[19]) << 8) & 65535) | ((((long)src[20])) & 255);        }

        private static void Pack8LongValuesBE21(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2097151) >> 13)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2097151) >> 5)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2097151) << 3)                | ((src[1] & 2097151) >> 18)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 2097151) >> 10)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 2097151) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 2097151) << 6)                | ((src[2] & 2097151) >> 15)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 2097151) >> 7)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 2097151) << 1)                | ((src[3] & 2097151) >> 20)) & 255);
                            dest[8] = 
                (byte)((((src[3] & 2097151) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 2097151) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 2097151) << 4)                | ((src[4] & 2097151) >> 17)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 2097151) >> 9)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 2097151) >> 1)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 2097151) << 7)                | ((src[5] & 2097151) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 2097151) >> 6)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 2097151) << 2)                | ((src[6] & 2097151) >> 19)) & 255);
                            dest[16] = 
                (byte)((((src[6] & 2097151) >> 11)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 2097151) >> 3)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 2097151) << 5)                | ((src[7] & 2097151) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[7] & 2097151) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[7] & 2097151))) & 255);
                        }
        private static void Unpack8LongValuesLE22(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 4194303);            dest[1] = ((((long)src[2]) >> 6) & 3) | ((((long)src[3]) << 2) & 1023) | ((((long)src[4]) << 10) & 262143) | ((((long)src[5]) << 18) & 4194303);            dest[2] = ((((long)src[5]) >> 4) & 15) | ((((long)src[6]) << 4) & 4095) | ((((long)src[7]) << 12) & 1048575) | ((((long)src[8]) << 20) & 4194303);            dest[3] = ((((long)src[8]) >> 2) & 63) | ((((long)src[9]) << 6) & 16383) | ((((long)src[10]) << 14) & 4194303);            dest[4] = ((((long)src[11])) & 255) | ((((long)src[12]) << 8) & 65535) | ((((long)src[13]) << 16) & 4194303);            dest[5] = ((((long)src[13]) >> 6) & 3) | ((((long)src[14]) << 2) & 1023) | ((((long)src[15]) << 10) & 262143) | ((((long)src[16]) << 18) & 4194303);            dest[6] = ((((long)src[16]) >> 4) & 15) | ((((long)src[17]) << 4) & 4095) | ((((long)src[18]) << 12) & 1048575) | ((((long)src[19]) << 20) & 4194303);            dest[7] = ((((long)src[19]) >> 2) & 63) | ((((long)src[20]) << 6) & 16383) | ((((long)src[21]) << 14) & 4194303);        }

        private static void Pack8LongValuesLE22(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4194303))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4194303) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4194303) >> 16)                | ((src[1] & 4194303) << 6)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 4194303) >> 2)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 4194303) >> 10)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 4194303) >> 18)                | ((src[2] & 4194303) << 4)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 4194303) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 4194303) >> 12)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 4194303) >> 20)                | ((src[3] & 4194303) << 2)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 4194303) >> 6)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 4194303) >> 14)) & 255);
                            dest[11] = 
                (byte)((((src[4] & 4194303))) & 255);
                            dest[12] = 
                (byte)((((src[4] & 4194303) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 4194303) >> 16)                | ((src[5] & 4194303) << 6)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 4194303) >> 2)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 4194303) >> 10)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 4194303) >> 18)                | ((src[6] & 4194303) << 4)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 4194303) >> 4)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 4194303) >> 12)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 4194303) >> 20)                | ((src[7] & 4194303) << 2)) & 255);
                            dest[20] = 
                (byte)((((src[7] & 4194303) >> 6)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 4194303) >> 14)) & 255);
                        }
        private static void Unpack8LongValuesBE22(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 14) & 4194303) | ((((long)src[1]) << 6) & 16383) | ((((long)src[2]) >> 2) & 63);            dest[1] = ((((long)src[2]) << 20) & 4194303) | ((((long)src[3]) << 12) & 1048575) | ((((long)src[4]) << 4) & 4095) | ((((long)src[5]) >> 4) & 15);            dest[2] = ((((long)src[5]) << 18) & 4194303) | ((((long)src[6]) << 10) & 262143) | ((((long)src[7]) << 2) & 1023) | ((((long)src[8]) >> 6) & 3);            dest[3] = ((((long)src[8]) << 16) & 4194303) | ((((long)src[9]) << 8) & 65535) | ((((long)src[10])) & 255);            dest[4] = ((((long)src[11]) << 14) & 4194303) | ((((long)src[12]) << 6) & 16383) | ((((long)src[13]) >> 2) & 63);            dest[5] = ((((long)src[13]) << 20) & 4194303) | ((((long)src[14]) << 12) & 1048575) | ((((long)src[15]) << 4) & 4095) | ((((long)src[16]) >> 4) & 15);            dest[6] = ((((long)src[16]) << 18) & 4194303) | ((((long)src[17]) << 10) & 262143) | ((((long)src[18]) << 2) & 1023) | ((((long)src[19]) >> 6) & 3);            dest[7] = ((((long)src[19]) << 16) & 4194303) | ((((long)src[20]) << 8) & 65535) | ((((long)src[21])) & 255);        }

        private static void Pack8LongValuesBE22(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4194303) >> 14)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4194303) >> 6)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4194303) << 2)                | ((src[1] & 4194303) >> 20)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 4194303) >> 12)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 4194303) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 4194303) << 4)                | ((src[2] & 4194303) >> 18)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 4194303) >> 10)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 4194303) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 4194303) << 6)                | ((src[3] & 4194303) >> 16)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 4194303) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 4194303))) & 255);
                            dest[11] = 
                (byte)((((src[4] & 4194303) >> 14)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 4194303) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 4194303) << 2)                | ((src[5] & 4194303) >> 20)) & 255);
                            dest[14] = 
                (byte)((((src[5] & 4194303) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 4194303) >> 4)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 4194303) << 4)                | ((src[6] & 4194303) >> 18)) & 255);
                            dest[17] = 
                (byte)((((src[6] & 4194303) >> 10)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 4194303) >> 2)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 4194303) << 6)                | ((src[7] & 4194303) >> 16)) & 255);
                            dest[20] = 
                (byte)((((src[7] & 4194303) >> 8)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 4194303))) & 255);
                        }
        private static void Unpack8LongValuesLE23(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 8388607);            dest[1] = ((((long)src[2]) >> 7) & 1) | ((((long)src[3]) << 1) & 511) | ((((long)src[4]) << 9) & 131071) | ((((long)src[5]) << 17) & 8388607);            dest[2] = ((((long)src[5]) >> 6) & 3) | ((((long)src[6]) << 2) & 1023) | ((((long)src[7]) << 10) & 262143) | ((((long)src[8]) << 18) & 8388607);            dest[3] = ((((long)src[8]) >> 5) & 7) | ((((long)src[9]) << 3) & 2047) | ((((long)src[10]) << 11) & 524287) | ((((long)src[11]) << 19) & 8388607);            dest[4] = ((((long)src[11]) >> 4) & 15) | ((((long)src[12]) << 4) & 4095) | ((((long)src[13]) << 12) & 1048575) | ((((long)src[14]) << 20) & 8388607);            dest[5] = ((((long)src[14]) >> 3) & 31) | ((((long)src[15]) << 5) & 8191) | ((((long)src[16]) << 13) & 2097151) | ((((long)src[17]) << 21) & 8388607);            dest[6] = ((((long)src[17]) >> 2) & 63) | ((((long)src[18]) << 6) & 16383) | ((((long)src[19]) << 14) & 4194303) | ((((long)src[20]) << 22) & 8388607);            dest[7] = ((((long)src[20]) >> 1) & 127) | ((((long)src[21]) << 7) & 32767) | ((((long)src[22]) << 15) & 8388607);        }

        private static void Pack8LongValuesLE23(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8388607))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8388607) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 8388607) >> 16)                | ((src[1] & 8388607) << 7)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 8388607) >> 1)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 8388607) >> 9)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 8388607) >> 17)                | ((src[2] & 8388607) << 6)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 8388607) >> 2)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 8388607) >> 10)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 8388607) >> 18)                | ((src[3] & 8388607) << 5)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 8388607) >> 3)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 8388607) >> 11)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 8388607) >> 19)                | ((src[4] & 8388607) << 4)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 8388607) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 8388607) >> 12)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 8388607) >> 20)                | ((src[5] & 8388607) << 3)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 8388607) >> 5)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 8388607) >> 13)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 8388607) >> 21)                | ((src[6] & 8388607) << 2)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 8388607) >> 6)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 8388607) >> 14)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 8388607) >> 22)                | ((src[7] & 8388607) << 1)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 8388607) >> 7)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 8388607) >> 15)) & 255);
                        }
        private static void Unpack8LongValuesBE23(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 15) & 8388607) | ((((long)src[1]) << 7) & 32767) | ((((long)src[2]) >> 1) & 127);            dest[1] = ((((long)src[2]) << 22) & 8388607) | ((((long)src[3]) << 14) & 4194303) | ((((long)src[4]) << 6) & 16383) | ((((long)src[5]) >> 2) & 63);            dest[2] = ((((long)src[5]) << 21) & 8388607) | ((((long)src[6]) << 13) & 2097151) | ((((long)src[7]) << 5) & 8191) | ((((long)src[8]) >> 3) & 31);            dest[3] = ((((long)src[8]) << 20) & 8388607) | ((((long)src[9]) << 12) & 1048575) | ((((long)src[10]) << 4) & 4095) | ((((long)src[11]) >> 4) & 15);            dest[4] = ((((long)src[11]) << 19) & 8388607) | ((((long)src[12]) << 11) & 524287) | ((((long)src[13]) << 3) & 2047) | ((((long)src[14]) >> 5) & 7);            dest[5] = ((((long)src[14]) << 18) & 8388607) | ((((long)src[15]) << 10) & 262143) | ((((long)src[16]) << 2) & 1023) | ((((long)src[17]) >> 6) & 3);            dest[6] = ((((long)src[17]) << 17) & 8388607) | ((((long)src[18]) << 9) & 131071) | ((((long)src[19]) << 1) & 511) | ((((long)src[20]) >> 7) & 1);            dest[7] = ((((long)src[20]) << 16) & 8388607) | ((((long)src[21]) << 8) & 65535) | ((((long)src[22])) & 255);        }

        private static void Pack8LongValuesBE23(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8388607) >> 15)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8388607) >> 7)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 8388607) << 1)                | ((src[1] & 8388607) >> 22)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 8388607) >> 14)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 8388607) >> 6)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 8388607) << 2)                | ((src[2] & 8388607) >> 21)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 8388607) >> 13)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 8388607) >> 5)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 8388607) << 3)                | ((src[3] & 8388607) >> 20)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 8388607) >> 12)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 8388607) >> 4)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 8388607) << 4)                | ((src[4] & 8388607) >> 19)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 8388607) >> 11)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 8388607) >> 3)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 8388607) << 5)                | ((src[5] & 8388607) >> 18)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 8388607) >> 10)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 8388607) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 8388607) << 6)                | ((src[6] & 8388607) >> 17)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 8388607) >> 9)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 8388607) >> 1)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 8388607) << 7)                | ((src[7] & 8388607) >> 16)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 8388607) >> 8)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 8388607))) & 255);
                        }
        private static void Unpack8LongValuesLE24(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215);            dest[1] = ((((long)src[3])) & 255) | ((((long)src[4]) << 8) & 65535) | ((((long)src[5]) << 16) & 16777215);            dest[2] = ((((long)src[6])) & 255) | ((((long)src[7]) << 8) & 65535) | ((((long)src[8]) << 16) & 16777215);            dest[3] = ((((long)src[9])) & 255) | ((((long)src[10]) << 8) & 65535) | ((((long)src[11]) << 16) & 16777215);            dest[4] = ((((long)src[12])) & 255) | ((((long)src[13]) << 8) & 65535) | ((((long)src[14]) << 16) & 16777215);            dest[5] = ((((long)src[15])) & 255) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17]) << 16) & 16777215);            dest[6] = ((((long)src[18])) & 255) | ((((long)src[19]) << 8) & 65535) | ((((long)src[20]) << 16) & 16777215);            dest[7] = ((((long)src[21])) & 255) | ((((long)src[22]) << 8) & 65535) | ((((long)src[23]) << 16) & 16777215);        }

        private static void Pack8LongValuesLE24(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 16777215))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 16777215) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 16777215) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[1] & 16777215))) & 255);
                            dest[4] = 
                (byte)((((src[1] & 16777215) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 16777215) >> 16)) & 255);
                            dest[6] = 
                (byte)((((src[2] & 16777215))) & 255);
                            dest[7] = 
                (byte)((((src[2] & 16777215) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 16777215) >> 16)) & 255);
                            dest[9] = 
                (byte)((((src[3] & 16777215))) & 255);
                            dest[10] = 
                (byte)((((src[3] & 16777215) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 16777215) >> 16)) & 255);
                            dest[12] = 
                (byte)((((src[4] & 16777215))) & 255);
                            dest[13] = 
                (byte)((((src[4] & 16777215) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 16777215) >> 16)) & 255);
                            dest[15] = 
                (byte)((((src[5] & 16777215))) & 255);
                            dest[16] = 
                (byte)((((src[5] & 16777215) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 16777215) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[6] & 16777215))) & 255);
                            dest[19] = 
                (byte)((((src[6] & 16777215) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 16777215) >> 16)) & 255);
                            dest[21] = 
                (byte)((((src[7] & 16777215))) & 255);
                            dest[22] = 
                (byte)((((src[7] & 16777215) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 16777215) >> 16)) & 255);
                        }
        private static void Unpack8LongValuesBE24(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 16) & 16777215) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2])) & 255);            dest[1] = ((((long)src[3]) << 16) & 16777215) | ((((long)src[4]) << 8) & 65535) | ((((long)src[5])) & 255);            dest[2] = ((((long)src[6]) << 16) & 16777215) | ((((long)src[7]) << 8) & 65535) | ((((long)src[8])) & 255);            dest[3] = ((((long)src[9]) << 16) & 16777215) | ((((long)src[10]) << 8) & 65535) | ((((long)src[11])) & 255);            dest[4] = ((((long)src[12]) << 16) & 16777215) | ((((long)src[13]) << 8) & 65535) | ((((long)src[14])) & 255);            dest[5] = ((((long)src[15]) << 16) & 16777215) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17])) & 255);            dest[6] = ((((long)src[18]) << 16) & 16777215) | ((((long)src[19]) << 8) & 65535) | ((((long)src[20])) & 255);            dest[7] = ((((long)src[21]) << 16) & 16777215) | ((((long)src[22]) << 8) & 65535) | ((((long)src[23])) & 255);        }

        private static void Pack8LongValuesBE24(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 16777215) >> 16)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 16777215) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 16777215))) & 255);
                            dest[3] = 
                (byte)((((src[1] & 16777215) >> 16)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 16777215) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 16777215))) & 255);
                            dest[6] = 
                (byte)((((src[2] & 16777215) >> 16)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 16777215) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 16777215))) & 255);
                            dest[9] = 
                (byte)((((src[3] & 16777215) >> 16)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 16777215) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 16777215))) & 255);
                            dest[12] = 
                (byte)((((src[4] & 16777215) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 16777215) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 16777215))) & 255);
                            dest[15] = 
                (byte)((((src[5] & 16777215) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 16777215) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 16777215))) & 255);
                            dest[18] = 
                (byte)((((src[6] & 16777215) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 16777215) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 16777215))) & 255);
                            dest[21] = 
                (byte)((((src[7] & 16777215) >> 16)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 16777215) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 16777215))) & 255);
                        }
        private static void Unpack8LongValuesLE25(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 33554431);            dest[1] = ((((long)src[3]) >> 1) & 127) | ((((long)src[4]) << 7) & 32767) | ((((long)src[5]) << 15) & 8388607) | ((((long)src[6]) << 23) & 33554431);            dest[2] = ((((long)src[6]) >> 2) & 63) | ((((long)src[7]) << 6) & 16383) | ((((long)src[8]) << 14) & 4194303) | ((((long)src[9]) << 22) & 33554431);            dest[3] = ((((long)src[9]) >> 3) & 31) | ((((long)src[10]) << 5) & 8191) | ((((long)src[11]) << 13) & 2097151) | ((((long)src[12]) << 21) & 33554431);            dest[4] = ((((long)src[12]) >> 4) & 15) | ((((long)src[13]) << 4) & 4095) | ((((long)src[14]) << 12) & 1048575) | ((((long)src[15]) << 20) & 33554431);            dest[5] = ((((long)src[15]) >> 5) & 7) | ((((long)src[16]) << 3) & 2047) | ((((long)src[17]) << 11) & 524287) | ((((long)src[18]) << 19) & 33554431);            dest[6] = ((((long)src[18]) >> 6) & 3) | ((((long)src[19]) << 2) & 1023) | ((((long)src[20]) << 10) & 262143) | ((((long)src[21]) << 18) & 33554431);            dest[7] = ((((long)src[21]) >> 7) & 1) | ((((long)src[22]) << 1) & 511) | ((((long)src[23]) << 9) & 131071) | ((((long)src[24]) << 17) & 33554431);        }

        private static void Pack8LongValuesLE25(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 33554431))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 33554431) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 33554431) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 33554431) >> 24)                | ((src[1] & 33554431) << 1)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 33554431) >> 7)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 33554431) >> 15)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 33554431) >> 23)                | ((src[2] & 33554431) << 2)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 33554431) >> 6)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 33554431) >> 14)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 33554431) >> 22)                | ((src[3] & 33554431) << 3)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 33554431) >> 5)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 33554431) >> 13)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 33554431) >> 21)                | ((src[4] & 33554431) << 4)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 33554431) >> 4)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 33554431) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 33554431) >> 20)                | ((src[5] & 33554431) << 5)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 33554431) >> 3)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 33554431) >> 11)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 33554431) >> 19)                | ((src[6] & 33554431) << 6)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 33554431) >> 2)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 33554431) >> 10)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 33554431) >> 18)                | ((src[7] & 33554431) << 7)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 33554431) >> 1)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 33554431) >> 9)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 33554431) >> 17)) & 255);
                        }
        private static void Unpack8LongValuesBE25(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 17) & 33554431) | ((((long)src[1]) << 9) & 131071) | ((((long)src[2]) << 1) & 511) | ((((long)src[3]) >> 7) & 1);            dest[1] = ((((long)src[3]) << 18) & 33554431) | ((((long)src[4]) << 10) & 262143) | ((((long)src[5]) << 2) & 1023) | ((((long)src[6]) >> 6) & 3);            dest[2] = ((((long)src[6]) << 19) & 33554431) | ((((long)src[7]) << 11) & 524287) | ((((long)src[8]) << 3) & 2047) | ((((long)src[9]) >> 5) & 7);            dest[3] = ((((long)src[9]) << 20) & 33554431) | ((((long)src[10]) << 12) & 1048575) | ((((long)src[11]) << 4) & 4095) | ((((long)src[12]) >> 4) & 15);            dest[4] = ((((long)src[12]) << 21) & 33554431) | ((((long)src[13]) << 13) & 2097151) | ((((long)src[14]) << 5) & 8191) | ((((long)src[15]) >> 3) & 31);            dest[5] = ((((long)src[15]) << 22) & 33554431) | ((((long)src[16]) << 14) & 4194303) | ((((long)src[17]) << 6) & 16383) | ((((long)src[18]) >> 2) & 63);            dest[6] = ((((long)src[18]) << 23) & 33554431) | ((((long)src[19]) << 15) & 8388607) | ((((long)src[20]) << 7) & 32767) | ((((long)src[21]) >> 1) & 127);            dest[7] = ((((long)src[21]) << 24) & 33554431) | ((((long)src[22]) << 16) & 16777215) | ((((long)src[23]) << 8) & 65535) | ((((long)src[24])) & 255);        }

        private static void Pack8LongValuesBE25(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 33554431) >> 17)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 33554431) >> 9)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 33554431) >> 1)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 33554431) << 7)                | ((src[1] & 33554431) >> 18)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 33554431) >> 10)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 33554431) >> 2)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 33554431) << 6)                | ((src[2] & 33554431) >> 19)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 33554431) >> 11)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 33554431) >> 3)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 33554431) << 5)                | ((src[3] & 33554431) >> 20)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 33554431) >> 12)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 33554431) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 33554431) << 4)                | ((src[4] & 33554431) >> 21)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 33554431) >> 13)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 33554431) >> 5)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 33554431) << 3)                | ((src[5] & 33554431) >> 22)) & 255);
                            dest[16] = 
                (byte)((((src[5] & 33554431) >> 14)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 33554431) >> 6)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 33554431) << 2)                | ((src[6] & 33554431) >> 23)) & 255);
                            dest[19] = 
                (byte)((((src[6] & 33554431) >> 15)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 33554431) >> 7)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 33554431) << 1)                | ((src[7] & 33554431) >> 24)) & 255);
                            dest[22] = 
                (byte)((((src[7] & 33554431) >> 16)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 33554431) >> 8)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 33554431))) & 255);
                        }
        private static void Unpack8LongValuesLE26(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 67108863);            dest[1] = ((((long)src[3]) >> 2) & 63) | ((((long)src[4]) << 6) & 16383) | ((((long)src[5]) << 14) & 4194303) | ((((long)src[6]) << 22) & 67108863);            dest[2] = ((((long)src[6]) >> 4) & 15) | ((((long)src[7]) << 4) & 4095) | ((((long)src[8]) << 12) & 1048575) | ((((long)src[9]) << 20) & 67108863);            dest[3] = ((((long)src[9]) >> 6) & 3) | ((((long)src[10]) << 2) & 1023) | ((((long)src[11]) << 10) & 262143) | ((((long)src[12]) << 18) & 67108863);            dest[4] = ((((long)src[13])) & 255) | ((((long)src[14]) << 8) & 65535) | ((((long)src[15]) << 16) & 16777215) | ((((long)src[16]) << 24) & 67108863);            dest[5] = ((((long)src[16]) >> 2) & 63) | ((((long)src[17]) << 6) & 16383) | ((((long)src[18]) << 14) & 4194303) | ((((long)src[19]) << 22) & 67108863);            dest[6] = ((((long)src[19]) >> 4) & 15) | ((((long)src[20]) << 4) & 4095) | ((((long)src[21]) << 12) & 1048575) | ((((long)src[22]) << 20) & 67108863);            dest[7] = ((((long)src[22]) >> 6) & 3) | ((((long)src[23]) << 2) & 1023) | ((((long)src[24]) << 10) & 262143) | ((((long)src[25]) << 18) & 67108863);        }

        private static void Pack8LongValuesLE26(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 67108863))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 67108863) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 67108863) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 67108863) >> 24)                | ((src[1] & 67108863) << 2)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 67108863) >> 6)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 67108863) >> 14)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 67108863) >> 22)                | ((src[2] & 67108863) << 4)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 67108863) >> 4)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 67108863) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 67108863) >> 20)                | ((src[3] & 67108863) << 6)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 67108863) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 67108863) >> 10)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 67108863) >> 18)) & 255);
                            dest[13] = 
                (byte)((((src[4] & 67108863))) & 255);
                            dest[14] = 
                (byte)((((src[4] & 67108863) >> 8)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 67108863) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 67108863) >> 24)                | ((src[5] & 67108863) << 2)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 67108863) >> 6)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 67108863) >> 14)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 67108863) >> 22)                | ((src[6] & 67108863) << 4)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 67108863) >> 4)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 67108863) >> 12)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 67108863) >> 20)                | ((src[7] & 67108863) << 6)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 67108863) >> 2)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 67108863) >> 10)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 67108863) >> 18)) & 255);
                        }
        private static void Unpack8LongValuesBE26(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 18) & 67108863) | ((((long)src[1]) << 10) & 262143) | ((((long)src[2]) << 2) & 1023) | ((((long)src[3]) >> 6) & 3);            dest[1] = ((((long)src[3]) << 20) & 67108863) | ((((long)src[4]) << 12) & 1048575) | ((((long)src[5]) << 4) & 4095) | ((((long)src[6]) >> 4) & 15);            dest[2] = ((((long)src[6]) << 22) & 67108863) | ((((long)src[7]) << 14) & 4194303) | ((((long)src[8]) << 6) & 16383) | ((((long)src[9]) >> 2) & 63);            dest[3] = ((((long)src[9]) << 24) & 67108863) | ((((long)src[10]) << 16) & 16777215) | ((((long)src[11]) << 8) & 65535) | ((((long)src[12])) & 255);            dest[4] = ((((long)src[13]) << 18) & 67108863) | ((((long)src[14]) << 10) & 262143) | ((((long)src[15]) << 2) & 1023) | ((((long)src[16]) >> 6) & 3);            dest[5] = ((((long)src[16]) << 20) & 67108863) | ((((long)src[17]) << 12) & 1048575) | ((((long)src[18]) << 4) & 4095) | ((((long)src[19]) >> 4) & 15);            dest[6] = ((((long)src[19]) << 22) & 67108863) | ((((long)src[20]) << 14) & 4194303) | ((((long)src[21]) << 6) & 16383) | ((((long)src[22]) >> 2) & 63);            dest[7] = ((((long)src[22]) << 24) & 67108863) | ((((long)src[23]) << 16) & 16777215) | ((((long)src[24]) << 8) & 65535) | ((((long)src[25])) & 255);        }

        private static void Pack8LongValuesBE26(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 67108863) >> 18)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 67108863) >> 10)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 67108863) >> 2)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 67108863) << 6)                | ((src[1] & 67108863) >> 20)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 67108863) >> 12)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 67108863) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 67108863) << 4)                | ((src[2] & 67108863) >> 22)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 67108863) >> 14)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 67108863) >> 6)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 67108863) << 2)                | ((src[3] & 67108863) >> 24)) & 255);
                            dest[10] = 
                (byte)((((src[3] & 67108863) >> 16)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 67108863) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 67108863))) & 255);
                            dest[13] = 
                (byte)((((src[4] & 67108863) >> 18)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 67108863) >> 10)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 67108863) >> 2)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 67108863) << 6)                | ((src[5] & 67108863) >> 20)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 67108863) >> 12)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 67108863) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 67108863) << 4)                | ((src[6] & 67108863) >> 22)) & 255);
                            dest[20] = 
                (byte)((((src[6] & 67108863) >> 14)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 67108863) >> 6)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 67108863) << 2)                | ((src[7] & 67108863) >> 24)) & 255);
                            dest[23] = 
                (byte)((((src[7] & 67108863) >> 16)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 67108863) >> 8)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 67108863))) & 255);
                        }
        private static void Unpack8LongValuesLE27(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 134217727);            dest[1] = ((((long)src[3]) >> 3) & 31) | ((((long)src[4]) << 5) & 8191) | ((((long)src[5]) << 13) & 2097151) | ((((long)src[6]) << 21) & 134217727);            dest[2] = ((((long)src[6]) >> 6) & 3) | ((((long)src[7]) << 2) & 1023) | ((((long)src[8]) << 10) & 262143) | ((((long)src[9]) << 18) & 67108863) | ((((long)src[10]) << 26) & 134217727);            dest[3] = ((((long)src[10]) >> 1) & 127) | ((((long)src[11]) << 7) & 32767) | ((((long)src[12]) << 15) & 8388607) | ((((long)src[13]) << 23) & 134217727);            dest[4] = ((((long)src[13]) >> 4) & 15) | ((((long)src[14]) << 4) & 4095) | ((((long)src[15]) << 12) & 1048575) | ((((long)src[16]) << 20) & 134217727);            dest[5] = ((((long)src[16]) >> 7) & 1) | ((((long)src[17]) << 1) & 511) | ((((long)src[18]) << 9) & 131071) | ((((long)src[19]) << 17) & 33554431) | ((((long)src[20]) << 25) & 134217727);            dest[6] = ((((long)src[20]) >> 2) & 63) | ((((long)src[21]) << 6) & 16383) | ((((long)src[22]) << 14) & 4194303) | ((((long)src[23]) << 22) & 134217727);            dest[7] = ((((long)src[23]) >> 5) & 7) | ((((long)src[24]) << 3) & 2047) | ((((long)src[25]) << 11) & 524287) | ((((long)src[26]) << 19) & 134217727);        }

        private static void Pack8LongValuesLE27(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 134217727))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 134217727) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 134217727) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 134217727) >> 24)                | ((src[1] & 134217727) << 3)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 134217727) >> 5)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 134217727) >> 13)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 134217727) >> 21)                | ((src[2] & 134217727) << 6)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 134217727) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 134217727) >> 10)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 134217727) >> 18)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 134217727) >> 26)                | ((src[3] & 134217727) << 1)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 134217727) >> 7)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 134217727) >> 15)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 134217727) >> 23)                | ((src[4] & 134217727) << 4)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 134217727) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 134217727) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 134217727) >> 20)                | ((src[5] & 134217727) << 7)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 134217727) >> 1)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 134217727) >> 9)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 134217727) >> 17)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 134217727) >> 25)                | ((src[6] & 134217727) << 2)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 134217727) >> 6)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 134217727) >> 14)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 134217727) >> 22)                | ((src[7] & 134217727) << 5)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 134217727) >> 3)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 134217727) >> 11)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 134217727) >> 19)) & 255);
                        }
        private static void Unpack8LongValuesBE27(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 19) & 134217727) | ((((long)src[1]) << 11) & 524287) | ((((long)src[2]) << 3) & 2047) | ((((long)src[3]) >> 5) & 7);            dest[1] = ((((long)src[3]) << 22) & 134217727) | ((((long)src[4]) << 14) & 4194303) | ((((long)src[5]) << 6) & 16383) | ((((long)src[6]) >> 2) & 63);            dest[2] = ((((long)src[6]) << 25) & 134217727) | ((((long)src[7]) << 17) & 33554431) | ((((long)src[8]) << 9) & 131071) | ((((long)src[9]) << 1) & 511) | ((((long)src[10]) >> 7) & 1);            dest[3] = ((((long)src[10]) << 20) & 134217727) | ((((long)src[11]) << 12) & 1048575) | ((((long)src[12]) << 4) & 4095) | ((((long)src[13]) >> 4) & 15);            dest[4] = ((((long)src[13]) << 23) & 134217727) | ((((long)src[14]) << 15) & 8388607) | ((((long)src[15]) << 7) & 32767) | ((((long)src[16]) >> 1) & 127);            dest[5] = ((((long)src[16]) << 26) & 134217727) | ((((long)src[17]) << 18) & 67108863) | ((((long)src[18]) << 10) & 262143) | ((((long)src[19]) << 2) & 1023) | ((((long)src[20]) >> 6) & 3);            dest[6] = ((((long)src[20]) << 21) & 134217727) | ((((long)src[21]) << 13) & 2097151) | ((((long)src[22]) << 5) & 8191) | ((((long)src[23]) >> 3) & 31);            dest[7] = ((((long)src[23]) << 24) & 134217727) | ((((long)src[24]) << 16) & 16777215) | ((((long)src[25]) << 8) & 65535) | ((((long)src[26])) & 255);        }

        private static void Pack8LongValuesBE27(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 134217727) >> 19)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 134217727) >> 11)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 134217727) >> 3)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 134217727) << 5)                | ((src[1] & 134217727) >> 22)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 134217727) >> 14)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 134217727) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 134217727) << 2)                | ((src[2] & 134217727) >> 25)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 134217727) >> 17)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 134217727) >> 9)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 134217727) >> 1)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 134217727) << 7)                | ((src[3] & 134217727) >> 20)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 134217727) >> 12)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 134217727) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 134217727) << 4)                | ((src[4] & 134217727) >> 23)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 134217727) >> 15)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 134217727) >> 7)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 134217727) << 1)                | ((src[5] & 134217727) >> 26)) & 255);
                            dest[17] = 
                (byte)((((src[5] & 134217727) >> 18)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 134217727) >> 10)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 134217727) >> 2)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 134217727) << 6)                | ((src[6] & 134217727) >> 21)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 134217727) >> 13)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 134217727) >> 5)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 134217727) << 3)                | ((src[7] & 134217727) >> 24)) & 255);
                            dest[24] = 
                (byte)((((src[7] & 134217727) >> 16)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 134217727) >> 8)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 134217727))) & 255);
                        }
        private static void Unpack8LongValuesLE28(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 268435455);            dest[1] = ((((long)src[3]) >> 4) & 15) | ((((long)src[4]) << 4) & 4095) | ((((long)src[5]) << 12) & 1048575) | ((((long)src[6]) << 20) & 268435455);            dest[2] = ((((long)src[7])) & 255) | ((((long)src[8]) << 8) & 65535) | ((((long)src[9]) << 16) & 16777215) | ((((long)src[10]) << 24) & 268435455);            dest[3] = ((((long)src[10]) >> 4) & 15) | ((((long)src[11]) << 4) & 4095) | ((((long)src[12]) << 12) & 1048575) | ((((long)src[13]) << 20) & 268435455);            dest[4] = ((((long)src[14])) & 255) | ((((long)src[15]) << 8) & 65535) | ((((long)src[16]) << 16) & 16777215) | ((((long)src[17]) << 24) & 268435455);            dest[5] = ((((long)src[17]) >> 4) & 15) | ((((long)src[18]) << 4) & 4095) | ((((long)src[19]) << 12) & 1048575) | ((((long)src[20]) << 20) & 268435455);            dest[6] = ((((long)src[21])) & 255) | ((((long)src[22]) << 8) & 65535) | ((((long)src[23]) << 16) & 16777215) | ((((long)src[24]) << 24) & 268435455);            dest[7] = ((((long)src[24]) >> 4) & 15) | ((((long)src[25]) << 4) & 4095) | ((((long)src[26]) << 12) & 1048575) | ((((long)src[27]) << 20) & 268435455);        }

        private static void Pack8LongValuesLE28(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 268435455))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 268435455) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 268435455) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 268435455) >> 24)                | ((src[1] & 268435455) << 4)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 268435455) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 268435455) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 268435455) >> 20)) & 255);
                            dest[7] = 
                (byte)((((src[2] & 268435455))) & 255);
                            dest[8] = 
                (byte)((((src[2] & 268435455) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 268435455) >> 16)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 268435455) >> 24)                | ((src[3] & 268435455) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 268435455) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 268435455) >> 12)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 268435455) >> 20)) & 255);
                            dest[14] = 
                (byte)((((src[4] & 268435455))) & 255);
                            dest[15] = 
                (byte)((((src[4] & 268435455) >> 8)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 268435455) >> 16)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 268435455) >> 24)                | ((src[5] & 268435455) << 4)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 268435455) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 268435455) >> 12)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 268435455) >> 20)) & 255);
                            dest[21] = 
                (byte)((((src[6] & 268435455))) & 255);
                            dest[22] = 
                (byte)((((src[6] & 268435455) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 268435455) >> 16)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 268435455) >> 24)                | ((src[7] & 268435455) << 4)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 268435455) >> 4)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 268435455) >> 12)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 268435455) >> 20)) & 255);
                        }
        private static void Unpack8LongValuesBE28(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 20) & 268435455) | ((((long)src[1]) << 12) & 1048575) | ((((long)src[2]) << 4) & 4095) | ((((long)src[3]) >> 4) & 15);            dest[1] = ((((long)src[3]) << 24) & 268435455) | ((((long)src[4]) << 16) & 16777215) | ((((long)src[5]) << 8) & 65535) | ((((long)src[6])) & 255);            dest[2] = ((((long)src[7]) << 20) & 268435455) | ((((long)src[8]) << 12) & 1048575) | ((((long)src[9]) << 4) & 4095) | ((((long)src[10]) >> 4) & 15);            dest[3] = ((((long)src[10]) << 24) & 268435455) | ((((long)src[11]) << 16) & 16777215) | ((((long)src[12]) << 8) & 65535) | ((((long)src[13])) & 255);            dest[4] = ((((long)src[14]) << 20) & 268435455) | ((((long)src[15]) << 12) & 1048575) | ((((long)src[16]) << 4) & 4095) | ((((long)src[17]) >> 4) & 15);            dest[5] = ((((long)src[17]) << 24) & 268435455) | ((((long)src[18]) << 16) & 16777215) | ((((long)src[19]) << 8) & 65535) | ((((long)src[20])) & 255);            dest[6] = ((((long)src[21]) << 20) & 268435455) | ((((long)src[22]) << 12) & 1048575) | ((((long)src[23]) << 4) & 4095) | ((((long)src[24]) >> 4) & 15);            dest[7] = ((((long)src[24]) << 24) & 268435455) | ((((long)src[25]) << 16) & 16777215) | ((((long)src[26]) << 8) & 65535) | ((((long)src[27])) & 255);        }

        private static void Pack8LongValuesBE28(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 268435455) >> 20)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 268435455) >> 12)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 268435455) >> 4)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 268435455) << 4)                | ((src[1] & 268435455) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 268435455) >> 16)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 268435455) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 268435455))) & 255);
                            dest[7] = 
                (byte)((((src[2] & 268435455) >> 20)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 268435455) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 268435455) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 268435455) << 4)                | ((src[3] & 268435455) >> 24)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 268435455) >> 16)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 268435455) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 268435455))) & 255);
                            dest[14] = 
                (byte)((((src[4] & 268435455) >> 20)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 268435455) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 268435455) >> 4)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 268435455) << 4)                | ((src[5] & 268435455) >> 24)) & 255);
                            dest[18] = 
                (byte)((((src[5] & 268435455) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 268435455) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 268435455))) & 255);
                            dest[21] = 
                (byte)((((src[6] & 268435455) >> 20)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 268435455) >> 12)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 268435455) >> 4)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 268435455) << 4)                | ((src[7] & 268435455) >> 24)) & 255);
                            dest[25] = 
                (byte)((((src[7] & 268435455) >> 16)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 268435455) >> 8)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 268435455))) & 255);
                        }
        private static void Unpack8LongValuesLE29(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 536870911);            dest[1] = ((((long)src[3]) >> 5) & 7) | ((((long)src[4]) << 3) & 2047) | ((((long)src[5]) << 11) & 524287) | ((((long)src[6]) << 19) & 134217727) | ((((long)src[7]) << 27) & 536870911);            dest[2] = ((((long)src[7]) >> 2) & 63) | ((((long)src[8]) << 6) & 16383) | ((((long)src[9]) << 14) & 4194303) | ((((long)src[10]) << 22) & 536870911);            dest[3] = ((((long)src[10]) >> 7) & 1) | ((((long)src[11]) << 1) & 511) | ((((long)src[12]) << 9) & 131071) | ((((long)src[13]) << 17) & 33554431) | ((((long)src[14]) << 25) & 536870911);            dest[4] = ((((long)src[14]) >> 4) & 15) | ((((long)src[15]) << 4) & 4095) | ((((long)src[16]) << 12) & 1048575) | ((((long)src[17]) << 20) & 268435455) | ((((long)src[18]) << 28) & 536870911);            dest[5] = ((((long)src[18]) >> 1) & 127) | ((((long)src[19]) << 7) & 32767) | ((((long)src[20]) << 15) & 8388607) | ((((long)src[21]) << 23) & 536870911);            dest[6] = ((((long)src[21]) >> 6) & 3) | ((((long)src[22]) << 2) & 1023) | ((((long)src[23]) << 10) & 262143) | ((((long)src[24]) << 18) & 67108863) | ((((long)src[25]) << 26) & 536870911);            dest[7] = ((((long)src[25]) >> 3) & 31) | ((((long)src[26]) << 5) & 8191) | ((((long)src[27]) << 13) & 2097151) | ((((long)src[28]) << 21) & 536870911);        }

        private static void Pack8LongValuesLE29(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 536870911))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 536870911) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 536870911) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 536870911) >> 24)                | ((src[1] & 536870911) << 5)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 536870911) >> 3)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 536870911) >> 11)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 536870911) >> 19)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 536870911) >> 27)                | ((src[2] & 536870911) << 2)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 536870911) >> 6)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 536870911) >> 14)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 536870911) >> 22)                | ((src[3] & 536870911) << 7)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 536870911) >> 1)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 536870911) >> 9)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 536870911) >> 17)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 536870911) >> 25)                | ((src[4] & 536870911) << 4)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 536870911) >> 4)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 536870911) >> 12)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 536870911) >> 20)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 536870911) >> 28)                | ((src[5] & 536870911) << 1)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 536870911) >> 7)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 536870911) >> 15)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 536870911) >> 23)                | ((src[6] & 536870911) << 6)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 536870911) >> 2)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 536870911) >> 10)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 536870911) >> 18)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 536870911) >> 26)                | ((src[7] & 536870911) << 3)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 536870911) >> 5)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 536870911) >> 13)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 536870911) >> 21)) & 255);
                        }
        private static void Unpack8LongValuesBE29(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 21) & 536870911) | ((((long)src[1]) << 13) & 2097151) | ((((long)src[2]) << 5) & 8191) | ((((long)src[3]) >> 3) & 31);            dest[1] = ((((long)src[3]) << 26) & 536870911) | ((((long)src[4]) << 18) & 67108863) | ((((long)src[5]) << 10) & 262143) | ((((long)src[6]) << 2) & 1023) | ((((long)src[7]) >> 6) & 3);            dest[2] = ((((long)src[7]) << 23) & 536870911) | ((((long)src[8]) << 15) & 8388607) | ((((long)src[9]) << 7) & 32767) | ((((long)src[10]) >> 1) & 127);            dest[3] = ((((long)src[10]) << 28) & 536870911) | ((((long)src[11]) << 20) & 268435455) | ((((long)src[12]) << 12) & 1048575) | ((((long)src[13]) << 4) & 4095) | ((((long)src[14]) >> 4) & 15);            dest[4] = ((((long)src[14]) << 25) & 536870911) | ((((long)src[15]) << 17) & 33554431) | ((((long)src[16]) << 9) & 131071) | ((((long)src[17]) << 1) & 511) | ((((long)src[18]) >> 7) & 1);            dest[5] = ((((long)src[18]) << 22) & 536870911) | ((((long)src[19]) << 14) & 4194303) | ((((long)src[20]) << 6) & 16383) | ((((long)src[21]) >> 2) & 63);            dest[6] = ((((long)src[21]) << 27) & 536870911) | ((((long)src[22]) << 19) & 134217727) | ((((long)src[23]) << 11) & 524287) | ((((long)src[24]) << 3) & 2047) | ((((long)src[25]) >> 5) & 7);            dest[7] = ((((long)src[25]) << 24) & 536870911) | ((((long)src[26]) << 16) & 16777215) | ((((long)src[27]) << 8) & 65535) | ((((long)src[28])) & 255);        }

        private static void Pack8LongValuesBE29(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 536870911) >> 21)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 536870911) >> 13)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 536870911) >> 5)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 536870911) << 3)                | ((src[1] & 536870911) >> 26)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 536870911) >> 18)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 536870911) >> 10)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 536870911) >> 2)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 536870911) << 6)                | ((src[2] & 536870911) >> 23)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 536870911) >> 15)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 536870911) >> 7)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 536870911) << 1)                | ((src[3] & 536870911) >> 28)) & 255);
                            dest[11] = 
                (byte)((((src[3] & 536870911) >> 20)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 536870911) >> 12)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 536870911) >> 4)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 536870911) << 4)                | ((src[4] & 536870911) >> 25)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 536870911) >> 17)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 536870911) >> 9)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 536870911) >> 1)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 536870911) << 7)                | ((src[5] & 536870911) >> 22)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 536870911) >> 14)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 536870911) >> 6)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 536870911) << 2)                | ((src[6] & 536870911) >> 27)) & 255);
                            dest[22] = 
                (byte)((((src[6] & 536870911) >> 19)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 536870911) >> 11)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 536870911) >> 3)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 536870911) << 5)                | ((src[7] & 536870911) >> 24)) & 255);
                            dest[26] = 
                (byte)((((src[7] & 536870911) >> 16)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 536870911) >> 8)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 536870911))) & 255);
                        }
        private static void Unpack8LongValuesLE30(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 1073741823);            dest[1] = ((((long)src[3]) >> 6) & 3) | ((((long)src[4]) << 2) & 1023) | ((((long)src[5]) << 10) & 262143) | ((((long)src[6]) << 18) & 67108863) | ((((long)src[7]) << 26) & 1073741823);            dest[2] = ((((long)src[7]) >> 4) & 15) | ((((long)src[8]) << 4) & 4095) | ((((long)src[9]) << 12) & 1048575) | ((((long)src[10]) << 20) & 268435455) | ((((long)src[11]) << 28) & 1073741823);            dest[3] = ((((long)src[11]) >> 2) & 63) | ((((long)src[12]) << 6) & 16383) | ((((long)src[13]) << 14) & 4194303) | ((((long)src[14]) << 22) & 1073741823);            dest[4] = ((((long)src[15])) & 255) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17]) << 16) & 16777215) | ((((long)src[18]) << 24) & 1073741823);            dest[5] = ((((long)src[18]) >> 6) & 3) | ((((long)src[19]) << 2) & 1023) | ((((long)src[20]) << 10) & 262143) | ((((long)src[21]) << 18) & 67108863) | ((((long)src[22]) << 26) & 1073741823);            dest[6] = ((((long)src[22]) >> 4) & 15) | ((((long)src[23]) << 4) & 4095) | ((((long)src[24]) << 12) & 1048575) | ((((long)src[25]) << 20) & 268435455) | ((((long)src[26]) << 28) & 1073741823);            dest[7] = ((((long)src[26]) >> 2) & 63) | ((((long)src[27]) << 6) & 16383) | ((((long)src[28]) << 14) & 4194303) | ((((long)src[29]) << 22) & 1073741823);        }

        private static void Pack8LongValuesLE30(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1073741823))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1073741823) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1073741823) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1073741823) >> 24)                | ((src[1] & 1073741823) << 6)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 1073741823) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 1073741823) >> 10)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 1073741823) >> 18)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 1073741823) >> 26)                | ((src[2] & 1073741823) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 1073741823) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 1073741823) >> 12)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 1073741823) >> 20)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 1073741823) >> 28)                | ((src[3] & 1073741823) << 2)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 1073741823) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 1073741823) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 1073741823) >> 22)) & 255);
                            dest[15] = 
                (byte)((((src[4] & 1073741823))) & 255);
                            dest[16] = 
                (byte)((((src[4] & 1073741823) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 1073741823) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 1073741823) >> 24)                | ((src[5] & 1073741823) << 6)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 1073741823) >> 2)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 1073741823) >> 10)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 1073741823) >> 18)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 1073741823) >> 26)                | ((src[6] & 1073741823) << 4)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 1073741823) >> 4)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 1073741823) >> 12)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 1073741823) >> 20)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 1073741823) >> 28)                | ((src[7] & 1073741823) << 2)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 1073741823) >> 6)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 1073741823) >> 14)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 1073741823) >> 22)) & 255);
                        }
        private static void Unpack8LongValuesBE30(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 22) & 1073741823) | ((((long)src[1]) << 14) & 4194303) | ((((long)src[2]) << 6) & 16383) | ((((long)src[3]) >> 2) & 63);            dest[1] = ((((long)src[3]) << 28) & 1073741823) | ((((long)src[4]) << 20) & 268435455) | ((((long)src[5]) << 12) & 1048575) | ((((long)src[6]) << 4) & 4095) | ((((long)src[7]) >> 4) & 15);            dest[2] = ((((long)src[7]) << 26) & 1073741823) | ((((long)src[8]) << 18) & 67108863) | ((((long)src[9]) << 10) & 262143) | ((((long)src[10]) << 2) & 1023) | ((((long)src[11]) >> 6) & 3);            dest[3] = ((((long)src[11]) << 24) & 1073741823) | ((((long)src[12]) << 16) & 16777215) | ((((long)src[13]) << 8) & 65535) | ((((long)src[14])) & 255);            dest[4] = ((((long)src[15]) << 22) & 1073741823) | ((((long)src[16]) << 14) & 4194303) | ((((long)src[17]) << 6) & 16383) | ((((long)src[18]) >> 2) & 63);            dest[5] = ((((long)src[18]) << 28) & 1073741823) | ((((long)src[19]) << 20) & 268435455) | ((((long)src[20]) << 12) & 1048575) | ((((long)src[21]) << 4) & 4095) | ((((long)src[22]) >> 4) & 15);            dest[6] = ((((long)src[22]) << 26) & 1073741823) | ((((long)src[23]) << 18) & 67108863) | ((((long)src[24]) << 10) & 262143) | ((((long)src[25]) << 2) & 1023) | ((((long)src[26]) >> 6) & 3);            dest[7] = ((((long)src[26]) << 24) & 1073741823) | ((((long)src[27]) << 16) & 16777215) | ((((long)src[28]) << 8) & 65535) | ((((long)src[29])) & 255);        }

        private static void Pack8LongValuesBE30(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1073741823) >> 22)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1073741823) >> 14)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1073741823) >> 6)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1073741823) << 2)                | ((src[1] & 1073741823) >> 28)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 1073741823) >> 20)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 1073741823) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 1073741823) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 1073741823) << 4)                | ((src[2] & 1073741823) >> 26)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 1073741823) >> 18)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 1073741823) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 1073741823) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 1073741823) << 6)                | ((src[3] & 1073741823) >> 24)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 1073741823) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 1073741823) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 1073741823))) & 255);
                            dest[15] = 
                (byte)((((src[4] & 1073741823) >> 22)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 1073741823) >> 14)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 1073741823) >> 6)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 1073741823) << 2)                | ((src[5] & 1073741823) >> 28)) & 255);
                            dest[19] = 
                (byte)((((src[5] & 1073741823) >> 20)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 1073741823) >> 12)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 1073741823) >> 4)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 1073741823) << 4)                | ((src[6] & 1073741823) >> 26)) & 255);
                            dest[23] = 
                (byte)((((src[6] & 1073741823) >> 18)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 1073741823) >> 10)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 1073741823) >> 2)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 1073741823) << 6)                | ((src[7] & 1073741823) >> 24)) & 255);
                            dest[27] = 
                (byte)((((src[7] & 1073741823) >> 16)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 1073741823) >> 8)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 1073741823))) & 255);
                        }
        private static void Unpack8LongValuesLE31(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 2147483647);            dest[1] = ((((long)src[3]) >> 7) & 1) | ((((long)src[4]) << 1) & 511) | ((((long)src[5]) << 9) & 131071) | ((((long)src[6]) << 17) & 33554431) | ((((long)src[7]) << 25) & 2147483647);            dest[2] = ((((long)src[7]) >> 6) & 3) | ((((long)src[8]) << 2) & 1023) | ((((long)src[9]) << 10) & 262143) | ((((long)src[10]) << 18) & 67108863) | ((((long)src[11]) << 26) & 2147483647);            dest[3] = ((((long)src[11]) >> 5) & 7) | ((((long)src[12]) << 3) & 2047) | ((((long)src[13]) << 11) & 524287) | ((((long)src[14]) << 19) & 134217727) | ((((long)src[15]) << 27) & 2147483647);            dest[4] = ((((long)src[15]) >> 4) & 15) | ((((long)src[16]) << 4) & 4095) | ((((long)src[17]) << 12) & 1048575) | ((((long)src[18]) << 20) & 268435455) | ((((long)src[19]) << 28) & 2147483647);            dest[5] = ((((long)src[19]) >> 3) & 31) | ((((long)src[20]) << 5) & 8191) | ((((long)src[21]) << 13) & 2097151) | ((((long)src[22]) << 21) & 536870911) | ((((long)src[23]) << 29) & 2147483647);            dest[6] = ((((long)src[23]) >> 2) & 63) | ((((long)src[24]) << 6) & 16383) | ((((long)src[25]) << 14) & 4194303) | ((((long)src[26]) << 22) & 1073741823) | ((((long)src[27]) << 30) & 2147483647);            dest[7] = ((((long)src[27]) >> 1) & 127) | ((((long)src[28]) << 7) & 32767) | ((((long)src[29]) << 15) & 8388607) | ((((long)src[30]) << 23) & 2147483647);        }

        private static void Pack8LongValuesLE31(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2147483647))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2147483647) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2147483647) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2147483647) >> 24)                | ((src[1] & 2147483647) << 7)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 2147483647) >> 1)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 2147483647) >> 9)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 2147483647) >> 17)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 2147483647) >> 25)                | ((src[2] & 2147483647) << 6)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 2147483647) >> 2)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 2147483647) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 2147483647) >> 18)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 2147483647) >> 26)                | ((src[3] & 2147483647) << 5)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 2147483647) >> 3)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 2147483647) >> 11)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 2147483647) >> 19)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 2147483647) >> 27)                | ((src[4] & 2147483647) << 4)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 2147483647) >> 4)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 2147483647) >> 12)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 2147483647) >> 20)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 2147483647) >> 28)                | ((src[5] & 2147483647) << 3)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 2147483647) >> 5)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 2147483647) >> 13)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 2147483647) >> 21)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 2147483647) >> 29)                | ((src[6] & 2147483647) << 2)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 2147483647) >> 6)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 2147483647) >> 14)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 2147483647) >> 22)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 2147483647) >> 30)                | ((src[7] & 2147483647) << 1)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 2147483647) >> 7)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 2147483647) >> 15)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 2147483647) >> 23)) & 255);
                        }
        private static void Unpack8LongValuesBE31(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 23) & 2147483647) | ((((long)src[1]) << 15) & 8388607) | ((((long)src[2]) << 7) & 32767) | ((((long)src[3]) >> 1) & 127);            dest[1] = ((((long)src[3]) << 30) & 2147483647) | ((((long)src[4]) << 22) & 1073741823) | ((((long)src[5]) << 14) & 4194303) | ((((long)src[6]) << 6) & 16383) | ((((long)src[7]) >> 2) & 63);            dest[2] = ((((long)src[7]) << 29) & 2147483647) | ((((long)src[8]) << 21) & 536870911) | ((((long)src[9]) << 13) & 2097151) | ((((long)src[10]) << 5) & 8191) | ((((long)src[11]) >> 3) & 31);            dest[3] = ((((long)src[11]) << 28) & 2147483647) | ((((long)src[12]) << 20) & 268435455) | ((((long)src[13]) << 12) & 1048575) | ((((long)src[14]) << 4) & 4095) | ((((long)src[15]) >> 4) & 15);            dest[4] = ((((long)src[15]) << 27) & 2147483647) | ((((long)src[16]) << 19) & 134217727) | ((((long)src[17]) << 11) & 524287) | ((((long)src[18]) << 3) & 2047) | ((((long)src[19]) >> 5) & 7);            dest[5] = ((((long)src[19]) << 26) & 2147483647) | ((((long)src[20]) << 18) & 67108863) | ((((long)src[21]) << 10) & 262143) | ((((long)src[22]) << 2) & 1023) | ((((long)src[23]) >> 6) & 3);            dest[6] = ((((long)src[23]) << 25) & 2147483647) | ((((long)src[24]) << 17) & 33554431) | ((((long)src[25]) << 9) & 131071) | ((((long)src[26]) << 1) & 511) | ((((long)src[27]) >> 7) & 1);            dest[7] = ((((long)src[27]) << 24) & 2147483647) | ((((long)src[28]) << 16) & 16777215) | ((((long)src[29]) << 8) & 65535) | ((((long)src[30])) & 255);        }

        private static void Pack8LongValuesBE31(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2147483647) >> 23)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2147483647) >> 15)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2147483647) >> 7)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2147483647) << 1)                | ((src[1] & 2147483647) >> 30)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 2147483647) >> 22)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 2147483647) >> 14)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 2147483647) >> 6)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 2147483647) << 2)                | ((src[2] & 2147483647) >> 29)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 2147483647) >> 21)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 2147483647) >> 13)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 2147483647) >> 5)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 2147483647) << 3)                | ((src[3] & 2147483647) >> 28)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 2147483647) >> 20)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 2147483647) >> 12)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 2147483647) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 2147483647) << 4)                | ((src[4] & 2147483647) >> 27)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 2147483647) >> 19)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 2147483647) >> 11)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 2147483647) >> 3)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 2147483647) << 5)                | ((src[5] & 2147483647) >> 26)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 2147483647) >> 18)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 2147483647) >> 10)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 2147483647) >> 2)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 2147483647) << 6)                | ((src[6] & 2147483647) >> 25)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 2147483647) >> 17)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 2147483647) >> 9)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 2147483647) >> 1)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 2147483647) << 7)                | ((src[7] & 2147483647) >> 24)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 2147483647) >> 16)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 2147483647) >> 8)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 2147483647))) & 255);
                        }
        private static void Unpack8LongValuesLE32(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295);            dest[1] = ((((long)src[4])) & 255) | ((((long)src[5]) << 8) & 65535) | ((((long)src[6]) << 16) & 16777215) | ((((long)src[7]) << 24) & 4294967295);            dest[2] = ((((long)src[8])) & 255) | ((((long)src[9]) << 8) & 65535) | ((((long)src[10]) << 16) & 16777215) | ((((long)src[11]) << 24) & 4294967295);            dest[3] = ((((long)src[12])) & 255) | ((((long)src[13]) << 8) & 65535) | ((((long)src[14]) << 16) & 16777215) | ((((long)src[15]) << 24) & 4294967295);            dest[4] = ((((long)src[16])) & 255) | ((((long)src[17]) << 8) & 65535) | ((((long)src[18]) << 16) & 16777215) | ((((long)src[19]) << 24) & 4294967295);            dest[5] = ((((long)src[20])) & 255) | ((((long)src[21]) << 8) & 65535) | ((((long)src[22]) << 16) & 16777215) | ((((long)src[23]) << 24) & 4294967295);            dest[6] = ((((long)src[24])) & 255) | ((((long)src[25]) << 8) & 65535) | ((((long)src[26]) << 16) & 16777215) | ((((long)src[27]) << 24) & 4294967295);            dest[7] = ((((long)src[28])) & 255) | ((((long)src[29]) << 8) & 65535) | ((((long)src[30]) << 16) & 16777215) | ((((long)src[31]) << 24) & 4294967295);        }

        private static void Pack8LongValuesLE32(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4294967295))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4294967295) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4294967295) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4294967295) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[1] & 4294967295))) & 255);
                            dest[5] = 
                (byte)((((src[1] & 4294967295) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 4294967295) >> 16)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 4294967295) >> 24)) & 255);
                            dest[8] = 
                (byte)((((src[2] & 4294967295))) & 255);
                            dest[9] = 
                (byte)((((src[2] & 4294967295) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 4294967295) >> 16)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 4294967295) >> 24)) & 255);
                            dest[12] = 
                (byte)((((src[3] & 4294967295))) & 255);
                            dest[13] = 
                (byte)((((src[3] & 4294967295) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 4294967295) >> 16)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 4294967295) >> 24)) & 255);
                            dest[16] = 
                (byte)((((src[4] & 4294967295))) & 255);
                            dest[17] = 
                (byte)((((src[4] & 4294967295) >> 8)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 4294967295) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 4294967295) >> 24)) & 255);
                            dest[20] = 
                (byte)((((src[5] & 4294967295))) & 255);
                            dest[21] = 
                (byte)((((src[5] & 4294967295) >> 8)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 4294967295) >> 16)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 4294967295) >> 24)) & 255);
                            dest[24] = 
                (byte)((((src[6] & 4294967295))) & 255);
                            dest[25] = 
                (byte)((((src[6] & 4294967295) >> 8)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 4294967295) >> 16)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 4294967295) >> 24)) & 255);
                            dest[28] = 
                (byte)((((src[7] & 4294967295))) & 255);
                            dest[29] = 
                (byte)((((src[7] & 4294967295) >> 8)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 4294967295) >> 16)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 4294967295) >> 24)) & 255);
                        }
        private static void Unpack8LongValuesBE32(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 24) & 4294967295) | ((((long)src[1]) << 16) & 16777215) | ((((long)src[2]) << 8) & 65535) | ((((long)src[3])) & 255);            dest[1] = ((((long)src[4]) << 24) & 4294967295) | ((((long)src[5]) << 16) & 16777215) | ((((long)src[6]) << 8) & 65535) | ((((long)src[7])) & 255);            dest[2] = ((((long)src[8]) << 24) & 4294967295) | ((((long)src[9]) << 16) & 16777215) | ((((long)src[10]) << 8) & 65535) | ((((long)src[11])) & 255);            dest[3] = ((((long)src[12]) << 24) & 4294967295) | ((((long)src[13]) << 16) & 16777215) | ((((long)src[14]) << 8) & 65535) | ((((long)src[15])) & 255);            dest[4] = ((((long)src[16]) << 24) & 4294967295) | ((((long)src[17]) << 16) & 16777215) | ((((long)src[18]) << 8) & 65535) | ((((long)src[19])) & 255);            dest[5] = ((((long)src[20]) << 24) & 4294967295) | ((((long)src[21]) << 16) & 16777215) | ((((long)src[22]) << 8) & 65535) | ((((long)src[23])) & 255);            dest[6] = ((((long)src[24]) << 24) & 4294967295) | ((((long)src[25]) << 16) & 16777215) | ((((long)src[26]) << 8) & 65535) | ((((long)src[27])) & 255);            dest[7] = ((((long)src[28]) << 24) & 4294967295) | ((((long)src[29]) << 16) & 16777215) | ((((long)src[30]) << 8) & 65535) | ((((long)src[31])) & 255);        }

        private static void Pack8LongValuesBE32(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4294967295) >> 24)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4294967295) >> 16)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4294967295) >> 8)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4294967295))) & 255);
                            dest[4] = 
                (byte)((((src[1] & 4294967295) >> 24)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 4294967295) >> 16)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 4294967295) >> 8)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 4294967295))) & 255);
                            dest[8] = 
                (byte)((((src[2] & 4294967295) >> 24)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 4294967295) >> 16)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 4294967295) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 4294967295))) & 255);
                            dest[12] = 
                (byte)((((src[3] & 4294967295) >> 24)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 4294967295) >> 16)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 4294967295) >> 8)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 4294967295))) & 255);
                            dest[16] = 
                (byte)((((src[4] & 4294967295) >> 24)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 4294967295) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 4294967295) >> 8)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 4294967295))) & 255);
                            dest[20] = 
                (byte)((((src[5] & 4294967295) >> 24)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 4294967295) >> 16)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 4294967295) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 4294967295))) & 255);
                            dest[24] = 
                (byte)((((src[6] & 4294967295) >> 24)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 4294967295) >> 16)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 4294967295) >> 8)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 4294967295))) & 255);
                            dest[28] = 
                (byte)((((src[7] & 4294967295) >> 24)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 4294967295) >> 16)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 4294967295) >> 8)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 4294967295))) & 255);
                        }
        private static void Unpack8LongValuesLE33(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 8589934591);            dest[1] = ((((long)src[4]) >> 1) & 127) | ((((long)src[5]) << 7) & 32767) | ((((long)src[6]) << 15) & 8388607) | ((((long)src[7]) << 23) & 2147483647) | ((((long)src[8]) << 31) & 8589934591);            dest[2] = ((((long)src[8]) >> 2) & 63) | ((((long)src[9]) << 6) & 16383) | ((((long)src[10]) << 14) & 4194303) | ((((long)src[11]) << 22) & 1073741823) | ((((long)src[12]) << 30) & 8589934591);            dest[3] = ((((long)src[12]) >> 3) & 31) | ((((long)src[13]) << 5) & 8191) | ((((long)src[14]) << 13) & 2097151) | ((((long)src[15]) << 21) & 536870911) | ((((long)src[16]) << 29) & 8589934591);            dest[4] = ((((long)src[16]) >> 4) & 15) | ((((long)src[17]) << 4) & 4095) | ((((long)src[18]) << 12) & 1048575) | ((((long)src[19]) << 20) & 268435455) | ((((long)src[20]) << 28) & 8589934591);            dest[5] = ((((long)src[20]) >> 5) & 7) | ((((long)src[21]) << 3) & 2047) | ((((long)src[22]) << 11) & 524287) | ((((long)src[23]) << 19) & 134217727) | ((((long)src[24]) << 27) & 8589934591);            dest[6] = ((((long)src[24]) >> 6) & 3) | ((((long)src[25]) << 2) & 1023) | ((((long)src[26]) << 10) & 262143) | ((((long)src[27]) << 18) & 67108863) | ((((long)src[28]) << 26) & 8589934591);            dest[7] = ((((long)src[28]) >> 7) & 1) | ((((long)src[29]) << 1) & 511) | ((((long)src[30]) << 9) & 131071) | ((((long)src[31]) << 17) & 33554431) | ((((long)src[32]) << 25) & 8589934591);        }

        private static void Pack8LongValuesLE33(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8589934591))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8589934591) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 8589934591) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 8589934591) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 8589934591) >> 32)                | ((src[1] & 8589934591) << 1)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 8589934591) >> 7)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 8589934591) >> 15)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 8589934591) >> 23)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 8589934591) >> 31)                | ((src[2] & 8589934591) << 2)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 8589934591) >> 6)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 8589934591) >> 14)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 8589934591) >> 22)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 8589934591) >> 30)                | ((src[3] & 8589934591) << 3)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 8589934591) >> 5)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 8589934591) >> 13)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 8589934591) >> 21)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 8589934591) >> 29)                | ((src[4] & 8589934591) << 4)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 8589934591) >> 4)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 8589934591) >> 12)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 8589934591) >> 20)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 8589934591) >> 28)                | ((src[5] & 8589934591) << 5)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 8589934591) >> 3)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 8589934591) >> 11)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 8589934591) >> 19)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 8589934591) >> 27)                | ((src[6] & 8589934591) << 6)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 8589934591) >> 2)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 8589934591) >> 10)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 8589934591) >> 18)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 8589934591) >> 26)                | ((src[7] & 8589934591) << 7)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 8589934591) >> 1)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 8589934591) >> 9)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 8589934591) >> 17)) & 255);
                            dest[32] = 
                (byte)((((src[7] & 8589934591) >> 25)) & 255);
                        }
        private static void Unpack8LongValuesBE33(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 25) & 8589934591) | ((((long)src[1]) << 17) & 33554431) | ((((long)src[2]) << 9) & 131071) | ((((long)src[3]) << 1) & 511) | ((((long)src[4]) >> 7) & 1);            dest[1] = ((((long)src[4]) << 26) & 8589934591) | ((((long)src[5]) << 18) & 67108863) | ((((long)src[6]) << 10) & 262143) | ((((long)src[7]) << 2) & 1023) | ((((long)src[8]) >> 6) & 3);            dest[2] = ((((long)src[8]) << 27) & 8589934591) | ((((long)src[9]) << 19) & 134217727) | ((((long)src[10]) << 11) & 524287) | ((((long)src[11]) << 3) & 2047) | ((((long)src[12]) >> 5) & 7);            dest[3] = ((((long)src[12]) << 28) & 8589934591) | ((((long)src[13]) << 20) & 268435455) | ((((long)src[14]) << 12) & 1048575) | ((((long)src[15]) << 4) & 4095) | ((((long)src[16]) >> 4) & 15);            dest[4] = ((((long)src[16]) << 29) & 8589934591) | ((((long)src[17]) << 21) & 536870911) | ((((long)src[18]) << 13) & 2097151) | ((((long)src[19]) << 5) & 8191) | ((((long)src[20]) >> 3) & 31);            dest[5] = ((((long)src[20]) << 30) & 8589934591) | ((((long)src[21]) << 22) & 1073741823) | ((((long)src[22]) << 14) & 4194303) | ((((long)src[23]) << 6) & 16383) | ((((long)src[24]) >> 2) & 63);            dest[6] = ((((long)src[24]) << 31) & 8589934591) | ((((long)src[25]) << 23) & 2147483647) | ((((long)src[26]) << 15) & 8388607) | ((((long)src[27]) << 7) & 32767) | ((((long)src[28]) >> 1) & 127);            dest[7] = ((((long)src[28]) << 32) & 8589934591) | ((((long)src[29]) << 24) & 4294967295) | ((((long)src[30]) << 16) & 16777215) | ((((long)src[31]) << 8) & 65535) | ((((long)src[32])) & 255);        }

        private static void Pack8LongValuesBE33(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8589934591) >> 25)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8589934591) >> 17)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 8589934591) >> 9)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 8589934591) >> 1)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 8589934591) << 7)                | ((src[1] & 8589934591) >> 26)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 8589934591) >> 18)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 8589934591) >> 10)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 8589934591) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 8589934591) << 6)                | ((src[2] & 8589934591) >> 27)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 8589934591) >> 19)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 8589934591) >> 11)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 8589934591) >> 3)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 8589934591) << 5)                | ((src[3] & 8589934591) >> 28)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 8589934591) >> 20)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 8589934591) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 8589934591) >> 4)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 8589934591) << 4)                | ((src[4] & 8589934591) >> 29)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 8589934591) >> 21)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 8589934591) >> 13)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 8589934591) >> 5)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 8589934591) << 3)                | ((src[5] & 8589934591) >> 30)) & 255);
                            dest[21] = 
                (byte)((((src[5] & 8589934591) >> 22)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 8589934591) >> 14)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 8589934591) >> 6)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 8589934591) << 2)                | ((src[6] & 8589934591) >> 31)) & 255);
                            dest[25] = 
                (byte)((((src[6] & 8589934591) >> 23)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 8589934591) >> 15)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 8589934591) >> 7)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 8589934591) << 1)                | ((src[7] & 8589934591) >> 32)) & 255);
                            dest[29] = 
                (byte)((((src[7] & 8589934591) >> 24)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 8589934591) >> 16)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 8589934591) >> 8)) & 255);
                            dest[32] = 
                (byte)((((src[7] & 8589934591))) & 255);
                        }
        private static void Unpack8LongValuesLE34(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 17179869183);            dest[1] = ((((long)src[4]) >> 2) & 63) | ((((long)src[5]) << 6) & 16383) | ((((long)src[6]) << 14) & 4194303) | ((((long)src[7]) << 22) & 1073741823) | ((((long)src[8]) << 30) & 17179869183);            dest[2] = ((((long)src[8]) >> 4) & 15) | ((((long)src[9]) << 4) & 4095) | ((((long)src[10]) << 12) & 1048575) | ((((long)src[11]) << 20) & 268435455) | ((((long)src[12]) << 28) & 17179869183);            dest[3] = ((((long)src[12]) >> 6) & 3) | ((((long)src[13]) << 2) & 1023) | ((((long)src[14]) << 10) & 262143) | ((((long)src[15]) << 18) & 67108863) | ((((long)src[16]) << 26) & 17179869183);            dest[4] = ((((long)src[17])) & 255) | ((((long)src[18]) << 8) & 65535) | ((((long)src[19]) << 16) & 16777215) | ((((long)src[20]) << 24) & 4294967295) | ((((long)src[21]) << 32) & 17179869183);            dest[5] = ((((long)src[21]) >> 2) & 63) | ((((long)src[22]) << 6) & 16383) | ((((long)src[23]) << 14) & 4194303) | ((((long)src[24]) << 22) & 1073741823) | ((((long)src[25]) << 30) & 17179869183);            dest[6] = ((((long)src[25]) >> 4) & 15) | ((((long)src[26]) << 4) & 4095) | ((((long)src[27]) << 12) & 1048575) | ((((long)src[28]) << 20) & 268435455) | ((((long)src[29]) << 28) & 17179869183);            dest[7] = ((((long)src[29]) >> 6) & 3) | ((((long)src[30]) << 2) & 1023) | ((((long)src[31]) << 10) & 262143) | ((((long)src[32]) << 18) & 67108863) | ((((long)src[33]) << 26) & 17179869183);        }

        private static void Pack8LongValuesLE34(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 17179869183))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 17179869183) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 17179869183) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 17179869183) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 17179869183) >> 32)                | ((src[1] & 17179869183) << 2)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 17179869183) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 17179869183) >> 14)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 17179869183) >> 22)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 17179869183) >> 30)                | ((src[2] & 17179869183) << 4)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 17179869183) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 17179869183) >> 12)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 17179869183) >> 20)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 17179869183) >> 28)                | ((src[3] & 17179869183) << 6)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 17179869183) >> 2)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 17179869183) >> 10)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 17179869183) >> 18)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 17179869183) >> 26)) & 255);
                            dest[17] = 
                (byte)((((src[4] & 17179869183))) & 255);
                            dest[18] = 
                (byte)((((src[4] & 17179869183) >> 8)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 17179869183) >> 16)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 17179869183) >> 24)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 17179869183) >> 32)                | ((src[5] & 17179869183) << 2)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 17179869183) >> 6)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 17179869183) >> 14)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 17179869183) >> 22)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 17179869183) >> 30)                | ((src[6] & 17179869183) << 4)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 17179869183) >> 4)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 17179869183) >> 12)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 17179869183) >> 20)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 17179869183) >> 28)                | ((src[7] & 17179869183) << 6)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 17179869183) >> 2)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 17179869183) >> 10)) & 255);
                            dest[32] = 
                (byte)((((src[7] & 17179869183) >> 18)) & 255);
                            dest[33] = 
                (byte)((((src[7] & 17179869183) >> 26)) & 255);
                        }
        private static void Unpack8LongValuesBE34(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 26) & 17179869183) | ((((long)src[1]) << 18) & 67108863) | ((((long)src[2]) << 10) & 262143) | ((((long)src[3]) << 2) & 1023) | ((((long)src[4]) >> 6) & 3);            dest[1] = ((((long)src[4]) << 28) & 17179869183) | ((((long)src[5]) << 20) & 268435455) | ((((long)src[6]) << 12) & 1048575) | ((((long)src[7]) << 4) & 4095) | ((((long)src[8]) >> 4) & 15);            dest[2] = ((((long)src[8]) << 30) & 17179869183) | ((((long)src[9]) << 22) & 1073741823) | ((((long)src[10]) << 14) & 4194303) | ((((long)src[11]) << 6) & 16383) | ((((long)src[12]) >> 2) & 63);            dest[3] = ((((long)src[12]) << 32) & 17179869183) | ((((long)src[13]) << 24) & 4294967295) | ((((long)src[14]) << 16) & 16777215) | ((((long)src[15]) << 8) & 65535) | ((((long)src[16])) & 255);            dest[4] = ((((long)src[17]) << 26) & 17179869183) | ((((long)src[18]) << 18) & 67108863) | ((((long)src[19]) << 10) & 262143) | ((((long)src[20]) << 2) & 1023) | ((((long)src[21]) >> 6) & 3);            dest[5] = ((((long)src[21]) << 28) & 17179869183) | ((((long)src[22]) << 20) & 268435455) | ((((long)src[23]) << 12) & 1048575) | ((((long)src[24]) << 4) & 4095) | ((((long)src[25]) >> 4) & 15);            dest[6] = ((((long)src[25]) << 30) & 17179869183) | ((((long)src[26]) << 22) & 1073741823) | ((((long)src[27]) << 14) & 4194303) | ((((long)src[28]) << 6) & 16383) | ((((long)src[29]) >> 2) & 63);            dest[7] = ((((long)src[29]) << 32) & 17179869183) | ((((long)src[30]) << 24) & 4294967295) | ((((long)src[31]) << 16) & 16777215) | ((((long)src[32]) << 8) & 65535) | ((((long)src[33])) & 255);        }

        private static void Pack8LongValuesBE34(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 17179869183) >> 26)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 17179869183) >> 18)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 17179869183) >> 10)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 17179869183) >> 2)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 17179869183) << 6)                | ((src[1] & 17179869183) >> 28)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 17179869183) >> 20)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 17179869183) >> 12)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 17179869183) >> 4)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 17179869183) << 4)                | ((src[2] & 17179869183) >> 30)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 17179869183) >> 22)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 17179869183) >> 14)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 17179869183) >> 6)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 17179869183) << 2)                | ((src[3] & 17179869183) >> 32)) & 255);
                            dest[13] = 
                (byte)((((src[3] & 17179869183) >> 24)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 17179869183) >> 16)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 17179869183) >> 8)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 17179869183))) & 255);
                            dest[17] = 
                (byte)((((src[4] & 17179869183) >> 26)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 17179869183) >> 18)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 17179869183) >> 10)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 17179869183) >> 2)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 17179869183) << 6)                | ((src[5] & 17179869183) >> 28)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 17179869183) >> 20)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 17179869183) >> 12)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 17179869183) >> 4)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 17179869183) << 4)                | ((src[6] & 17179869183) >> 30)) & 255);
                            dest[26] = 
                (byte)((((src[6] & 17179869183) >> 22)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 17179869183) >> 14)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 17179869183) >> 6)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 17179869183) << 2)                | ((src[7] & 17179869183) >> 32)) & 255);
                            dest[30] = 
                (byte)((((src[7] & 17179869183) >> 24)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 17179869183) >> 16)) & 255);
                            dest[32] = 
                (byte)((((src[7] & 17179869183) >> 8)) & 255);
                            dest[33] = 
                (byte)((((src[7] & 17179869183))) & 255);
                        }
        private static void Unpack8LongValuesLE35(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 34359738367);            dest[1] = ((((long)src[4]) >> 3) & 31) | ((((long)src[5]) << 5) & 8191) | ((((long)src[6]) << 13) & 2097151) | ((((long)src[7]) << 21) & 536870911) | ((((long)src[8]) << 29) & 34359738367);            dest[2] = ((((long)src[8]) >> 6) & 3) | ((((long)src[9]) << 2) & 1023) | ((((long)src[10]) << 10) & 262143) | ((((long)src[11]) << 18) & 67108863) | ((((long)src[12]) << 26) & 17179869183) | ((((long)src[13]) << 34) & 34359738367);            dest[3] = ((((long)src[13]) >> 1) & 127) | ((((long)src[14]) << 7) & 32767) | ((((long)src[15]) << 15) & 8388607) | ((((long)src[16]) << 23) & 2147483647) | ((((long)src[17]) << 31) & 34359738367);            dest[4] = ((((long)src[17]) >> 4) & 15) | ((((long)src[18]) << 4) & 4095) | ((((long)src[19]) << 12) & 1048575) | ((((long)src[20]) << 20) & 268435455) | ((((long)src[21]) << 28) & 34359738367);            dest[5] = ((((long)src[21]) >> 7) & 1) | ((((long)src[22]) << 1) & 511) | ((((long)src[23]) << 9) & 131071) | ((((long)src[24]) << 17) & 33554431) | ((((long)src[25]) << 25) & 8589934591) | ((((long)src[26]) << 33) & 34359738367);            dest[6] = ((((long)src[26]) >> 2) & 63) | ((((long)src[27]) << 6) & 16383) | ((((long)src[28]) << 14) & 4194303) | ((((long)src[29]) << 22) & 1073741823) | ((((long)src[30]) << 30) & 34359738367);            dest[7] = ((((long)src[30]) >> 5) & 7) | ((((long)src[31]) << 3) & 2047) | ((((long)src[32]) << 11) & 524287) | ((((long)src[33]) << 19) & 134217727) | ((((long)src[34]) << 27) & 34359738367);        }

        private static void Pack8LongValuesLE35(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 34359738367))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 34359738367) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 34359738367) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 34359738367) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 34359738367) >> 32)                | ((src[1] & 34359738367) << 3)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 34359738367) >> 5)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 34359738367) >> 13)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 34359738367) >> 21)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 34359738367) >> 29)                | ((src[2] & 34359738367) << 6)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 34359738367) >> 2)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 34359738367) >> 10)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 34359738367) >> 18)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 34359738367) >> 26)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 34359738367) >> 34)                | ((src[3] & 34359738367) << 1)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 34359738367) >> 7)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 34359738367) >> 15)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 34359738367) >> 23)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 34359738367) >> 31)                | ((src[4] & 34359738367) << 4)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 34359738367) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 34359738367) >> 12)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 34359738367) >> 20)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 34359738367) >> 28)                | ((src[5] & 34359738367) << 7)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 34359738367) >> 1)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 34359738367) >> 9)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 34359738367) >> 17)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 34359738367) >> 25)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 34359738367) >> 33)                | ((src[6] & 34359738367) << 2)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 34359738367) >> 6)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 34359738367) >> 14)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 34359738367) >> 22)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 34359738367) >> 30)                | ((src[7] & 34359738367) << 5)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 34359738367) >> 3)) & 255);
                            dest[32] = 
                (byte)((((src[7] & 34359738367) >> 11)) & 255);
                            dest[33] = 
                (byte)((((src[7] & 34359738367) >> 19)) & 255);
                            dest[34] = 
                (byte)((((src[7] & 34359738367) >> 27)) & 255);
                        }
        private static void Unpack8LongValuesBE35(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 27) & 34359738367) | ((((long)src[1]) << 19) & 134217727) | ((((long)src[2]) << 11) & 524287) | ((((long)src[3]) << 3) & 2047) | ((((long)src[4]) >> 5) & 7);            dest[1] = ((((long)src[4]) << 30) & 34359738367) | ((((long)src[5]) << 22) & 1073741823) | ((((long)src[6]) << 14) & 4194303) | ((((long)src[7]) << 6) & 16383) | ((((long)src[8]) >> 2) & 63);            dest[2] = ((((long)src[8]) << 33) & 34359738367) | ((((long)src[9]) << 25) & 8589934591) | ((((long)src[10]) << 17) & 33554431) | ((((long)src[11]) << 9) & 131071) | ((((long)src[12]) << 1) & 511) | ((((long)src[13]) >> 7) & 1);            dest[3] = ((((long)src[13]) << 28) & 34359738367) | ((((long)src[14]) << 20) & 268435455) | ((((long)src[15]) << 12) & 1048575) | ((((long)src[16]) << 4) & 4095) | ((((long)src[17]) >> 4) & 15);            dest[4] = ((((long)src[17]) << 31) & 34359738367) | ((((long)src[18]) << 23) & 2147483647) | ((((long)src[19]) << 15) & 8388607) | ((((long)src[20]) << 7) & 32767) | ((((long)src[21]) >> 1) & 127);            dest[5] = ((((long)src[21]) << 34) & 34359738367) | ((((long)src[22]) << 26) & 17179869183) | ((((long)src[23]) << 18) & 67108863) | ((((long)src[24]) << 10) & 262143) | ((((long)src[25]) << 2) & 1023) | ((((long)src[26]) >> 6) & 3);            dest[6] = ((((long)src[26]) << 29) & 34359738367) | ((((long)src[27]) << 21) & 536870911) | ((((long)src[28]) << 13) & 2097151) | ((((long)src[29]) << 5) & 8191) | ((((long)src[30]) >> 3) & 31);            dest[7] = ((((long)src[30]) << 32) & 34359738367) | ((((long)src[31]) << 24) & 4294967295) | ((((long)src[32]) << 16) & 16777215) | ((((long)src[33]) << 8) & 65535) | ((((long)src[34])) & 255);        }

        private static void Pack8LongValuesBE35(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 34359738367) >> 27)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 34359738367) >> 19)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 34359738367) >> 11)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 34359738367) >> 3)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 34359738367) << 5)                | ((src[1] & 34359738367) >> 30)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 34359738367) >> 22)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 34359738367) >> 14)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 34359738367) >> 6)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 34359738367) << 2)                | ((src[2] & 34359738367) >> 33)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 34359738367) >> 25)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 34359738367) >> 17)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 34359738367) >> 9)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 34359738367) >> 1)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 34359738367) << 7)                | ((src[3] & 34359738367) >> 28)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 34359738367) >> 20)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 34359738367) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 34359738367) >> 4)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 34359738367) << 4)                | ((src[4] & 34359738367) >> 31)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 34359738367) >> 23)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 34359738367) >> 15)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 34359738367) >> 7)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 34359738367) << 1)                | ((src[5] & 34359738367) >> 34)) & 255);
                            dest[22] = 
                (byte)((((src[5] & 34359738367) >> 26)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 34359738367) >> 18)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 34359738367) >> 10)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 34359738367) >> 2)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 34359738367) << 6)                | ((src[6] & 34359738367) >> 29)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 34359738367) >> 21)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 34359738367) >> 13)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 34359738367) >> 5)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 34359738367) << 3)                | ((src[7] & 34359738367) >> 32)) & 255);
                            dest[31] = 
                (byte)((((src[7] & 34359738367) >> 24)) & 255);
                            dest[32] = 
                (byte)((((src[7] & 34359738367) >> 16)) & 255);
                            dest[33] = 
                (byte)((((src[7] & 34359738367) >> 8)) & 255);
                            dest[34] = 
                (byte)((((src[7] & 34359738367))) & 255);
                        }
        private static void Unpack8LongValuesLE36(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 68719476735);            dest[1] = ((((long)src[4]) >> 4) & 15) | ((((long)src[5]) << 4) & 4095) | ((((long)src[6]) << 12) & 1048575) | ((((long)src[7]) << 20) & 268435455) | ((((long)src[8]) << 28) & 68719476735);            dest[2] = ((((long)src[9])) & 255) | ((((long)src[10]) << 8) & 65535) | ((((long)src[11]) << 16) & 16777215) | ((((long)src[12]) << 24) & 4294967295) | ((((long)src[13]) << 32) & 68719476735);            dest[3] = ((((long)src[13]) >> 4) & 15) | ((((long)src[14]) << 4) & 4095) | ((((long)src[15]) << 12) & 1048575) | ((((long)src[16]) << 20) & 268435455) | ((((long)src[17]) << 28) & 68719476735);            dest[4] = ((((long)src[18])) & 255) | ((((long)src[19]) << 8) & 65535) | ((((long)src[20]) << 16) & 16777215) | ((((long)src[21]) << 24) & 4294967295) | ((((long)src[22]) << 32) & 68719476735);            dest[5] = ((((long)src[22]) >> 4) & 15) | ((((long)src[23]) << 4) & 4095) | ((((long)src[24]) << 12) & 1048575) | ((((long)src[25]) << 20) & 268435455) | ((((long)src[26]) << 28) & 68719476735);            dest[6] = ((((long)src[27])) & 255) | ((((long)src[28]) << 8) & 65535) | ((((long)src[29]) << 16) & 16777215) | ((((long)src[30]) << 24) & 4294967295) | ((((long)src[31]) << 32) & 68719476735);            dest[7] = ((((long)src[31]) >> 4) & 15) | ((((long)src[32]) << 4) & 4095) | ((((long)src[33]) << 12) & 1048575) | ((((long)src[34]) << 20) & 268435455) | ((((long)src[35]) << 28) & 68719476735);        }

        private static void Pack8LongValuesLE36(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 68719476735))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 68719476735) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 68719476735) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 68719476735) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 68719476735) >> 32)                | ((src[1] & 68719476735) << 4)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 68719476735) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 68719476735) >> 12)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 68719476735) >> 20)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 68719476735) >> 28)) & 255);
                            dest[9] = 
                (byte)((((src[2] & 68719476735))) & 255);
                            dest[10] = 
                (byte)((((src[2] & 68719476735) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 68719476735) >> 16)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 68719476735) >> 24)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 68719476735) >> 32)                | ((src[3] & 68719476735) << 4)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 68719476735) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 68719476735) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 68719476735) >> 20)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 68719476735) >> 28)) & 255);
                            dest[18] = 
                (byte)((((src[4] & 68719476735))) & 255);
                            dest[19] = 
                (byte)((((src[4] & 68719476735) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 68719476735) >> 16)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 68719476735) >> 24)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 68719476735) >> 32)                | ((src[5] & 68719476735) << 4)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 68719476735) >> 4)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 68719476735) >> 12)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 68719476735) >> 20)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 68719476735) >> 28)) & 255);
                            dest[27] = 
                (byte)((((src[6] & 68719476735))) & 255);
                            dest[28] = 
                (byte)((((src[6] & 68719476735) >> 8)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 68719476735) >> 16)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 68719476735) >> 24)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 68719476735) >> 32)                | ((src[7] & 68719476735) << 4)) & 255);
                            dest[32] = 
                (byte)((((src[7] & 68719476735) >> 4)) & 255);
                            dest[33] = 
                (byte)((((src[7] & 68719476735) >> 12)) & 255);
                            dest[34] = 
                (byte)((((src[7] & 68719476735) >> 20)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 68719476735) >> 28)) & 255);
                        }
        private static void Unpack8LongValuesBE36(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 28) & 68719476735) | ((((long)src[1]) << 20) & 268435455) | ((((long)src[2]) << 12) & 1048575) | ((((long)src[3]) << 4) & 4095) | ((((long)src[4]) >> 4) & 15);            dest[1] = ((((long)src[4]) << 32) & 68719476735) | ((((long)src[5]) << 24) & 4294967295) | ((((long)src[6]) << 16) & 16777215) | ((((long)src[7]) << 8) & 65535) | ((((long)src[8])) & 255);            dest[2] = ((((long)src[9]) << 28) & 68719476735) | ((((long)src[10]) << 20) & 268435455) | ((((long)src[11]) << 12) & 1048575) | ((((long)src[12]) << 4) & 4095) | ((((long)src[13]) >> 4) & 15);            dest[3] = ((((long)src[13]) << 32) & 68719476735) | ((((long)src[14]) << 24) & 4294967295) | ((((long)src[15]) << 16) & 16777215) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17])) & 255);            dest[4] = ((((long)src[18]) << 28) & 68719476735) | ((((long)src[19]) << 20) & 268435455) | ((((long)src[20]) << 12) & 1048575) | ((((long)src[21]) << 4) & 4095) | ((((long)src[22]) >> 4) & 15);            dest[5] = ((((long)src[22]) << 32) & 68719476735) | ((((long)src[23]) << 24) & 4294967295) | ((((long)src[24]) << 16) & 16777215) | ((((long)src[25]) << 8) & 65535) | ((((long)src[26])) & 255);            dest[6] = ((((long)src[27]) << 28) & 68719476735) | ((((long)src[28]) << 20) & 268435455) | ((((long)src[29]) << 12) & 1048575) | ((((long)src[30]) << 4) & 4095) | ((((long)src[31]) >> 4) & 15);            dest[7] = ((((long)src[31]) << 32) & 68719476735) | ((((long)src[32]) << 24) & 4294967295) | ((((long)src[33]) << 16) & 16777215) | ((((long)src[34]) << 8) & 65535) | ((((long)src[35])) & 255);        }

        private static void Pack8LongValuesBE36(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 68719476735) >> 28)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 68719476735) >> 20)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 68719476735) >> 12)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 68719476735) >> 4)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 68719476735) << 4)                | ((src[1] & 68719476735) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 68719476735) >> 24)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 68719476735) >> 16)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 68719476735) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 68719476735))) & 255);
                            dest[9] = 
                (byte)((((src[2] & 68719476735) >> 28)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 68719476735) >> 20)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 68719476735) >> 12)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 68719476735) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 68719476735) << 4)                | ((src[3] & 68719476735) >> 32)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 68719476735) >> 24)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 68719476735) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 68719476735) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 68719476735))) & 255);
                            dest[18] = 
                (byte)((((src[4] & 68719476735) >> 28)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 68719476735) >> 20)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 68719476735) >> 12)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 68719476735) >> 4)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 68719476735) << 4)                | ((src[5] & 68719476735) >> 32)) & 255);
                            dest[23] = 
                (byte)((((src[5] & 68719476735) >> 24)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 68719476735) >> 16)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 68719476735) >> 8)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 68719476735))) & 255);
                            dest[27] = 
                (byte)((((src[6] & 68719476735) >> 28)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 68719476735) >> 20)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 68719476735) >> 12)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 68719476735) >> 4)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 68719476735) << 4)                | ((src[7] & 68719476735) >> 32)) & 255);
                            dest[32] = 
                (byte)((((src[7] & 68719476735) >> 24)) & 255);
                            dest[33] = 
                (byte)((((src[7] & 68719476735) >> 16)) & 255);
                            dest[34] = 
                (byte)((((src[7] & 68719476735) >> 8)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 68719476735))) & 255);
                        }
        private static void Unpack8LongValuesLE37(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 137438953471);            dest[1] = ((((long)src[4]) >> 5) & 7) | ((((long)src[5]) << 3) & 2047) | ((((long)src[6]) << 11) & 524287) | ((((long)src[7]) << 19) & 134217727) | ((((long)src[8]) << 27) & 34359738367) | ((((long)src[9]) << 35) & 137438953471);            dest[2] = ((((long)src[9]) >> 2) & 63) | ((((long)src[10]) << 6) & 16383) | ((((long)src[11]) << 14) & 4194303) | ((((long)src[12]) << 22) & 1073741823) | ((((long)src[13]) << 30) & 137438953471);            dest[3] = ((((long)src[13]) >> 7) & 1) | ((((long)src[14]) << 1) & 511) | ((((long)src[15]) << 9) & 131071) | ((((long)src[16]) << 17) & 33554431) | ((((long)src[17]) << 25) & 8589934591) | ((((long)src[18]) << 33) & 137438953471);            dest[4] = ((((long)src[18]) >> 4) & 15) | ((((long)src[19]) << 4) & 4095) | ((((long)src[20]) << 12) & 1048575) | ((((long)src[21]) << 20) & 268435455) | ((((long)src[22]) << 28) & 68719476735) | ((((long)src[23]) << 36) & 137438953471);            dest[5] = ((((long)src[23]) >> 1) & 127) | ((((long)src[24]) << 7) & 32767) | ((((long)src[25]) << 15) & 8388607) | ((((long)src[26]) << 23) & 2147483647) | ((((long)src[27]) << 31) & 137438953471);            dest[6] = ((((long)src[27]) >> 6) & 3) | ((((long)src[28]) << 2) & 1023) | ((((long)src[29]) << 10) & 262143) | ((((long)src[30]) << 18) & 67108863) | ((((long)src[31]) << 26) & 17179869183) | ((((long)src[32]) << 34) & 137438953471);            dest[7] = ((((long)src[32]) >> 3) & 31) | ((((long)src[33]) << 5) & 8191) | ((((long)src[34]) << 13) & 2097151) | ((((long)src[35]) << 21) & 536870911) | ((((long)src[36]) << 29) & 137438953471);        }

        private static void Pack8LongValuesLE37(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 137438953471))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 137438953471) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 137438953471) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 137438953471) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 137438953471) >> 32)                | ((src[1] & 137438953471) << 5)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 137438953471) >> 3)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 137438953471) >> 11)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 137438953471) >> 19)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 137438953471) >> 27)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 137438953471) >> 35)                | ((src[2] & 137438953471) << 2)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 137438953471) >> 6)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 137438953471) >> 14)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 137438953471) >> 22)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 137438953471) >> 30)                | ((src[3] & 137438953471) << 7)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 137438953471) >> 1)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 137438953471) >> 9)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 137438953471) >> 17)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 137438953471) >> 25)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 137438953471) >> 33)                | ((src[4] & 137438953471) << 4)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 137438953471) >> 4)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 137438953471) >> 12)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 137438953471) >> 20)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 137438953471) >> 28)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 137438953471) >> 36)                | ((src[5] & 137438953471) << 1)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 137438953471) >> 7)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 137438953471) >> 15)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 137438953471) >> 23)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 137438953471) >> 31)                | ((src[6] & 137438953471) << 6)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 137438953471) >> 2)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 137438953471) >> 10)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 137438953471) >> 18)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 137438953471) >> 26)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 137438953471) >> 34)                | ((src[7] & 137438953471) << 3)) & 255);
                            dest[33] = 
                (byte)((((src[7] & 137438953471) >> 5)) & 255);
                            dest[34] = 
                (byte)((((src[7] & 137438953471) >> 13)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 137438953471) >> 21)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 137438953471) >> 29)) & 255);
                        }
        private static void Unpack8LongValuesBE37(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 29) & 137438953471) | ((((long)src[1]) << 21) & 536870911) | ((((long)src[2]) << 13) & 2097151) | ((((long)src[3]) << 5) & 8191) | ((((long)src[4]) >> 3) & 31);            dest[1] = ((((long)src[4]) << 34) & 137438953471) | ((((long)src[5]) << 26) & 17179869183) | ((((long)src[6]) << 18) & 67108863) | ((((long)src[7]) << 10) & 262143) | ((((long)src[8]) << 2) & 1023) | ((((long)src[9]) >> 6) & 3);            dest[2] = ((((long)src[9]) << 31) & 137438953471) | ((((long)src[10]) << 23) & 2147483647) | ((((long)src[11]) << 15) & 8388607) | ((((long)src[12]) << 7) & 32767) | ((((long)src[13]) >> 1) & 127);            dest[3] = ((((long)src[13]) << 36) & 137438953471) | ((((long)src[14]) << 28) & 68719476735) | ((((long)src[15]) << 20) & 268435455) | ((((long)src[16]) << 12) & 1048575) | ((((long)src[17]) << 4) & 4095) | ((((long)src[18]) >> 4) & 15);            dest[4] = ((((long)src[18]) << 33) & 137438953471) | ((((long)src[19]) << 25) & 8589934591) | ((((long)src[20]) << 17) & 33554431) | ((((long)src[21]) << 9) & 131071) | ((((long)src[22]) << 1) & 511) | ((((long)src[23]) >> 7) & 1);            dest[5] = ((((long)src[23]) << 30) & 137438953471) | ((((long)src[24]) << 22) & 1073741823) | ((((long)src[25]) << 14) & 4194303) | ((((long)src[26]) << 6) & 16383) | ((((long)src[27]) >> 2) & 63);            dest[6] = ((((long)src[27]) << 35) & 137438953471) | ((((long)src[28]) << 27) & 34359738367) | ((((long)src[29]) << 19) & 134217727) | ((((long)src[30]) << 11) & 524287) | ((((long)src[31]) << 3) & 2047) | ((((long)src[32]) >> 5) & 7);            dest[7] = ((((long)src[32]) << 32) & 137438953471) | ((((long)src[33]) << 24) & 4294967295) | ((((long)src[34]) << 16) & 16777215) | ((((long)src[35]) << 8) & 65535) | ((((long)src[36])) & 255);        }

        private static void Pack8LongValuesBE37(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 137438953471) >> 29)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 137438953471) >> 21)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 137438953471) >> 13)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 137438953471) >> 5)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 137438953471) << 3)                | ((src[1] & 137438953471) >> 34)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 137438953471) >> 26)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 137438953471) >> 18)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 137438953471) >> 10)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 137438953471) >> 2)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 137438953471) << 6)                | ((src[2] & 137438953471) >> 31)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 137438953471) >> 23)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 137438953471) >> 15)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 137438953471) >> 7)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 137438953471) << 1)                | ((src[3] & 137438953471) >> 36)) & 255);
                            dest[14] = 
                (byte)((((src[3] & 137438953471) >> 28)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 137438953471) >> 20)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 137438953471) >> 12)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 137438953471) >> 4)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 137438953471) << 4)                | ((src[4] & 137438953471) >> 33)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 137438953471) >> 25)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 137438953471) >> 17)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 137438953471) >> 9)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 137438953471) >> 1)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 137438953471) << 7)                | ((src[5] & 137438953471) >> 30)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 137438953471) >> 22)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 137438953471) >> 14)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 137438953471) >> 6)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 137438953471) << 2)                | ((src[6] & 137438953471) >> 35)) & 255);
                            dest[28] = 
                (byte)((((src[6] & 137438953471) >> 27)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 137438953471) >> 19)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 137438953471) >> 11)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 137438953471) >> 3)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 137438953471) << 5)                | ((src[7] & 137438953471) >> 32)) & 255);
                            dest[33] = 
                (byte)((((src[7] & 137438953471) >> 24)) & 255);
                            dest[34] = 
                (byte)((((src[7] & 137438953471) >> 16)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 137438953471) >> 8)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 137438953471))) & 255);
                        }
        private static void Unpack8LongValuesLE38(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 274877906943);            dest[1] = ((((long)src[4]) >> 6) & 3) | ((((long)src[5]) << 2) & 1023) | ((((long)src[6]) << 10) & 262143) | ((((long)src[7]) << 18) & 67108863) | ((((long)src[8]) << 26) & 17179869183) | ((((long)src[9]) << 34) & 274877906943);            dest[2] = ((((long)src[9]) >> 4) & 15) | ((((long)src[10]) << 4) & 4095) | ((((long)src[11]) << 12) & 1048575) | ((((long)src[12]) << 20) & 268435455) | ((((long)src[13]) << 28) & 68719476735) | ((((long)src[14]) << 36) & 274877906943);            dest[3] = ((((long)src[14]) >> 2) & 63) | ((((long)src[15]) << 6) & 16383) | ((((long)src[16]) << 14) & 4194303) | ((((long)src[17]) << 22) & 1073741823) | ((((long)src[18]) << 30) & 274877906943);            dest[4] = ((((long)src[19])) & 255) | ((((long)src[20]) << 8) & 65535) | ((((long)src[21]) << 16) & 16777215) | ((((long)src[22]) << 24) & 4294967295) | ((((long)src[23]) << 32) & 274877906943);            dest[5] = ((((long)src[23]) >> 6) & 3) | ((((long)src[24]) << 2) & 1023) | ((((long)src[25]) << 10) & 262143) | ((((long)src[26]) << 18) & 67108863) | ((((long)src[27]) << 26) & 17179869183) | ((((long)src[28]) << 34) & 274877906943);            dest[6] = ((((long)src[28]) >> 4) & 15) | ((((long)src[29]) << 4) & 4095) | ((((long)src[30]) << 12) & 1048575) | ((((long)src[31]) << 20) & 268435455) | ((((long)src[32]) << 28) & 68719476735) | ((((long)src[33]) << 36) & 274877906943);            dest[7] = ((((long)src[33]) >> 2) & 63) | ((((long)src[34]) << 6) & 16383) | ((((long)src[35]) << 14) & 4194303) | ((((long)src[36]) << 22) & 1073741823) | ((((long)src[37]) << 30) & 274877906943);        }

        private static void Pack8LongValuesLE38(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 274877906943))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 274877906943) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 274877906943) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 274877906943) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 274877906943) >> 32)                | ((src[1] & 274877906943) << 6)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 274877906943) >> 2)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 274877906943) >> 10)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 274877906943) >> 18)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 274877906943) >> 26)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 274877906943) >> 34)                | ((src[2] & 274877906943) << 4)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 274877906943) >> 4)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 274877906943) >> 12)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 274877906943) >> 20)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 274877906943) >> 28)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 274877906943) >> 36)                | ((src[3] & 274877906943) << 2)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 274877906943) >> 6)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 274877906943) >> 14)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 274877906943) >> 22)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 274877906943) >> 30)) & 255);
                            dest[19] = 
                (byte)((((src[4] & 274877906943))) & 255);
                            dest[20] = 
                (byte)((((src[4] & 274877906943) >> 8)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 274877906943) >> 16)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 274877906943) >> 24)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 274877906943) >> 32)                | ((src[5] & 274877906943) << 6)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 274877906943) >> 2)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 274877906943) >> 10)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 274877906943) >> 18)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 274877906943) >> 26)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 274877906943) >> 34)                | ((src[6] & 274877906943) << 4)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 274877906943) >> 4)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 274877906943) >> 12)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 274877906943) >> 20)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 274877906943) >> 28)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 274877906943) >> 36)                | ((src[7] & 274877906943) << 2)) & 255);
                            dest[34] = 
                (byte)((((src[7] & 274877906943) >> 6)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 274877906943) >> 14)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 274877906943) >> 22)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 274877906943) >> 30)) & 255);
                        }
        private static void Unpack8LongValuesBE38(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 30) & 274877906943) | ((((long)src[1]) << 22) & 1073741823) | ((((long)src[2]) << 14) & 4194303) | ((((long)src[3]) << 6) & 16383) | ((((long)src[4]) >> 2) & 63);            dest[1] = ((((long)src[4]) << 36) & 274877906943) | ((((long)src[5]) << 28) & 68719476735) | ((((long)src[6]) << 20) & 268435455) | ((((long)src[7]) << 12) & 1048575) | ((((long)src[8]) << 4) & 4095) | ((((long)src[9]) >> 4) & 15);            dest[2] = ((((long)src[9]) << 34) & 274877906943) | ((((long)src[10]) << 26) & 17179869183) | ((((long)src[11]) << 18) & 67108863) | ((((long)src[12]) << 10) & 262143) | ((((long)src[13]) << 2) & 1023) | ((((long)src[14]) >> 6) & 3);            dest[3] = ((((long)src[14]) << 32) & 274877906943) | ((((long)src[15]) << 24) & 4294967295) | ((((long)src[16]) << 16) & 16777215) | ((((long)src[17]) << 8) & 65535) | ((((long)src[18])) & 255);            dest[4] = ((((long)src[19]) << 30) & 274877906943) | ((((long)src[20]) << 22) & 1073741823) | ((((long)src[21]) << 14) & 4194303) | ((((long)src[22]) << 6) & 16383) | ((((long)src[23]) >> 2) & 63);            dest[5] = ((((long)src[23]) << 36) & 274877906943) | ((((long)src[24]) << 28) & 68719476735) | ((((long)src[25]) << 20) & 268435455) | ((((long)src[26]) << 12) & 1048575) | ((((long)src[27]) << 4) & 4095) | ((((long)src[28]) >> 4) & 15);            dest[6] = ((((long)src[28]) << 34) & 274877906943) | ((((long)src[29]) << 26) & 17179869183) | ((((long)src[30]) << 18) & 67108863) | ((((long)src[31]) << 10) & 262143) | ((((long)src[32]) << 2) & 1023) | ((((long)src[33]) >> 6) & 3);            dest[7] = ((((long)src[33]) << 32) & 274877906943) | ((((long)src[34]) << 24) & 4294967295) | ((((long)src[35]) << 16) & 16777215) | ((((long)src[36]) << 8) & 65535) | ((((long)src[37])) & 255);        }

        private static void Pack8LongValuesBE38(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 274877906943) >> 30)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 274877906943) >> 22)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 274877906943) >> 14)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 274877906943) >> 6)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 274877906943) << 2)                | ((src[1] & 274877906943) >> 36)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 274877906943) >> 28)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 274877906943) >> 20)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 274877906943) >> 12)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 274877906943) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 274877906943) << 4)                | ((src[2] & 274877906943) >> 34)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 274877906943) >> 26)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 274877906943) >> 18)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 274877906943) >> 10)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 274877906943) >> 2)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 274877906943) << 6)                | ((src[3] & 274877906943) >> 32)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 274877906943) >> 24)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 274877906943) >> 16)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 274877906943) >> 8)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 274877906943))) & 255);
                            dest[19] = 
                (byte)((((src[4] & 274877906943) >> 30)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 274877906943) >> 22)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 274877906943) >> 14)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 274877906943) >> 6)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 274877906943) << 2)                | ((src[5] & 274877906943) >> 36)) & 255);
                            dest[24] = 
                (byte)((((src[5] & 274877906943) >> 28)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 274877906943) >> 20)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 274877906943) >> 12)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 274877906943) >> 4)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 274877906943) << 4)                | ((src[6] & 274877906943) >> 34)) & 255);
                            dest[29] = 
                (byte)((((src[6] & 274877906943) >> 26)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 274877906943) >> 18)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 274877906943) >> 10)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 274877906943) >> 2)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 274877906943) << 6)                | ((src[7] & 274877906943) >> 32)) & 255);
                            dest[34] = 
                (byte)((((src[7] & 274877906943) >> 24)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 274877906943) >> 16)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 274877906943) >> 8)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 274877906943))) & 255);
                        }
        private static void Unpack8LongValuesLE39(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 549755813887);            dest[1] = ((((long)src[4]) >> 7) & 1) | ((((long)src[5]) << 1) & 511) | ((((long)src[6]) << 9) & 131071) | ((((long)src[7]) << 17) & 33554431) | ((((long)src[8]) << 25) & 8589934591) | ((((long)src[9]) << 33) & 549755813887);            dest[2] = ((((long)src[9]) >> 6) & 3) | ((((long)src[10]) << 2) & 1023) | ((((long)src[11]) << 10) & 262143) | ((((long)src[12]) << 18) & 67108863) | ((((long)src[13]) << 26) & 17179869183) | ((((long)src[14]) << 34) & 549755813887);            dest[3] = ((((long)src[14]) >> 5) & 7) | ((((long)src[15]) << 3) & 2047) | ((((long)src[16]) << 11) & 524287) | ((((long)src[17]) << 19) & 134217727) | ((((long)src[18]) << 27) & 34359738367) | ((((long)src[19]) << 35) & 549755813887);            dest[4] = ((((long)src[19]) >> 4) & 15) | ((((long)src[20]) << 4) & 4095) | ((((long)src[21]) << 12) & 1048575) | ((((long)src[22]) << 20) & 268435455) | ((((long)src[23]) << 28) & 68719476735) | ((((long)src[24]) << 36) & 549755813887);            dest[5] = ((((long)src[24]) >> 3) & 31) | ((((long)src[25]) << 5) & 8191) | ((((long)src[26]) << 13) & 2097151) | ((((long)src[27]) << 21) & 536870911) | ((((long)src[28]) << 29) & 137438953471) | ((((long)src[29]) << 37) & 549755813887);            dest[6] = ((((long)src[29]) >> 2) & 63) | ((((long)src[30]) << 6) & 16383) | ((((long)src[31]) << 14) & 4194303) | ((((long)src[32]) << 22) & 1073741823) | ((((long)src[33]) << 30) & 274877906943) | ((((long)src[34]) << 38) & 549755813887);            dest[7] = ((((long)src[34]) >> 1) & 127) | ((((long)src[35]) << 7) & 32767) | ((((long)src[36]) << 15) & 8388607) | ((((long)src[37]) << 23) & 2147483647) | ((((long)src[38]) << 31) & 549755813887);        }

        private static void Pack8LongValuesLE39(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 549755813887))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 549755813887) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 549755813887) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 549755813887) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 549755813887) >> 32)                | ((src[1] & 549755813887) << 7)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 549755813887) >> 1)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 549755813887) >> 9)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 549755813887) >> 17)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 549755813887) >> 25)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 549755813887) >> 33)                | ((src[2] & 549755813887) << 6)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 549755813887) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 549755813887) >> 10)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 549755813887) >> 18)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 549755813887) >> 26)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 549755813887) >> 34)                | ((src[3] & 549755813887) << 5)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 549755813887) >> 3)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 549755813887) >> 11)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 549755813887) >> 19)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 549755813887) >> 27)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 549755813887) >> 35)                | ((src[4] & 549755813887) << 4)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 549755813887) >> 4)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 549755813887) >> 12)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 549755813887) >> 20)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 549755813887) >> 28)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 549755813887) >> 36)                | ((src[5] & 549755813887) << 3)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 549755813887) >> 5)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 549755813887) >> 13)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 549755813887) >> 21)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 549755813887) >> 29)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 549755813887) >> 37)                | ((src[6] & 549755813887) << 2)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 549755813887) >> 6)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 549755813887) >> 14)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 549755813887) >> 22)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 549755813887) >> 30)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 549755813887) >> 38)                | ((src[7] & 549755813887) << 1)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 549755813887) >> 7)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 549755813887) >> 15)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 549755813887) >> 23)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 549755813887) >> 31)) & 255);
                        }
        private static void Unpack8LongValuesBE39(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 31) & 549755813887) | ((((long)src[1]) << 23) & 2147483647) | ((((long)src[2]) << 15) & 8388607) | ((((long)src[3]) << 7) & 32767) | ((((long)src[4]) >> 1) & 127);            dest[1] = ((((long)src[4]) << 38) & 549755813887) | ((((long)src[5]) << 30) & 274877906943) | ((((long)src[6]) << 22) & 1073741823) | ((((long)src[7]) << 14) & 4194303) | ((((long)src[8]) << 6) & 16383) | ((((long)src[9]) >> 2) & 63);            dest[2] = ((((long)src[9]) << 37) & 549755813887) | ((((long)src[10]) << 29) & 137438953471) | ((((long)src[11]) << 21) & 536870911) | ((((long)src[12]) << 13) & 2097151) | ((((long)src[13]) << 5) & 8191) | ((((long)src[14]) >> 3) & 31);            dest[3] = ((((long)src[14]) << 36) & 549755813887) | ((((long)src[15]) << 28) & 68719476735) | ((((long)src[16]) << 20) & 268435455) | ((((long)src[17]) << 12) & 1048575) | ((((long)src[18]) << 4) & 4095) | ((((long)src[19]) >> 4) & 15);            dest[4] = ((((long)src[19]) << 35) & 549755813887) | ((((long)src[20]) << 27) & 34359738367) | ((((long)src[21]) << 19) & 134217727) | ((((long)src[22]) << 11) & 524287) | ((((long)src[23]) << 3) & 2047) | ((((long)src[24]) >> 5) & 7);            dest[5] = ((((long)src[24]) << 34) & 549755813887) | ((((long)src[25]) << 26) & 17179869183) | ((((long)src[26]) << 18) & 67108863) | ((((long)src[27]) << 10) & 262143) | ((((long)src[28]) << 2) & 1023) | ((((long)src[29]) >> 6) & 3);            dest[6] = ((((long)src[29]) << 33) & 549755813887) | ((((long)src[30]) << 25) & 8589934591) | ((((long)src[31]) << 17) & 33554431) | ((((long)src[32]) << 9) & 131071) | ((((long)src[33]) << 1) & 511) | ((((long)src[34]) >> 7) & 1);            dest[7] = ((((long)src[34]) << 32) & 549755813887) | ((((long)src[35]) << 24) & 4294967295) | ((((long)src[36]) << 16) & 16777215) | ((((long)src[37]) << 8) & 65535) | ((((long)src[38])) & 255);        }

        private static void Pack8LongValuesBE39(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 549755813887) >> 31)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 549755813887) >> 23)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 549755813887) >> 15)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 549755813887) >> 7)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 549755813887) << 1)                | ((src[1] & 549755813887) >> 38)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 549755813887) >> 30)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 549755813887) >> 22)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 549755813887) >> 14)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 549755813887) >> 6)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 549755813887) << 2)                | ((src[2] & 549755813887) >> 37)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 549755813887) >> 29)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 549755813887) >> 21)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 549755813887) >> 13)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 549755813887) >> 5)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 549755813887) << 3)                | ((src[3] & 549755813887) >> 36)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 549755813887) >> 28)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 549755813887) >> 20)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 549755813887) >> 12)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 549755813887) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 549755813887) << 4)                | ((src[4] & 549755813887) >> 35)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 549755813887) >> 27)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 549755813887) >> 19)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 549755813887) >> 11)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 549755813887) >> 3)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 549755813887) << 5)                | ((src[5] & 549755813887) >> 34)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 549755813887) >> 26)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 549755813887) >> 18)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 549755813887) >> 10)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 549755813887) >> 2)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 549755813887) << 6)                | ((src[6] & 549755813887) >> 33)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 549755813887) >> 25)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 549755813887) >> 17)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 549755813887) >> 9)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 549755813887) >> 1)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 549755813887) << 7)                | ((src[7] & 549755813887) >> 32)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 549755813887) >> 24)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 549755813887) >> 16)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 549755813887) >> 8)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 549755813887))) & 255);
                        }
        private static void Unpack8LongValuesLE40(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775);            dest[1] = ((((long)src[5])) & 255) | ((((long)src[6]) << 8) & 65535) | ((((long)src[7]) << 16) & 16777215) | ((((long)src[8]) << 24) & 4294967295) | ((((long)src[9]) << 32) & 1099511627775);            dest[2] = ((((long)src[10])) & 255) | ((((long)src[11]) << 8) & 65535) | ((((long)src[12]) << 16) & 16777215) | ((((long)src[13]) << 24) & 4294967295) | ((((long)src[14]) << 32) & 1099511627775);            dest[3] = ((((long)src[15])) & 255) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17]) << 16) & 16777215) | ((((long)src[18]) << 24) & 4294967295) | ((((long)src[19]) << 32) & 1099511627775);            dest[4] = ((((long)src[20])) & 255) | ((((long)src[21]) << 8) & 65535) | ((((long)src[22]) << 16) & 16777215) | ((((long)src[23]) << 24) & 4294967295) | ((((long)src[24]) << 32) & 1099511627775);            dest[5] = ((((long)src[25])) & 255) | ((((long)src[26]) << 8) & 65535) | ((((long)src[27]) << 16) & 16777215) | ((((long)src[28]) << 24) & 4294967295) | ((((long)src[29]) << 32) & 1099511627775);            dest[6] = ((((long)src[30])) & 255) | ((((long)src[31]) << 8) & 65535) | ((((long)src[32]) << 16) & 16777215) | ((((long)src[33]) << 24) & 4294967295) | ((((long)src[34]) << 32) & 1099511627775);            dest[7] = ((((long)src[35])) & 255) | ((((long)src[36]) << 8) & 65535) | ((((long)src[37]) << 16) & 16777215) | ((((long)src[38]) << 24) & 4294967295) | ((((long)src[39]) << 32) & 1099511627775);        }

        private static void Pack8LongValuesLE40(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1099511627775))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1099511627775) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1099511627775) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1099511627775) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 1099511627775) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[1] & 1099511627775))) & 255);
                            dest[6] = 
                (byte)((((src[1] & 1099511627775) >> 8)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 1099511627775) >> 16)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 1099511627775) >> 24)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 1099511627775) >> 32)) & 255);
                            dest[10] = 
                (byte)((((src[2] & 1099511627775))) & 255);
                            dest[11] = 
                (byte)((((src[2] & 1099511627775) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 1099511627775) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 1099511627775) >> 24)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 1099511627775) >> 32)) & 255);
                            dest[15] = 
                (byte)((((src[3] & 1099511627775))) & 255);
                            dest[16] = 
                (byte)((((src[3] & 1099511627775) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 1099511627775) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 1099511627775) >> 24)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 1099511627775) >> 32)) & 255);
                            dest[20] = 
                (byte)((((src[4] & 1099511627775))) & 255);
                            dest[21] = 
                (byte)((((src[4] & 1099511627775) >> 8)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 1099511627775) >> 16)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 1099511627775) >> 24)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 1099511627775) >> 32)) & 255);
                            dest[25] = 
                (byte)((((src[5] & 1099511627775))) & 255);
                            dest[26] = 
                (byte)((((src[5] & 1099511627775) >> 8)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 1099511627775) >> 16)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 1099511627775) >> 24)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 1099511627775) >> 32)) & 255);
                            dest[30] = 
                (byte)((((src[6] & 1099511627775))) & 255);
                            dest[31] = 
                (byte)((((src[6] & 1099511627775) >> 8)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 1099511627775) >> 16)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 1099511627775) >> 24)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 1099511627775) >> 32)) & 255);
                            dest[35] = 
                (byte)((((src[7] & 1099511627775))) & 255);
                            dest[36] = 
                (byte)((((src[7] & 1099511627775) >> 8)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 1099511627775) >> 16)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 1099511627775) >> 24)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 1099511627775) >> 32)) & 255);
                        }
        private static void Unpack8LongValuesBE40(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 32) & 1099511627775) | ((((long)src[1]) << 24) & 4294967295) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 8) & 65535) | ((((long)src[4])) & 255);            dest[1] = ((((long)src[5]) << 32) & 1099511627775) | ((((long)src[6]) << 24) & 4294967295) | ((((long)src[7]) << 16) & 16777215) | ((((long)src[8]) << 8) & 65535) | ((((long)src[9])) & 255);            dest[2] = ((((long)src[10]) << 32) & 1099511627775) | ((((long)src[11]) << 24) & 4294967295) | ((((long)src[12]) << 16) & 16777215) | ((((long)src[13]) << 8) & 65535) | ((((long)src[14])) & 255);            dest[3] = ((((long)src[15]) << 32) & 1099511627775) | ((((long)src[16]) << 24) & 4294967295) | ((((long)src[17]) << 16) & 16777215) | ((((long)src[18]) << 8) & 65535) | ((((long)src[19])) & 255);            dest[4] = ((((long)src[20]) << 32) & 1099511627775) | ((((long)src[21]) << 24) & 4294967295) | ((((long)src[22]) << 16) & 16777215) | ((((long)src[23]) << 8) & 65535) | ((((long)src[24])) & 255);            dest[5] = ((((long)src[25]) << 32) & 1099511627775) | ((((long)src[26]) << 24) & 4294967295) | ((((long)src[27]) << 16) & 16777215) | ((((long)src[28]) << 8) & 65535) | ((((long)src[29])) & 255);            dest[6] = ((((long)src[30]) << 32) & 1099511627775) | ((((long)src[31]) << 24) & 4294967295) | ((((long)src[32]) << 16) & 16777215) | ((((long)src[33]) << 8) & 65535) | ((((long)src[34])) & 255);            dest[7] = ((((long)src[35]) << 32) & 1099511627775) | ((((long)src[36]) << 24) & 4294967295) | ((((long)src[37]) << 16) & 16777215) | ((((long)src[38]) << 8) & 65535) | ((((long)src[39])) & 255);        }

        private static void Pack8LongValuesBE40(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1099511627775) >> 32)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1099511627775) >> 24)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1099511627775) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1099511627775) >> 8)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 1099511627775))) & 255);
                            dest[5] = 
                (byte)((((src[1] & 1099511627775) >> 32)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 1099511627775) >> 24)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 1099511627775) >> 16)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 1099511627775) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 1099511627775))) & 255);
                            dest[10] = 
                (byte)((((src[2] & 1099511627775) >> 32)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 1099511627775) >> 24)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 1099511627775) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 1099511627775) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 1099511627775))) & 255);
                            dest[15] = 
                (byte)((((src[3] & 1099511627775) >> 32)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 1099511627775) >> 24)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 1099511627775) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 1099511627775) >> 8)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 1099511627775))) & 255);
                            dest[20] = 
                (byte)((((src[4] & 1099511627775) >> 32)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 1099511627775) >> 24)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 1099511627775) >> 16)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 1099511627775) >> 8)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 1099511627775))) & 255);
                            dest[25] = 
                (byte)((((src[5] & 1099511627775) >> 32)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 1099511627775) >> 24)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 1099511627775) >> 16)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 1099511627775) >> 8)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 1099511627775))) & 255);
                            dest[30] = 
                (byte)((((src[6] & 1099511627775) >> 32)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 1099511627775) >> 24)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 1099511627775) >> 16)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 1099511627775) >> 8)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 1099511627775))) & 255);
                            dest[35] = 
                (byte)((((src[7] & 1099511627775) >> 32)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 1099511627775) >> 24)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 1099511627775) >> 16)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 1099511627775) >> 8)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 1099511627775))) & 255);
                        }
        private static void Unpack8LongValuesLE41(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 2199023255551);            dest[1] = ((((long)src[5]) >> 1) & 127) | ((((long)src[6]) << 7) & 32767) | ((((long)src[7]) << 15) & 8388607) | ((((long)src[8]) << 23) & 2147483647) | ((((long)src[9]) << 31) & 549755813887) | ((((long)src[10]) << 39) & 2199023255551);            dest[2] = ((((long)src[10]) >> 2) & 63) | ((((long)src[11]) << 6) & 16383) | ((((long)src[12]) << 14) & 4194303) | ((((long)src[13]) << 22) & 1073741823) | ((((long)src[14]) << 30) & 274877906943) | ((((long)src[15]) << 38) & 2199023255551);            dest[3] = ((((long)src[15]) >> 3) & 31) | ((((long)src[16]) << 5) & 8191) | ((((long)src[17]) << 13) & 2097151) | ((((long)src[18]) << 21) & 536870911) | ((((long)src[19]) << 29) & 137438953471) | ((((long)src[20]) << 37) & 2199023255551);            dest[4] = ((((long)src[20]) >> 4) & 15) | ((((long)src[21]) << 4) & 4095) | ((((long)src[22]) << 12) & 1048575) | ((((long)src[23]) << 20) & 268435455) | ((((long)src[24]) << 28) & 68719476735) | ((((long)src[25]) << 36) & 2199023255551);            dest[5] = ((((long)src[25]) >> 5) & 7) | ((((long)src[26]) << 3) & 2047) | ((((long)src[27]) << 11) & 524287) | ((((long)src[28]) << 19) & 134217727) | ((((long)src[29]) << 27) & 34359738367) | ((((long)src[30]) << 35) & 2199023255551);            dest[6] = ((((long)src[30]) >> 6) & 3) | ((((long)src[31]) << 2) & 1023) | ((((long)src[32]) << 10) & 262143) | ((((long)src[33]) << 18) & 67108863) | ((((long)src[34]) << 26) & 17179869183) | ((((long)src[35]) << 34) & 2199023255551);            dest[7] = ((((long)src[35]) >> 7) & 1) | ((((long)src[36]) << 1) & 511) | ((((long)src[37]) << 9) & 131071) | ((((long)src[38]) << 17) & 33554431) | ((((long)src[39]) << 25) & 8589934591) | ((((long)src[40]) << 33) & 2199023255551);        }

        private static void Pack8LongValuesLE41(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2199023255551))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2199023255551) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2199023255551) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2199023255551) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 2199023255551) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 2199023255551) >> 40)                | ((src[1] & 2199023255551) << 1)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 2199023255551) >> 7)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 2199023255551) >> 15)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 2199023255551) >> 23)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 2199023255551) >> 31)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 2199023255551) >> 39)                | ((src[2] & 2199023255551) << 2)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 2199023255551) >> 6)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 2199023255551) >> 14)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 2199023255551) >> 22)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 2199023255551) >> 30)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 2199023255551) >> 38)                | ((src[3] & 2199023255551) << 3)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 2199023255551) >> 5)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 2199023255551) >> 13)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 2199023255551) >> 21)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 2199023255551) >> 29)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 2199023255551) >> 37)                | ((src[4] & 2199023255551) << 4)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 2199023255551) >> 4)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 2199023255551) >> 12)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 2199023255551) >> 20)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 2199023255551) >> 28)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 2199023255551) >> 36)                | ((src[5] & 2199023255551) << 5)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 2199023255551) >> 3)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 2199023255551) >> 11)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 2199023255551) >> 19)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 2199023255551) >> 27)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 2199023255551) >> 35)                | ((src[6] & 2199023255551) << 6)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 2199023255551) >> 2)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 2199023255551) >> 10)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 2199023255551) >> 18)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 2199023255551) >> 26)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 2199023255551) >> 34)                | ((src[7] & 2199023255551) << 7)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 2199023255551) >> 1)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 2199023255551) >> 9)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 2199023255551) >> 17)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 2199023255551) >> 25)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 2199023255551) >> 33)) & 255);
                        }
        private static void Unpack8LongValuesBE41(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 33) & 2199023255551) | ((((long)src[1]) << 25) & 8589934591) | ((((long)src[2]) << 17) & 33554431) | ((((long)src[3]) << 9) & 131071) | ((((long)src[4]) << 1) & 511) | ((((long)src[5]) >> 7) & 1);            dest[1] = ((((long)src[5]) << 34) & 2199023255551) | ((((long)src[6]) << 26) & 17179869183) | ((((long)src[7]) << 18) & 67108863) | ((((long)src[8]) << 10) & 262143) | ((((long)src[9]) << 2) & 1023) | ((((long)src[10]) >> 6) & 3);            dest[2] = ((((long)src[10]) << 35) & 2199023255551) | ((((long)src[11]) << 27) & 34359738367) | ((((long)src[12]) << 19) & 134217727) | ((((long)src[13]) << 11) & 524287) | ((((long)src[14]) << 3) & 2047) | ((((long)src[15]) >> 5) & 7);            dest[3] = ((((long)src[15]) << 36) & 2199023255551) | ((((long)src[16]) << 28) & 68719476735) | ((((long)src[17]) << 20) & 268435455) | ((((long)src[18]) << 12) & 1048575) | ((((long)src[19]) << 4) & 4095) | ((((long)src[20]) >> 4) & 15);            dest[4] = ((((long)src[20]) << 37) & 2199023255551) | ((((long)src[21]) << 29) & 137438953471) | ((((long)src[22]) << 21) & 536870911) | ((((long)src[23]) << 13) & 2097151) | ((((long)src[24]) << 5) & 8191) | ((((long)src[25]) >> 3) & 31);            dest[5] = ((((long)src[25]) << 38) & 2199023255551) | ((((long)src[26]) << 30) & 274877906943) | ((((long)src[27]) << 22) & 1073741823) | ((((long)src[28]) << 14) & 4194303) | ((((long)src[29]) << 6) & 16383) | ((((long)src[30]) >> 2) & 63);            dest[6] = ((((long)src[30]) << 39) & 2199023255551) | ((((long)src[31]) << 31) & 549755813887) | ((((long)src[32]) << 23) & 2147483647) | ((((long)src[33]) << 15) & 8388607) | ((((long)src[34]) << 7) & 32767) | ((((long)src[35]) >> 1) & 127);            dest[7] = ((((long)src[35]) << 40) & 2199023255551) | ((((long)src[36]) << 32) & 1099511627775) | ((((long)src[37]) << 24) & 4294967295) | ((((long)src[38]) << 16) & 16777215) | ((((long)src[39]) << 8) & 65535) | ((((long)src[40])) & 255);        }

        private static void Pack8LongValuesBE41(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2199023255551) >> 33)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2199023255551) >> 25)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2199023255551) >> 17)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2199023255551) >> 9)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 2199023255551) >> 1)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 2199023255551) << 7)                | ((src[1] & 2199023255551) >> 34)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 2199023255551) >> 26)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 2199023255551) >> 18)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 2199023255551) >> 10)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 2199023255551) >> 2)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 2199023255551) << 6)                | ((src[2] & 2199023255551) >> 35)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 2199023255551) >> 27)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 2199023255551) >> 19)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 2199023255551) >> 11)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 2199023255551) >> 3)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 2199023255551) << 5)                | ((src[3] & 2199023255551) >> 36)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 2199023255551) >> 28)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 2199023255551) >> 20)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 2199023255551) >> 12)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 2199023255551) >> 4)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 2199023255551) << 4)                | ((src[4] & 2199023255551) >> 37)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 2199023255551) >> 29)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 2199023255551) >> 21)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 2199023255551) >> 13)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 2199023255551) >> 5)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 2199023255551) << 3)                | ((src[5] & 2199023255551) >> 38)) & 255);
                            dest[26] = 
                (byte)((((src[5] & 2199023255551) >> 30)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 2199023255551) >> 22)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 2199023255551) >> 14)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 2199023255551) >> 6)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 2199023255551) << 2)                | ((src[6] & 2199023255551) >> 39)) & 255);
                            dest[31] = 
                (byte)((((src[6] & 2199023255551) >> 31)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 2199023255551) >> 23)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 2199023255551) >> 15)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 2199023255551) >> 7)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 2199023255551) << 1)                | ((src[7] & 2199023255551) >> 40)) & 255);
                            dest[36] = 
                (byte)((((src[7] & 2199023255551) >> 32)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 2199023255551) >> 24)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 2199023255551) >> 16)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 2199023255551) >> 8)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 2199023255551))) & 255);
                        }
        private static void Unpack8LongValuesLE42(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 4398046511103);            dest[1] = ((((long)src[5]) >> 2) & 63) | ((((long)src[6]) << 6) & 16383) | ((((long)src[7]) << 14) & 4194303) | ((((long)src[8]) << 22) & 1073741823) | ((((long)src[9]) << 30) & 274877906943) | ((((long)src[10]) << 38) & 4398046511103);            dest[2] = ((((long)src[10]) >> 4) & 15) | ((((long)src[11]) << 4) & 4095) | ((((long)src[12]) << 12) & 1048575) | ((((long)src[13]) << 20) & 268435455) | ((((long)src[14]) << 28) & 68719476735) | ((((long)src[15]) << 36) & 4398046511103);            dest[3] = ((((long)src[15]) >> 6) & 3) | ((((long)src[16]) << 2) & 1023) | ((((long)src[17]) << 10) & 262143) | ((((long)src[18]) << 18) & 67108863) | ((((long)src[19]) << 26) & 17179869183) | ((((long)src[20]) << 34) & 4398046511103);            dest[4] = ((((long)src[21])) & 255) | ((((long)src[22]) << 8) & 65535) | ((((long)src[23]) << 16) & 16777215) | ((((long)src[24]) << 24) & 4294967295) | ((((long)src[25]) << 32) & 1099511627775) | ((((long)src[26]) << 40) & 4398046511103);            dest[5] = ((((long)src[26]) >> 2) & 63) | ((((long)src[27]) << 6) & 16383) | ((((long)src[28]) << 14) & 4194303) | ((((long)src[29]) << 22) & 1073741823) | ((((long)src[30]) << 30) & 274877906943) | ((((long)src[31]) << 38) & 4398046511103);            dest[6] = ((((long)src[31]) >> 4) & 15) | ((((long)src[32]) << 4) & 4095) | ((((long)src[33]) << 12) & 1048575) | ((((long)src[34]) << 20) & 268435455) | ((((long)src[35]) << 28) & 68719476735) | ((((long)src[36]) << 36) & 4398046511103);            dest[7] = ((((long)src[36]) >> 6) & 3) | ((((long)src[37]) << 2) & 1023) | ((((long)src[38]) << 10) & 262143) | ((((long)src[39]) << 18) & 67108863) | ((((long)src[40]) << 26) & 17179869183) | ((((long)src[41]) << 34) & 4398046511103);        }

        private static void Pack8LongValuesLE42(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4398046511103))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4398046511103) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4398046511103) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4398046511103) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 4398046511103) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 4398046511103) >> 40)                | ((src[1] & 4398046511103) << 2)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 4398046511103) >> 6)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 4398046511103) >> 14)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 4398046511103) >> 22)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 4398046511103) >> 30)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 4398046511103) >> 38)                | ((src[2] & 4398046511103) << 4)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 4398046511103) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 4398046511103) >> 12)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 4398046511103) >> 20)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 4398046511103) >> 28)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 4398046511103) >> 36)                | ((src[3] & 4398046511103) << 6)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 4398046511103) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 4398046511103) >> 10)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 4398046511103) >> 18)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 4398046511103) >> 26)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 4398046511103) >> 34)) & 255);
                            dest[21] = 
                (byte)((((src[4] & 4398046511103))) & 255);
                            dest[22] = 
                (byte)((((src[4] & 4398046511103) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 4398046511103) >> 16)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 4398046511103) >> 24)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 4398046511103) >> 32)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 4398046511103) >> 40)                | ((src[5] & 4398046511103) << 2)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 4398046511103) >> 6)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 4398046511103) >> 14)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 4398046511103) >> 22)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 4398046511103) >> 30)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 4398046511103) >> 38)                | ((src[6] & 4398046511103) << 4)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 4398046511103) >> 4)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 4398046511103) >> 12)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 4398046511103) >> 20)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 4398046511103) >> 28)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 4398046511103) >> 36)                | ((src[7] & 4398046511103) << 6)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 4398046511103) >> 2)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 4398046511103) >> 10)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 4398046511103) >> 18)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 4398046511103) >> 26)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 4398046511103) >> 34)) & 255);
                        }
        private static void Unpack8LongValuesBE42(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 34) & 4398046511103) | ((((long)src[1]) << 26) & 17179869183) | ((((long)src[2]) << 18) & 67108863) | ((((long)src[3]) << 10) & 262143) | ((((long)src[4]) << 2) & 1023) | ((((long)src[5]) >> 6) & 3);            dest[1] = ((((long)src[5]) << 36) & 4398046511103) | ((((long)src[6]) << 28) & 68719476735) | ((((long)src[7]) << 20) & 268435455) | ((((long)src[8]) << 12) & 1048575) | ((((long)src[9]) << 4) & 4095) | ((((long)src[10]) >> 4) & 15);            dest[2] = ((((long)src[10]) << 38) & 4398046511103) | ((((long)src[11]) << 30) & 274877906943) | ((((long)src[12]) << 22) & 1073741823) | ((((long)src[13]) << 14) & 4194303) | ((((long)src[14]) << 6) & 16383) | ((((long)src[15]) >> 2) & 63);            dest[3] = ((((long)src[15]) << 40) & 4398046511103) | ((((long)src[16]) << 32) & 1099511627775) | ((((long)src[17]) << 24) & 4294967295) | ((((long)src[18]) << 16) & 16777215) | ((((long)src[19]) << 8) & 65535) | ((((long)src[20])) & 255);            dest[4] = ((((long)src[21]) << 34) & 4398046511103) | ((((long)src[22]) << 26) & 17179869183) | ((((long)src[23]) << 18) & 67108863) | ((((long)src[24]) << 10) & 262143) | ((((long)src[25]) << 2) & 1023) | ((((long)src[26]) >> 6) & 3);            dest[5] = ((((long)src[26]) << 36) & 4398046511103) | ((((long)src[27]) << 28) & 68719476735) | ((((long)src[28]) << 20) & 268435455) | ((((long)src[29]) << 12) & 1048575) | ((((long)src[30]) << 4) & 4095) | ((((long)src[31]) >> 4) & 15);            dest[6] = ((((long)src[31]) << 38) & 4398046511103) | ((((long)src[32]) << 30) & 274877906943) | ((((long)src[33]) << 22) & 1073741823) | ((((long)src[34]) << 14) & 4194303) | ((((long)src[35]) << 6) & 16383) | ((((long)src[36]) >> 2) & 63);            dest[7] = ((((long)src[36]) << 40) & 4398046511103) | ((((long)src[37]) << 32) & 1099511627775) | ((((long)src[38]) << 24) & 4294967295) | ((((long)src[39]) << 16) & 16777215) | ((((long)src[40]) << 8) & 65535) | ((((long)src[41])) & 255);        }

        private static void Pack8LongValuesBE42(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4398046511103) >> 34)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4398046511103) >> 26)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4398046511103) >> 18)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4398046511103) >> 10)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 4398046511103) >> 2)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 4398046511103) << 6)                | ((src[1] & 4398046511103) >> 36)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 4398046511103) >> 28)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 4398046511103) >> 20)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 4398046511103) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 4398046511103) >> 4)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 4398046511103) << 4)                | ((src[2] & 4398046511103) >> 38)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 4398046511103) >> 30)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 4398046511103) >> 22)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 4398046511103) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 4398046511103) >> 6)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 4398046511103) << 2)                | ((src[3] & 4398046511103) >> 40)) & 255);
                            dest[16] = 
                (byte)((((src[3] & 4398046511103) >> 32)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 4398046511103) >> 24)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 4398046511103) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 4398046511103) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 4398046511103))) & 255);
                            dest[21] = 
                (byte)((((src[4] & 4398046511103) >> 34)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 4398046511103) >> 26)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 4398046511103) >> 18)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 4398046511103) >> 10)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 4398046511103) >> 2)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 4398046511103) << 6)                | ((src[5] & 4398046511103) >> 36)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 4398046511103) >> 28)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 4398046511103) >> 20)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 4398046511103) >> 12)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 4398046511103) >> 4)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 4398046511103) << 4)                | ((src[6] & 4398046511103) >> 38)) & 255);
                            dest[32] = 
                (byte)((((src[6] & 4398046511103) >> 30)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 4398046511103) >> 22)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 4398046511103) >> 14)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 4398046511103) >> 6)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 4398046511103) << 2)                | ((src[7] & 4398046511103) >> 40)) & 255);
                            dest[37] = 
                (byte)((((src[7] & 4398046511103) >> 32)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 4398046511103) >> 24)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 4398046511103) >> 16)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 4398046511103) >> 8)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 4398046511103))) & 255);
                        }
        private static void Unpack8LongValuesLE43(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 8796093022207);            dest[1] = ((((long)src[5]) >> 3) & 31) | ((((long)src[6]) << 5) & 8191) | ((((long)src[7]) << 13) & 2097151) | ((((long)src[8]) << 21) & 536870911) | ((((long)src[9]) << 29) & 137438953471) | ((((long)src[10]) << 37) & 8796093022207);            dest[2] = ((((long)src[10]) >> 6) & 3) | ((((long)src[11]) << 2) & 1023) | ((((long)src[12]) << 10) & 262143) | ((((long)src[13]) << 18) & 67108863) | ((((long)src[14]) << 26) & 17179869183) | ((((long)src[15]) << 34) & 4398046511103) | ((((long)src[16]) << 42) & 8796093022207);            dest[3] = ((((long)src[16]) >> 1) & 127) | ((((long)src[17]) << 7) & 32767) | ((((long)src[18]) << 15) & 8388607) | ((((long)src[19]) << 23) & 2147483647) | ((((long)src[20]) << 31) & 549755813887) | ((((long)src[21]) << 39) & 8796093022207);            dest[4] = ((((long)src[21]) >> 4) & 15) | ((((long)src[22]) << 4) & 4095) | ((((long)src[23]) << 12) & 1048575) | ((((long)src[24]) << 20) & 268435455) | ((((long)src[25]) << 28) & 68719476735) | ((((long)src[26]) << 36) & 8796093022207);            dest[5] = ((((long)src[26]) >> 7) & 1) | ((((long)src[27]) << 1) & 511) | ((((long)src[28]) << 9) & 131071) | ((((long)src[29]) << 17) & 33554431) | ((((long)src[30]) << 25) & 8589934591) | ((((long)src[31]) << 33) & 2199023255551) | ((((long)src[32]) << 41) & 8796093022207);            dest[6] = ((((long)src[32]) >> 2) & 63) | ((((long)src[33]) << 6) & 16383) | ((((long)src[34]) << 14) & 4194303) | ((((long)src[35]) << 22) & 1073741823) | ((((long)src[36]) << 30) & 274877906943) | ((((long)src[37]) << 38) & 8796093022207);            dest[7] = ((((long)src[37]) >> 5) & 7) | ((((long)src[38]) << 3) & 2047) | ((((long)src[39]) << 11) & 524287) | ((((long)src[40]) << 19) & 134217727) | ((((long)src[41]) << 27) & 34359738367) | ((((long)src[42]) << 35) & 8796093022207);        }

        private static void Pack8LongValuesLE43(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8796093022207))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8796093022207) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 8796093022207) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 8796093022207) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 8796093022207) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 8796093022207) >> 40)                | ((src[1] & 8796093022207) << 3)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 8796093022207) >> 5)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 8796093022207) >> 13)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 8796093022207) >> 21)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 8796093022207) >> 29)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 8796093022207) >> 37)                | ((src[2] & 8796093022207) << 6)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 8796093022207) >> 2)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 8796093022207) >> 10)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 8796093022207) >> 18)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 8796093022207) >> 26)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 8796093022207) >> 34)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 8796093022207) >> 42)                | ((src[3] & 8796093022207) << 1)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 8796093022207) >> 7)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 8796093022207) >> 15)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 8796093022207) >> 23)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 8796093022207) >> 31)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 8796093022207) >> 39)                | ((src[4] & 8796093022207) << 4)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 8796093022207) >> 4)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 8796093022207) >> 12)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 8796093022207) >> 20)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 8796093022207) >> 28)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 8796093022207) >> 36)                | ((src[5] & 8796093022207) << 7)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 8796093022207) >> 1)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 8796093022207) >> 9)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 8796093022207) >> 17)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 8796093022207) >> 25)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 8796093022207) >> 33)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 8796093022207) >> 41)                | ((src[6] & 8796093022207) << 2)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 8796093022207) >> 6)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 8796093022207) >> 14)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 8796093022207) >> 22)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 8796093022207) >> 30)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 8796093022207) >> 38)                | ((src[7] & 8796093022207) << 5)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 8796093022207) >> 3)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 8796093022207) >> 11)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 8796093022207) >> 19)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 8796093022207) >> 27)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 8796093022207) >> 35)) & 255);
                        }
        private static void Unpack8LongValuesBE43(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 35) & 8796093022207) | ((((long)src[1]) << 27) & 34359738367) | ((((long)src[2]) << 19) & 134217727) | ((((long)src[3]) << 11) & 524287) | ((((long)src[4]) << 3) & 2047) | ((((long)src[5]) >> 5) & 7);            dest[1] = ((((long)src[5]) << 38) & 8796093022207) | ((((long)src[6]) << 30) & 274877906943) | ((((long)src[7]) << 22) & 1073741823) | ((((long)src[8]) << 14) & 4194303) | ((((long)src[9]) << 6) & 16383) | ((((long)src[10]) >> 2) & 63);            dest[2] = ((((long)src[10]) << 41) & 8796093022207) | ((((long)src[11]) << 33) & 2199023255551) | ((((long)src[12]) << 25) & 8589934591) | ((((long)src[13]) << 17) & 33554431) | ((((long)src[14]) << 9) & 131071) | ((((long)src[15]) << 1) & 511) | ((((long)src[16]) >> 7) & 1);            dest[3] = ((((long)src[16]) << 36) & 8796093022207) | ((((long)src[17]) << 28) & 68719476735) | ((((long)src[18]) << 20) & 268435455) | ((((long)src[19]) << 12) & 1048575) | ((((long)src[20]) << 4) & 4095) | ((((long)src[21]) >> 4) & 15);            dest[4] = ((((long)src[21]) << 39) & 8796093022207) | ((((long)src[22]) << 31) & 549755813887) | ((((long)src[23]) << 23) & 2147483647) | ((((long)src[24]) << 15) & 8388607) | ((((long)src[25]) << 7) & 32767) | ((((long)src[26]) >> 1) & 127);            dest[5] = ((((long)src[26]) << 42) & 8796093022207) | ((((long)src[27]) << 34) & 4398046511103) | ((((long)src[28]) << 26) & 17179869183) | ((((long)src[29]) << 18) & 67108863) | ((((long)src[30]) << 10) & 262143) | ((((long)src[31]) << 2) & 1023) | ((((long)src[32]) >> 6) & 3);            dest[6] = ((((long)src[32]) << 37) & 8796093022207) | ((((long)src[33]) << 29) & 137438953471) | ((((long)src[34]) << 21) & 536870911) | ((((long)src[35]) << 13) & 2097151) | ((((long)src[36]) << 5) & 8191) | ((((long)src[37]) >> 3) & 31);            dest[7] = ((((long)src[37]) << 40) & 8796093022207) | ((((long)src[38]) << 32) & 1099511627775) | ((((long)src[39]) << 24) & 4294967295) | ((((long)src[40]) << 16) & 16777215) | ((((long)src[41]) << 8) & 65535) | ((((long)src[42])) & 255);        }

        private static void Pack8LongValuesBE43(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 8796093022207) >> 35)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 8796093022207) >> 27)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 8796093022207) >> 19)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 8796093022207) >> 11)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 8796093022207) >> 3)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 8796093022207) << 5)                | ((src[1] & 8796093022207) >> 38)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 8796093022207) >> 30)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 8796093022207) >> 22)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 8796093022207) >> 14)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 8796093022207) >> 6)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 8796093022207) << 2)                | ((src[2] & 8796093022207) >> 41)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 8796093022207) >> 33)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 8796093022207) >> 25)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 8796093022207) >> 17)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 8796093022207) >> 9)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 8796093022207) >> 1)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 8796093022207) << 7)                | ((src[3] & 8796093022207) >> 36)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 8796093022207) >> 28)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 8796093022207) >> 20)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 8796093022207) >> 12)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 8796093022207) >> 4)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 8796093022207) << 4)                | ((src[4] & 8796093022207) >> 39)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 8796093022207) >> 31)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 8796093022207) >> 23)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 8796093022207) >> 15)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 8796093022207) >> 7)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 8796093022207) << 1)                | ((src[5] & 8796093022207) >> 42)) & 255);
                            dest[27] = 
                (byte)((((src[5] & 8796093022207) >> 34)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 8796093022207) >> 26)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 8796093022207) >> 18)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 8796093022207) >> 10)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 8796093022207) >> 2)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 8796093022207) << 6)                | ((src[6] & 8796093022207) >> 37)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 8796093022207) >> 29)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 8796093022207) >> 21)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 8796093022207) >> 13)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 8796093022207) >> 5)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 8796093022207) << 3)                | ((src[7] & 8796093022207) >> 40)) & 255);
                            dest[38] = 
                (byte)((((src[7] & 8796093022207) >> 32)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 8796093022207) >> 24)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 8796093022207) >> 16)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 8796093022207) >> 8)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 8796093022207))) & 255);
                        }
        private static void Unpack8LongValuesLE44(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 17592186044415);            dest[1] = ((((long)src[5]) >> 4) & 15) | ((((long)src[6]) << 4) & 4095) | ((((long)src[7]) << 12) & 1048575) | ((((long)src[8]) << 20) & 268435455) | ((((long)src[9]) << 28) & 68719476735) | ((((long)src[10]) << 36) & 17592186044415);            dest[2] = ((((long)src[11])) & 255) | ((((long)src[12]) << 8) & 65535) | ((((long)src[13]) << 16) & 16777215) | ((((long)src[14]) << 24) & 4294967295) | ((((long)src[15]) << 32) & 1099511627775) | ((((long)src[16]) << 40) & 17592186044415);            dest[3] = ((((long)src[16]) >> 4) & 15) | ((((long)src[17]) << 4) & 4095) | ((((long)src[18]) << 12) & 1048575) | ((((long)src[19]) << 20) & 268435455) | ((((long)src[20]) << 28) & 68719476735) | ((((long)src[21]) << 36) & 17592186044415);            dest[4] = ((((long)src[22])) & 255) | ((((long)src[23]) << 8) & 65535) | ((((long)src[24]) << 16) & 16777215) | ((((long)src[25]) << 24) & 4294967295) | ((((long)src[26]) << 32) & 1099511627775) | ((((long)src[27]) << 40) & 17592186044415);            dest[5] = ((((long)src[27]) >> 4) & 15) | ((((long)src[28]) << 4) & 4095) | ((((long)src[29]) << 12) & 1048575) | ((((long)src[30]) << 20) & 268435455) | ((((long)src[31]) << 28) & 68719476735) | ((((long)src[32]) << 36) & 17592186044415);            dest[6] = ((((long)src[33])) & 255) | ((((long)src[34]) << 8) & 65535) | ((((long)src[35]) << 16) & 16777215) | ((((long)src[36]) << 24) & 4294967295) | ((((long)src[37]) << 32) & 1099511627775) | ((((long)src[38]) << 40) & 17592186044415);            dest[7] = ((((long)src[38]) >> 4) & 15) | ((((long)src[39]) << 4) & 4095) | ((((long)src[40]) << 12) & 1048575) | ((((long)src[41]) << 20) & 268435455) | ((((long)src[42]) << 28) & 68719476735) | ((((long)src[43]) << 36) & 17592186044415);        }

        private static void Pack8LongValuesLE44(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 17592186044415))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 17592186044415) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 17592186044415) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 17592186044415) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 17592186044415) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 17592186044415) >> 40)                | ((src[1] & 17592186044415) << 4)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 17592186044415) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 17592186044415) >> 12)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 17592186044415) >> 20)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 17592186044415) >> 28)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 17592186044415) >> 36)) & 255);
                            dest[11] = 
                (byte)((((src[2] & 17592186044415))) & 255);
                            dest[12] = 
                (byte)((((src[2] & 17592186044415) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 17592186044415) >> 16)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 17592186044415) >> 24)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 17592186044415) >> 32)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 17592186044415) >> 40)                | ((src[3] & 17592186044415) << 4)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 17592186044415) >> 4)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 17592186044415) >> 12)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 17592186044415) >> 20)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 17592186044415) >> 28)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 17592186044415) >> 36)) & 255);
                            dest[22] = 
                (byte)((((src[4] & 17592186044415))) & 255);
                            dest[23] = 
                (byte)((((src[4] & 17592186044415) >> 8)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 17592186044415) >> 16)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 17592186044415) >> 24)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 17592186044415) >> 32)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 17592186044415) >> 40)                | ((src[5] & 17592186044415) << 4)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 17592186044415) >> 4)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 17592186044415) >> 12)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 17592186044415) >> 20)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 17592186044415) >> 28)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 17592186044415) >> 36)) & 255);
                            dest[33] = 
                (byte)((((src[6] & 17592186044415))) & 255);
                            dest[34] = 
                (byte)((((src[6] & 17592186044415) >> 8)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 17592186044415) >> 16)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 17592186044415) >> 24)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 17592186044415) >> 32)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 17592186044415) >> 40)                | ((src[7] & 17592186044415) << 4)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 17592186044415) >> 4)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 17592186044415) >> 12)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 17592186044415) >> 20)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 17592186044415) >> 28)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 17592186044415) >> 36)) & 255);
                        }
        private static void Unpack8LongValuesBE44(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 36) & 17592186044415) | ((((long)src[1]) << 28) & 68719476735) | ((((long)src[2]) << 20) & 268435455) | ((((long)src[3]) << 12) & 1048575) | ((((long)src[4]) << 4) & 4095) | ((((long)src[5]) >> 4) & 15);            dest[1] = ((((long)src[5]) << 40) & 17592186044415) | ((((long)src[6]) << 32) & 1099511627775) | ((((long)src[7]) << 24) & 4294967295) | ((((long)src[8]) << 16) & 16777215) | ((((long)src[9]) << 8) & 65535) | ((((long)src[10])) & 255);            dest[2] = ((((long)src[11]) << 36) & 17592186044415) | ((((long)src[12]) << 28) & 68719476735) | ((((long)src[13]) << 20) & 268435455) | ((((long)src[14]) << 12) & 1048575) | ((((long)src[15]) << 4) & 4095) | ((((long)src[16]) >> 4) & 15);            dest[3] = ((((long)src[16]) << 40) & 17592186044415) | ((((long)src[17]) << 32) & 1099511627775) | ((((long)src[18]) << 24) & 4294967295) | ((((long)src[19]) << 16) & 16777215) | ((((long)src[20]) << 8) & 65535) | ((((long)src[21])) & 255);            dest[4] = ((((long)src[22]) << 36) & 17592186044415) | ((((long)src[23]) << 28) & 68719476735) | ((((long)src[24]) << 20) & 268435455) | ((((long)src[25]) << 12) & 1048575) | ((((long)src[26]) << 4) & 4095) | ((((long)src[27]) >> 4) & 15);            dest[5] = ((((long)src[27]) << 40) & 17592186044415) | ((((long)src[28]) << 32) & 1099511627775) | ((((long)src[29]) << 24) & 4294967295) | ((((long)src[30]) << 16) & 16777215) | ((((long)src[31]) << 8) & 65535) | ((((long)src[32])) & 255);            dest[6] = ((((long)src[33]) << 36) & 17592186044415) | ((((long)src[34]) << 28) & 68719476735) | ((((long)src[35]) << 20) & 268435455) | ((((long)src[36]) << 12) & 1048575) | ((((long)src[37]) << 4) & 4095) | ((((long)src[38]) >> 4) & 15);            dest[7] = ((((long)src[38]) << 40) & 17592186044415) | ((((long)src[39]) << 32) & 1099511627775) | ((((long)src[40]) << 24) & 4294967295) | ((((long)src[41]) << 16) & 16777215) | ((((long)src[42]) << 8) & 65535) | ((((long)src[43])) & 255);        }

        private static void Pack8LongValuesBE44(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 17592186044415) >> 36)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 17592186044415) >> 28)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 17592186044415) >> 20)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 17592186044415) >> 12)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 17592186044415) >> 4)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 17592186044415) << 4)                | ((src[1] & 17592186044415) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 17592186044415) >> 32)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 17592186044415) >> 24)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 17592186044415) >> 16)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 17592186044415) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 17592186044415))) & 255);
                            dest[11] = 
                (byte)((((src[2] & 17592186044415) >> 36)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 17592186044415) >> 28)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 17592186044415) >> 20)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 17592186044415) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 17592186044415) >> 4)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 17592186044415) << 4)                | ((src[3] & 17592186044415) >> 40)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 17592186044415) >> 32)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 17592186044415) >> 24)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 17592186044415) >> 16)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 17592186044415) >> 8)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 17592186044415))) & 255);
                            dest[22] = 
                (byte)((((src[4] & 17592186044415) >> 36)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 17592186044415) >> 28)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 17592186044415) >> 20)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 17592186044415) >> 12)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 17592186044415) >> 4)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 17592186044415) << 4)                | ((src[5] & 17592186044415) >> 40)) & 255);
                            dest[28] = 
                (byte)((((src[5] & 17592186044415) >> 32)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 17592186044415) >> 24)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 17592186044415) >> 16)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 17592186044415) >> 8)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 17592186044415))) & 255);
                            dest[33] = 
                (byte)((((src[6] & 17592186044415) >> 36)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 17592186044415) >> 28)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 17592186044415) >> 20)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 17592186044415) >> 12)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 17592186044415) >> 4)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 17592186044415) << 4)                | ((src[7] & 17592186044415) >> 40)) & 255);
                            dest[39] = 
                (byte)((((src[7] & 17592186044415) >> 32)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 17592186044415) >> 24)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 17592186044415) >> 16)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 17592186044415) >> 8)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 17592186044415))) & 255);
                        }
        private static void Unpack8LongValuesLE45(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 35184372088831);            dest[1] = ((((long)src[5]) >> 5) & 7) | ((((long)src[6]) << 3) & 2047) | ((((long)src[7]) << 11) & 524287) | ((((long)src[8]) << 19) & 134217727) | ((((long)src[9]) << 27) & 34359738367) | ((((long)src[10]) << 35) & 8796093022207) | ((((long)src[11]) << 43) & 35184372088831);            dest[2] = ((((long)src[11]) >> 2) & 63) | ((((long)src[12]) << 6) & 16383) | ((((long)src[13]) << 14) & 4194303) | ((((long)src[14]) << 22) & 1073741823) | ((((long)src[15]) << 30) & 274877906943) | ((((long)src[16]) << 38) & 35184372088831);            dest[3] = ((((long)src[16]) >> 7) & 1) | ((((long)src[17]) << 1) & 511) | ((((long)src[18]) << 9) & 131071) | ((((long)src[19]) << 17) & 33554431) | ((((long)src[20]) << 25) & 8589934591) | ((((long)src[21]) << 33) & 2199023255551) | ((((long)src[22]) << 41) & 35184372088831);            dest[4] = ((((long)src[22]) >> 4) & 15) | ((((long)src[23]) << 4) & 4095) | ((((long)src[24]) << 12) & 1048575) | ((((long)src[25]) << 20) & 268435455) | ((((long)src[26]) << 28) & 68719476735) | ((((long)src[27]) << 36) & 17592186044415) | ((((long)src[28]) << 44) & 35184372088831);            dest[5] = ((((long)src[28]) >> 1) & 127) | ((((long)src[29]) << 7) & 32767) | ((((long)src[30]) << 15) & 8388607) | ((((long)src[31]) << 23) & 2147483647) | ((((long)src[32]) << 31) & 549755813887) | ((((long)src[33]) << 39) & 35184372088831);            dest[6] = ((((long)src[33]) >> 6) & 3) | ((((long)src[34]) << 2) & 1023) | ((((long)src[35]) << 10) & 262143) | ((((long)src[36]) << 18) & 67108863) | ((((long)src[37]) << 26) & 17179869183) | ((((long)src[38]) << 34) & 4398046511103) | ((((long)src[39]) << 42) & 35184372088831);            dest[7] = ((((long)src[39]) >> 3) & 31) | ((((long)src[40]) << 5) & 8191) | ((((long)src[41]) << 13) & 2097151) | ((((long)src[42]) << 21) & 536870911) | ((((long)src[43]) << 29) & 137438953471) | ((((long)src[44]) << 37) & 35184372088831);        }

        private static void Pack8LongValuesLE45(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 35184372088831))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 35184372088831) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 35184372088831) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 35184372088831) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 35184372088831) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 35184372088831) >> 40)                | ((src[1] & 35184372088831) << 5)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 35184372088831) >> 3)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 35184372088831) >> 11)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 35184372088831) >> 19)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 35184372088831) >> 27)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 35184372088831) >> 35)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 35184372088831) >> 43)                | ((src[2] & 35184372088831) << 2)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 35184372088831) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 35184372088831) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 35184372088831) >> 22)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 35184372088831) >> 30)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 35184372088831) >> 38)                | ((src[3] & 35184372088831) << 7)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 35184372088831) >> 1)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 35184372088831) >> 9)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 35184372088831) >> 17)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 35184372088831) >> 25)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 35184372088831) >> 33)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 35184372088831) >> 41)                | ((src[4] & 35184372088831) << 4)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 35184372088831) >> 4)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 35184372088831) >> 12)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 35184372088831) >> 20)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 35184372088831) >> 28)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 35184372088831) >> 36)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 35184372088831) >> 44)                | ((src[5] & 35184372088831) << 1)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 35184372088831) >> 7)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 35184372088831) >> 15)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 35184372088831) >> 23)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 35184372088831) >> 31)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 35184372088831) >> 39)                | ((src[6] & 35184372088831) << 6)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 35184372088831) >> 2)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 35184372088831) >> 10)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 35184372088831) >> 18)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 35184372088831) >> 26)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 35184372088831) >> 34)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 35184372088831) >> 42)                | ((src[7] & 35184372088831) << 3)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 35184372088831) >> 5)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 35184372088831) >> 13)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 35184372088831) >> 21)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 35184372088831) >> 29)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 35184372088831) >> 37)) & 255);
                        }
        private static void Unpack8LongValuesBE45(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 37) & 35184372088831) | ((((long)src[1]) << 29) & 137438953471) | ((((long)src[2]) << 21) & 536870911) | ((((long)src[3]) << 13) & 2097151) | ((((long)src[4]) << 5) & 8191) | ((((long)src[5]) >> 3) & 31);            dest[1] = ((((long)src[5]) << 42) & 35184372088831) | ((((long)src[6]) << 34) & 4398046511103) | ((((long)src[7]) << 26) & 17179869183) | ((((long)src[8]) << 18) & 67108863) | ((((long)src[9]) << 10) & 262143) | ((((long)src[10]) << 2) & 1023) | ((((long)src[11]) >> 6) & 3);            dest[2] = ((((long)src[11]) << 39) & 35184372088831) | ((((long)src[12]) << 31) & 549755813887) | ((((long)src[13]) << 23) & 2147483647) | ((((long)src[14]) << 15) & 8388607) | ((((long)src[15]) << 7) & 32767) | ((((long)src[16]) >> 1) & 127);            dest[3] = ((((long)src[16]) << 44) & 35184372088831) | ((((long)src[17]) << 36) & 17592186044415) | ((((long)src[18]) << 28) & 68719476735) | ((((long)src[19]) << 20) & 268435455) | ((((long)src[20]) << 12) & 1048575) | ((((long)src[21]) << 4) & 4095) | ((((long)src[22]) >> 4) & 15);            dest[4] = ((((long)src[22]) << 41) & 35184372088831) | ((((long)src[23]) << 33) & 2199023255551) | ((((long)src[24]) << 25) & 8589934591) | ((((long)src[25]) << 17) & 33554431) | ((((long)src[26]) << 9) & 131071) | ((((long)src[27]) << 1) & 511) | ((((long)src[28]) >> 7) & 1);            dest[5] = ((((long)src[28]) << 38) & 35184372088831) | ((((long)src[29]) << 30) & 274877906943) | ((((long)src[30]) << 22) & 1073741823) | ((((long)src[31]) << 14) & 4194303) | ((((long)src[32]) << 6) & 16383) | ((((long)src[33]) >> 2) & 63);            dest[6] = ((((long)src[33]) << 43) & 35184372088831) | ((((long)src[34]) << 35) & 8796093022207) | ((((long)src[35]) << 27) & 34359738367) | ((((long)src[36]) << 19) & 134217727) | ((((long)src[37]) << 11) & 524287) | ((((long)src[38]) << 3) & 2047) | ((((long)src[39]) >> 5) & 7);            dest[7] = ((((long)src[39]) << 40) & 35184372088831) | ((((long)src[40]) << 32) & 1099511627775) | ((((long)src[41]) << 24) & 4294967295) | ((((long)src[42]) << 16) & 16777215) | ((((long)src[43]) << 8) & 65535) | ((((long)src[44])) & 255);        }

        private static void Pack8LongValuesBE45(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 35184372088831) >> 37)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 35184372088831) >> 29)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 35184372088831) >> 21)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 35184372088831) >> 13)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 35184372088831) >> 5)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 35184372088831) << 3)                | ((src[1] & 35184372088831) >> 42)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 35184372088831) >> 34)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 35184372088831) >> 26)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 35184372088831) >> 18)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 35184372088831) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 35184372088831) >> 2)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 35184372088831) << 6)                | ((src[2] & 35184372088831) >> 39)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 35184372088831) >> 31)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 35184372088831) >> 23)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 35184372088831) >> 15)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 35184372088831) >> 7)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 35184372088831) << 1)                | ((src[3] & 35184372088831) >> 44)) & 255);
                            dest[17] = 
                (byte)((((src[3] & 35184372088831) >> 36)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 35184372088831) >> 28)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 35184372088831) >> 20)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 35184372088831) >> 12)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 35184372088831) >> 4)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 35184372088831) << 4)                | ((src[4] & 35184372088831) >> 41)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 35184372088831) >> 33)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 35184372088831) >> 25)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 35184372088831) >> 17)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 35184372088831) >> 9)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 35184372088831) >> 1)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 35184372088831) << 7)                | ((src[5] & 35184372088831) >> 38)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 35184372088831) >> 30)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 35184372088831) >> 22)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 35184372088831) >> 14)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 35184372088831) >> 6)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 35184372088831) << 2)                | ((src[6] & 35184372088831) >> 43)) & 255);
                            dest[34] = 
                (byte)((((src[6] & 35184372088831) >> 35)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 35184372088831) >> 27)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 35184372088831) >> 19)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 35184372088831) >> 11)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 35184372088831) >> 3)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 35184372088831) << 5)                | ((src[7] & 35184372088831) >> 40)) & 255);
                            dest[40] = 
                (byte)((((src[7] & 35184372088831) >> 32)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 35184372088831) >> 24)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 35184372088831) >> 16)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 35184372088831) >> 8)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 35184372088831))) & 255);
                        }
        private static void Unpack8LongValuesLE46(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 70368744177663);            dest[1] = ((((long)src[5]) >> 6) & 3) | ((((long)src[6]) << 2) & 1023) | ((((long)src[7]) << 10) & 262143) | ((((long)src[8]) << 18) & 67108863) | ((((long)src[9]) << 26) & 17179869183) | ((((long)src[10]) << 34) & 4398046511103) | ((((long)src[11]) << 42) & 70368744177663);            dest[2] = ((((long)src[11]) >> 4) & 15) | ((((long)src[12]) << 4) & 4095) | ((((long)src[13]) << 12) & 1048575) | ((((long)src[14]) << 20) & 268435455) | ((((long)src[15]) << 28) & 68719476735) | ((((long)src[16]) << 36) & 17592186044415) | ((((long)src[17]) << 44) & 70368744177663);            dest[3] = ((((long)src[17]) >> 2) & 63) | ((((long)src[18]) << 6) & 16383) | ((((long)src[19]) << 14) & 4194303) | ((((long)src[20]) << 22) & 1073741823) | ((((long)src[21]) << 30) & 274877906943) | ((((long)src[22]) << 38) & 70368744177663);            dest[4] = ((((long)src[23])) & 255) | ((((long)src[24]) << 8) & 65535) | ((((long)src[25]) << 16) & 16777215) | ((((long)src[26]) << 24) & 4294967295) | ((((long)src[27]) << 32) & 1099511627775) | ((((long)src[28]) << 40) & 70368744177663);            dest[5] = ((((long)src[28]) >> 6) & 3) | ((((long)src[29]) << 2) & 1023) | ((((long)src[30]) << 10) & 262143) | ((((long)src[31]) << 18) & 67108863) | ((((long)src[32]) << 26) & 17179869183) | ((((long)src[33]) << 34) & 4398046511103) | ((((long)src[34]) << 42) & 70368744177663);            dest[6] = ((((long)src[34]) >> 4) & 15) | ((((long)src[35]) << 4) & 4095) | ((((long)src[36]) << 12) & 1048575) | ((((long)src[37]) << 20) & 268435455) | ((((long)src[38]) << 28) & 68719476735) | ((((long)src[39]) << 36) & 17592186044415) | ((((long)src[40]) << 44) & 70368744177663);            dest[7] = ((((long)src[40]) >> 2) & 63) | ((((long)src[41]) << 6) & 16383) | ((((long)src[42]) << 14) & 4194303) | ((((long)src[43]) << 22) & 1073741823) | ((((long)src[44]) << 30) & 274877906943) | ((((long)src[45]) << 38) & 70368744177663);        }

        private static void Pack8LongValuesLE46(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 70368744177663))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 70368744177663) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 70368744177663) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 70368744177663) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 70368744177663) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 70368744177663) >> 40)                | ((src[1] & 70368744177663) << 6)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 70368744177663) >> 2)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 70368744177663) >> 10)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 70368744177663) >> 18)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 70368744177663) >> 26)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 70368744177663) >> 34)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 70368744177663) >> 42)                | ((src[2] & 70368744177663) << 4)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 70368744177663) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 70368744177663) >> 12)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 70368744177663) >> 20)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 70368744177663) >> 28)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 70368744177663) >> 36)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 70368744177663) >> 44)                | ((src[3] & 70368744177663) << 2)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 70368744177663) >> 6)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 70368744177663) >> 14)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 70368744177663) >> 22)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 70368744177663) >> 30)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 70368744177663) >> 38)) & 255);
                            dest[23] = 
                (byte)((((src[4] & 70368744177663))) & 255);
                            dest[24] = 
                (byte)((((src[4] & 70368744177663) >> 8)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 70368744177663) >> 16)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 70368744177663) >> 24)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 70368744177663) >> 32)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 70368744177663) >> 40)                | ((src[5] & 70368744177663) << 6)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 70368744177663) >> 2)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 70368744177663) >> 10)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 70368744177663) >> 18)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 70368744177663) >> 26)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 70368744177663) >> 34)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 70368744177663) >> 42)                | ((src[6] & 70368744177663) << 4)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 70368744177663) >> 4)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 70368744177663) >> 12)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 70368744177663) >> 20)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 70368744177663) >> 28)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 70368744177663) >> 36)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 70368744177663) >> 44)                | ((src[7] & 70368744177663) << 2)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 70368744177663) >> 6)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 70368744177663) >> 14)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 70368744177663) >> 22)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 70368744177663) >> 30)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 70368744177663) >> 38)) & 255);
                        }
        private static void Unpack8LongValuesBE46(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 38) & 70368744177663) | ((((long)src[1]) << 30) & 274877906943) | ((((long)src[2]) << 22) & 1073741823) | ((((long)src[3]) << 14) & 4194303) | ((((long)src[4]) << 6) & 16383) | ((((long)src[5]) >> 2) & 63);            dest[1] = ((((long)src[5]) << 44) & 70368744177663) | ((((long)src[6]) << 36) & 17592186044415) | ((((long)src[7]) << 28) & 68719476735) | ((((long)src[8]) << 20) & 268435455) | ((((long)src[9]) << 12) & 1048575) | ((((long)src[10]) << 4) & 4095) | ((((long)src[11]) >> 4) & 15);            dest[2] = ((((long)src[11]) << 42) & 70368744177663) | ((((long)src[12]) << 34) & 4398046511103) | ((((long)src[13]) << 26) & 17179869183) | ((((long)src[14]) << 18) & 67108863) | ((((long)src[15]) << 10) & 262143) | ((((long)src[16]) << 2) & 1023) | ((((long)src[17]) >> 6) & 3);            dest[3] = ((((long)src[17]) << 40) & 70368744177663) | ((((long)src[18]) << 32) & 1099511627775) | ((((long)src[19]) << 24) & 4294967295) | ((((long)src[20]) << 16) & 16777215) | ((((long)src[21]) << 8) & 65535) | ((((long)src[22])) & 255);            dest[4] = ((((long)src[23]) << 38) & 70368744177663) | ((((long)src[24]) << 30) & 274877906943) | ((((long)src[25]) << 22) & 1073741823) | ((((long)src[26]) << 14) & 4194303) | ((((long)src[27]) << 6) & 16383) | ((((long)src[28]) >> 2) & 63);            dest[5] = ((((long)src[28]) << 44) & 70368744177663) | ((((long)src[29]) << 36) & 17592186044415) | ((((long)src[30]) << 28) & 68719476735) | ((((long)src[31]) << 20) & 268435455) | ((((long)src[32]) << 12) & 1048575) | ((((long)src[33]) << 4) & 4095) | ((((long)src[34]) >> 4) & 15);            dest[6] = ((((long)src[34]) << 42) & 70368744177663) | ((((long)src[35]) << 34) & 4398046511103) | ((((long)src[36]) << 26) & 17179869183) | ((((long)src[37]) << 18) & 67108863) | ((((long)src[38]) << 10) & 262143) | ((((long)src[39]) << 2) & 1023) | ((((long)src[40]) >> 6) & 3);            dest[7] = ((((long)src[40]) << 40) & 70368744177663) | ((((long)src[41]) << 32) & 1099511627775) | ((((long)src[42]) << 24) & 4294967295) | ((((long)src[43]) << 16) & 16777215) | ((((long)src[44]) << 8) & 65535) | ((((long)src[45])) & 255);        }

        private static void Pack8LongValuesBE46(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 70368744177663) >> 38)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 70368744177663) >> 30)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 70368744177663) >> 22)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 70368744177663) >> 14)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 70368744177663) >> 6)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 70368744177663) << 2)                | ((src[1] & 70368744177663) >> 44)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 70368744177663) >> 36)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 70368744177663) >> 28)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 70368744177663) >> 20)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 70368744177663) >> 12)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 70368744177663) >> 4)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 70368744177663) << 4)                | ((src[2] & 70368744177663) >> 42)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 70368744177663) >> 34)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 70368744177663) >> 26)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 70368744177663) >> 18)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 70368744177663) >> 10)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 70368744177663) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 70368744177663) << 6)                | ((src[3] & 70368744177663) >> 40)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 70368744177663) >> 32)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 70368744177663) >> 24)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 70368744177663) >> 16)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 70368744177663) >> 8)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 70368744177663))) & 255);
                            dest[23] = 
                (byte)((((src[4] & 70368744177663) >> 38)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 70368744177663) >> 30)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 70368744177663) >> 22)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 70368744177663) >> 14)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 70368744177663) >> 6)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 70368744177663) << 2)                | ((src[5] & 70368744177663) >> 44)) & 255);
                            dest[29] = 
                (byte)((((src[5] & 70368744177663) >> 36)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 70368744177663) >> 28)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 70368744177663) >> 20)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 70368744177663) >> 12)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 70368744177663) >> 4)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 70368744177663) << 4)                | ((src[6] & 70368744177663) >> 42)) & 255);
                            dest[35] = 
                (byte)((((src[6] & 70368744177663) >> 34)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 70368744177663) >> 26)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 70368744177663) >> 18)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 70368744177663) >> 10)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 70368744177663) >> 2)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 70368744177663) << 6)                | ((src[7] & 70368744177663) >> 40)) & 255);
                            dest[41] = 
                (byte)((((src[7] & 70368744177663) >> 32)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 70368744177663) >> 24)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 70368744177663) >> 16)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 70368744177663) >> 8)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 70368744177663))) & 255);
                        }
        private static void Unpack8LongValuesLE47(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 140737488355327);            dest[1] = ((((long)src[5]) >> 7) & 1) | ((((long)src[6]) << 1) & 511) | ((((long)src[7]) << 9) & 131071) | ((((long)src[8]) << 17) & 33554431) | ((((long)src[9]) << 25) & 8589934591) | ((((long)src[10]) << 33) & 2199023255551) | ((((long)src[11]) << 41) & 140737488355327);            dest[2] = ((((long)src[11]) >> 6) & 3) | ((((long)src[12]) << 2) & 1023) | ((((long)src[13]) << 10) & 262143) | ((((long)src[14]) << 18) & 67108863) | ((((long)src[15]) << 26) & 17179869183) | ((((long)src[16]) << 34) & 4398046511103) | ((((long)src[17]) << 42) & 140737488355327);            dest[3] = ((((long)src[17]) >> 5) & 7) | ((((long)src[18]) << 3) & 2047) | ((((long)src[19]) << 11) & 524287) | ((((long)src[20]) << 19) & 134217727) | ((((long)src[21]) << 27) & 34359738367) | ((((long)src[22]) << 35) & 8796093022207) | ((((long)src[23]) << 43) & 140737488355327);            dest[4] = ((((long)src[23]) >> 4) & 15) | ((((long)src[24]) << 4) & 4095) | ((((long)src[25]) << 12) & 1048575) | ((((long)src[26]) << 20) & 268435455) | ((((long)src[27]) << 28) & 68719476735) | ((((long)src[28]) << 36) & 17592186044415) | ((((long)src[29]) << 44) & 140737488355327);            dest[5] = ((((long)src[29]) >> 3) & 31) | ((((long)src[30]) << 5) & 8191) | ((((long)src[31]) << 13) & 2097151) | ((((long)src[32]) << 21) & 536870911) | ((((long)src[33]) << 29) & 137438953471) | ((((long)src[34]) << 37) & 35184372088831) | ((((long)src[35]) << 45) & 140737488355327);            dest[6] = ((((long)src[35]) >> 2) & 63) | ((((long)src[36]) << 6) & 16383) | ((((long)src[37]) << 14) & 4194303) | ((((long)src[38]) << 22) & 1073741823) | ((((long)src[39]) << 30) & 274877906943) | ((((long)src[40]) << 38) & 70368744177663) | ((((long)src[41]) << 46) & 140737488355327);            dest[7] = ((((long)src[41]) >> 1) & 127) | ((((long)src[42]) << 7) & 32767) | ((((long)src[43]) << 15) & 8388607) | ((((long)src[44]) << 23) & 2147483647) | ((((long)src[45]) << 31) & 549755813887) | ((((long)src[46]) << 39) & 140737488355327);        }

        private static void Pack8LongValuesLE47(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 140737488355327))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 140737488355327) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 140737488355327) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 140737488355327) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 140737488355327) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 140737488355327) >> 40)                | ((src[1] & 140737488355327) << 7)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 140737488355327) >> 1)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 140737488355327) >> 9)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 140737488355327) >> 17)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 140737488355327) >> 25)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 140737488355327) >> 33)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 140737488355327) >> 41)                | ((src[2] & 140737488355327) << 6)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 140737488355327) >> 2)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 140737488355327) >> 10)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 140737488355327) >> 18)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 140737488355327) >> 26)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 140737488355327) >> 34)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 140737488355327) >> 42)                | ((src[3] & 140737488355327) << 5)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 140737488355327) >> 3)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 140737488355327) >> 11)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 140737488355327) >> 19)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 140737488355327) >> 27)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 140737488355327) >> 35)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 140737488355327) >> 43)                | ((src[4] & 140737488355327) << 4)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 140737488355327) >> 4)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 140737488355327) >> 12)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 140737488355327) >> 20)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 140737488355327) >> 28)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 140737488355327) >> 36)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 140737488355327) >> 44)                | ((src[5] & 140737488355327) << 3)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 140737488355327) >> 5)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 140737488355327) >> 13)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 140737488355327) >> 21)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 140737488355327) >> 29)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 140737488355327) >> 37)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 140737488355327) >> 45)                | ((src[6] & 140737488355327) << 2)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 140737488355327) >> 6)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 140737488355327) >> 14)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 140737488355327) >> 22)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 140737488355327) >> 30)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 140737488355327) >> 38)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 140737488355327) >> 46)                | ((src[7] & 140737488355327) << 1)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 140737488355327) >> 7)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 140737488355327) >> 15)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 140737488355327) >> 23)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 140737488355327) >> 31)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 140737488355327) >> 39)) & 255);
                        }
        private static void Unpack8LongValuesBE47(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 39) & 140737488355327) | ((((long)src[1]) << 31) & 549755813887) | ((((long)src[2]) << 23) & 2147483647) | ((((long)src[3]) << 15) & 8388607) | ((((long)src[4]) << 7) & 32767) | ((((long)src[5]) >> 1) & 127);            dest[1] = ((((long)src[5]) << 46) & 140737488355327) | ((((long)src[6]) << 38) & 70368744177663) | ((((long)src[7]) << 30) & 274877906943) | ((((long)src[8]) << 22) & 1073741823) | ((((long)src[9]) << 14) & 4194303) | ((((long)src[10]) << 6) & 16383) | ((((long)src[11]) >> 2) & 63);            dest[2] = ((((long)src[11]) << 45) & 140737488355327) | ((((long)src[12]) << 37) & 35184372088831) | ((((long)src[13]) << 29) & 137438953471) | ((((long)src[14]) << 21) & 536870911) | ((((long)src[15]) << 13) & 2097151) | ((((long)src[16]) << 5) & 8191) | ((((long)src[17]) >> 3) & 31);            dest[3] = ((((long)src[17]) << 44) & 140737488355327) | ((((long)src[18]) << 36) & 17592186044415) | ((((long)src[19]) << 28) & 68719476735) | ((((long)src[20]) << 20) & 268435455) | ((((long)src[21]) << 12) & 1048575) | ((((long)src[22]) << 4) & 4095) | ((((long)src[23]) >> 4) & 15);            dest[4] = ((((long)src[23]) << 43) & 140737488355327) | ((((long)src[24]) << 35) & 8796093022207) | ((((long)src[25]) << 27) & 34359738367) | ((((long)src[26]) << 19) & 134217727) | ((((long)src[27]) << 11) & 524287) | ((((long)src[28]) << 3) & 2047) | ((((long)src[29]) >> 5) & 7);            dest[5] = ((((long)src[29]) << 42) & 140737488355327) | ((((long)src[30]) << 34) & 4398046511103) | ((((long)src[31]) << 26) & 17179869183) | ((((long)src[32]) << 18) & 67108863) | ((((long)src[33]) << 10) & 262143) | ((((long)src[34]) << 2) & 1023) | ((((long)src[35]) >> 6) & 3);            dest[6] = ((((long)src[35]) << 41) & 140737488355327) | ((((long)src[36]) << 33) & 2199023255551) | ((((long)src[37]) << 25) & 8589934591) | ((((long)src[38]) << 17) & 33554431) | ((((long)src[39]) << 9) & 131071) | ((((long)src[40]) << 1) & 511) | ((((long)src[41]) >> 7) & 1);            dest[7] = ((((long)src[41]) << 40) & 140737488355327) | ((((long)src[42]) << 32) & 1099511627775) | ((((long)src[43]) << 24) & 4294967295) | ((((long)src[44]) << 16) & 16777215) | ((((long)src[45]) << 8) & 65535) | ((((long)src[46])) & 255);        }

        private static void Pack8LongValuesBE47(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 140737488355327) >> 39)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 140737488355327) >> 31)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 140737488355327) >> 23)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 140737488355327) >> 15)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 140737488355327) >> 7)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 140737488355327) << 1)                | ((src[1] & 140737488355327) >> 46)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 140737488355327) >> 38)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 140737488355327) >> 30)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 140737488355327) >> 22)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 140737488355327) >> 14)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 140737488355327) >> 6)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 140737488355327) << 2)                | ((src[2] & 140737488355327) >> 45)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 140737488355327) >> 37)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 140737488355327) >> 29)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 140737488355327) >> 21)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 140737488355327) >> 13)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 140737488355327) >> 5)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 140737488355327) << 3)                | ((src[3] & 140737488355327) >> 44)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 140737488355327) >> 36)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 140737488355327) >> 28)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 140737488355327) >> 20)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 140737488355327) >> 12)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 140737488355327) >> 4)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 140737488355327) << 4)                | ((src[4] & 140737488355327) >> 43)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 140737488355327) >> 35)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 140737488355327) >> 27)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 140737488355327) >> 19)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 140737488355327) >> 11)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 140737488355327) >> 3)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 140737488355327) << 5)                | ((src[5] & 140737488355327) >> 42)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 140737488355327) >> 34)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 140737488355327) >> 26)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 140737488355327) >> 18)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 140737488355327) >> 10)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 140737488355327) >> 2)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 140737488355327) << 6)                | ((src[6] & 140737488355327) >> 41)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 140737488355327) >> 33)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 140737488355327) >> 25)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 140737488355327) >> 17)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 140737488355327) >> 9)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 140737488355327) >> 1)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 140737488355327) << 7)                | ((src[7] & 140737488355327) >> 40)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 140737488355327) >> 32)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 140737488355327) >> 24)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 140737488355327) >> 16)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 140737488355327) >> 8)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 140737488355327))) & 255);
                        }
        private static void Unpack8LongValuesLE48(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655);            dest[1] = ((((long)src[6])) & 255) | ((((long)src[7]) << 8) & 65535) | ((((long)src[8]) << 16) & 16777215) | ((((long)src[9]) << 24) & 4294967295) | ((((long)src[10]) << 32) & 1099511627775) | ((((long)src[11]) << 40) & 281474976710655);            dest[2] = ((((long)src[12])) & 255) | ((((long)src[13]) << 8) & 65535) | ((((long)src[14]) << 16) & 16777215) | ((((long)src[15]) << 24) & 4294967295) | ((((long)src[16]) << 32) & 1099511627775) | ((((long)src[17]) << 40) & 281474976710655);            dest[3] = ((((long)src[18])) & 255) | ((((long)src[19]) << 8) & 65535) | ((((long)src[20]) << 16) & 16777215) | ((((long)src[21]) << 24) & 4294967295) | ((((long)src[22]) << 32) & 1099511627775) | ((((long)src[23]) << 40) & 281474976710655);            dest[4] = ((((long)src[24])) & 255) | ((((long)src[25]) << 8) & 65535) | ((((long)src[26]) << 16) & 16777215) | ((((long)src[27]) << 24) & 4294967295) | ((((long)src[28]) << 32) & 1099511627775) | ((((long)src[29]) << 40) & 281474976710655);            dest[5] = ((((long)src[30])) & 255) | ((((long)src[31]) << 8) & 65535) | ((((long)src[32]) << 16) & 16777215) | ((((long)src[33]) << 24) & 4294967295) | ((((long)src[34]) << 32) & 1099511627775) | ((((long)src[35]) << 40) & 281474976710655);            dest[6] = ((((long)src[36])) & 255) | ((((long)src[37]) << 8) & 65535) | ((((long)src[38]) << 16) & 16777215) | ((((long)src[39]) << 24) & 4294967295) | ((((long)src[40]) << 32) & 1099511627775) | ((((long)src[41]) << 40) & 281474976710655);            dest[7] = ((((long)src[42])) & 255) | ((((long)src[43]) << 8) & 65535) | ((((long)src[44]) << 16) & 16777215) | ((((long)src[45]) << 24) & 4294967295) | ((((long)src[46]) << 32) & 1099511627775) | ((((long)src[47]) << 40) & 281474976710655);        }

        private static void Pack8LongValuesLE48(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 281474976710655))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 281474976710655) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 281474976710655) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 281474976710655) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 281474976710655) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 281474976710655) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[1] & 281474976710655))) & 255);
                            dest[7] = 
                (byte)((((src[1] & 281474976710655) >> 8)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 281474976710655) >> 16)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 281474976710655) >> 24)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 281474976710655) >> 32)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 281474976710655) >> 40)) & 255);
                            dest[12] = 
                (byte)((((src[2] & 281474976710655))) & 255);
                            dest[13] = 
                (byte)((((src[2] & 281474976710655) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 281474976710655) >> 16)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 281474976710655) >> 24)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 281474976710655) >> 32)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 281474976710655) >> 40)) & 255);
                            dest[18] = 
                (byte)((((src[3] & 281474976710655))) & 255);
                            dest[19] = 
                (byte)((((src[3] & 281474976710655) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 281474976710655) >> 16)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 281474976710655) >> 24)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 281474976710655) >> 32)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 281474976710655) >> 40)) & 255);
                            dest[24] = 
                (byte)((((src[4] & 281474976710655))) & 255);
                            dest[25] = 
                (byte)((((src[4] & 281474976710655) >> 8)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 281474976710655) >> 16)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 281474976710655) >> 24)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 281474976710655) >> 32)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 281474976710655) >> 40)) & 255);
                            dest[30] = 
                (byte)((((src[5] & 281474976710655))) & 255);
                            dest[31] = 
                (byte)((((src[5] & 281474976710655) >> 8)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 281474976710655) >> 16)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 281474976710655) >> 24)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 281474976710655) >> 32)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 281474976710655) >> 40)) & 255);
                            dest[36] = 
                (byte)((((src[6] & 281474976710655))) & 255);
                            dest[37] = 
                (byte)((((src[6] & 281474976710655) >> 8)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 281474976710655) >> 16)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 281474976710655) >> 24)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 281474976710655) >> 32)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 281474976710655) >> 40)) & 255);
                            dest[42] = 
                (byte)((((src[7] & 281474976710655))) & 255);
                            dest[43] = 
                (byte)((((src[7] & 281474976710655) >> 8)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 281474976710655) >> 16)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 281474976710655) >> 24)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 281474976710655) >> 32)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 281474976710655) >> 40)) & 255);
                        }
        private static void Unpack8LongValuesBE48(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 40) & 281474976710655) | ((((long)src[1]) << 32) & 1099511627775) | ((((long)src[2]) << 24) & 4294967295) | ((((long)src[3]) << 16) & 16777215) | ((((long)src[4]) << 8) & 65535) | ((((long)src[5])) & 255);            dest[1] = ((((long)src[6]) << 40) & 281474976710655) | ((((long)src[7]) << 32) & 1099511627775) | ((((long)src[8]) << 24) & 4294967295) | ((((long)src[9]) << 16) & 16777215) | ((((long)src[10]) << 8) & 65535) | ((((long)src[11])) & 255);            dest[2] = ((((long)src[12]) << 40) & 281474976710655) | ((((long)src[13]) << 32) & 1099511627775) | ((((long)src[14]) << 24) & 4294967295) | ((((long)src[15]) << 16) & 16777215) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17])) & 255);            dest[3] = ((((long)src[18]) << 40) & 281474976710655) | ((((long)src[19]) << 32) & 1099511627775) | ((((long)src[20]) << 24) & 4294967295) | ((((long)src[21]) << 16) & 16777215) | ((((long)src[22]) << 8) & 65535) | ((((long)src[23])) & 255);            dest[4] = ((((long)src[24]) << 40) & 281474976710655) | ((((long)src[25]) << 32) & 1099511627775) | ((((long)src[26]) << 24) & 4294967295) | ((((long)src[27]) << 16) & 16777215) | ((((long)src[28]) << 8) & 65535) | ((((long)src[29])) & 255);            dest[5] = ((((long)src[30]) << 40) & 281474976710655) | ((((long)src[31]) << 32) & 1099511627775) | ((((long)src[32]) << 24) & 4294967295) | ((((long)src[33]) << 16) & 16777215) | ((((long)src[34]) << 8) & 65535) | ((((long)src[35])) & 255);            dest[6] = ((((long)src[36]) << 40) & 281474976710655) | ((((long)src[37]) << 32) & 1099511627775) | ((((long)src[38]) << 24) & 4294967295) | ((((long)src[39]) << 16) & 16777215) | ((((long)src[40]) << 8) & 65535) | ((((long)src[41])) & 255);            dest[7] = ((((long)src[42]) << 40) & 281474976710655) | ((((long)src[43]) << 32) & 1099511627775) | ((((long)src[44]) << 24) & 4294967295) | ((((long)src[45]) << 16) & 16777215) | ((((long)src[46]) << 8) & 65535) | ((((long)src[47])) & 255);        }

        private static void Pack8LongValuesBE48(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 281474976710655) >> 40)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 281474976710655) >> 32)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 281474976710655) >> 24)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 281474976710655) >> 16)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 281474976710655) >> 8)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 281474976710655))) & 255);
                            dest[6] = 
                (byte)((((src[1] & 281474976710655) >> 40)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 281474976710655) >> 32)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 281474976710655) >> 24)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 281474976710655) >> 16)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 281474976710655) >> 8)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 281474976710655))) & 255);
                            dest[12] = 
                (byte)((((src[2] & 281474976710655) >> 40)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 281474976710655) >> 32)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 281474976710655) >> 24)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 281474976710655) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 281474976710655) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 281474976710655))) & 255);
                            dest[18] = 
                (byte)((((src[3] & 281474976710655) >> 40)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 281474976710655) >> 32)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 281474976710655) >> 24)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 281474976710655) >> 16)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 281474976710655) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 281474976710655))) & 255);
                            dest[24] = 
                (byte)((((src[4] & 281474976710655) >> 40)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 281474976710655) >> 32)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 281474976710655) >> 24)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 281474976710655) >> 16)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 281474976710655) >> 8)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 281474976710655))) & 255);
                            dest[30] = 
                (byte)((((src[5] & 281474976710655) >> 40)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 281474976710655) >> 32)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 281474976710655) >> 24)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 281474976710655) >> 16)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 281474976710655) >> 8)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 281474976710655))) & 255);
                            dest[36] = 
                (byte)((((src[6] & 281474976710655) >> 40)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 281474976710655) >> 32)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 281474976710655) >> 24)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 281474976710655) >> 16)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 281474976710655) >> 8)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 281474976710655))) & 255);
                            dest[42] = 
                (byte)((((src[7] & 281474976710655) >> 40)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 281474976710655) >> 32)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 281474976710655) >> 24)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 281474976710655) >> 16)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 281474976710655) >> 8)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 281474976710655))) & 255);
                        }
        private static void Unpack8LongValuesLE49(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 562949953421311);            dest[1] = ((((long)src[6]) >> 1) & 127) | ((((long)src[7]) << 7) & 32767) | ((((long)src[8]) << 15) & 8388607) | ((((long)src[9]) << 23) & 2147483647) | ((((long)src[10]) << 31) & 549755813887) | ((((long)src[11]) << 39) & 140737488355327) | ((((long)src[12]) << 47) & 562949953421311);            dest[2] = ((((long)src[12]) >> 2) & 63) | ((((long)src[13]) << 6) & 16383) | ((((long)src[14]) << 14) & 4194303) | ((((long)src[15]) << 22) & 1073741823) | ((((long)src[16]) << 30) & 274877906943) | ((((long)src[17]) << 38) & 70368744177663) | ((((long)src[18]) << 46) & 562949953421311);            dest[3] = ((((long)src[18]) >> 3) & 31) | ((((long)src[19]) << 5) & 8191) | ((((long)src[20]) << 13) & 2097151) | ((((long)src[21]) << 21) & 536870911) | ((((long)src[22]) << 29) & 137438953471) | ((((long)src[23]) << 37) & 35184372088831) | ((((long)src[24]) << 45) & 562949953421311);            dest[4] = ((((long)src[24]) >> 4) & 15) | ((((long)src[25]) << 4) & 4095) | ((((long)src[26]) << 12) & 1048575) | ((((long)src[27]) << 20) & 268435455) | ((((long)src[28]) << 28) & 68719476735) | ((((long)src[29]) << 36) & 17592186044415) | ((((long)src[30]) << 44) & 562949953421311);            dest[5] = ((((long)src[30]) >> 5) & 7) | ((((long)src[31]) << 3) & 2047) | ((((long)src[32]) << 11) & 524287) | ((((long)src[33]) << 19) & 134217727) | ((((long)src[34]) << 27) & 34359738367) | ((((long)src[35]) << 35) & 8796093022207) | ((((long)src[36]) << 43) & 562949953421311);            dest[6] = ((((long)src[36]) >> 6) & 3) | ((((long)src[37]) << 2) & 1023) | ((((long)src[38]) << 10) & 262143) | ((((long)src[39]) << 18) & 67108863) | ((((long)src[40]) << 26) & 17179869183) | ((((long)src[41]) << 34) & 4398046511103) | ((((long)src[42]) << 42) & 562949953421311);            dest[7] = ((((long)src[42]) >> 7) & 1) | ((((long)src[43]) << 1) & 511) | ((((long)src[44]) << 9) & 131071) | ((((long)src[45]) << 17) & 33554431) | ((((long)src[46]) << 25) & 8589934591) | ((((long)src[47]) << 33) & 2199023255551) | ((((long)src[48]) << 41) & 562949953421311);        }

        private static void Pack8LongValuesLE49(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 562949953421311))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 562949953421311) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 562949953421311) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 562949953421311) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 562949953421311) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 562949953421311) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 562949953421311) >> 48)                | ((src[1] & 562949953421311) << 1)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 562949953421311) >> 7)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 562949953421311) >> 15)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 562949953421311) >> 23)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 562949953421311) >> 31)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 562949953421311) >> 39)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 562949953421311) >> 47)                | ((src[2] & 562949953421311) << 2)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 562949953421311) >> 6)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 562949953421311) >> 14)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 562949953421311) >> 22)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 562949953421311) >> 30)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 562949953421311) >> 38)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 562949953421311) >> 46)                | ((src[3] & 562949953421311) << 3)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 562949953421311) >> 5)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 562949953421311) >> 13)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 562949953421311) >> 21)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 562949953421311) >> 29)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 562949953421311) >> 37)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 562949953421311) >> 45)                | ((src[4] & 562949953421311) << 4)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 562949953421311) >> 4)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 562949953421311) >> 12)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 562949953421311) >> 20)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 562949953421311) >> 28)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 562949953421311) >> 36)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 562949953421311) >> 44)                | ((src[5] & 562949953421311) << 5)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 562949953421311) >> 3)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 562949953421311) >> 11)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 562949953421311) >> 19)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 562949953421311) >> 27)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 562949953421311) >> 35)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 562949953421311) >> 43)                | ((src[6] & 562949953421311) << 6)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 562949953421311) >> 2)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 562949953421311) >> 10)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 562949953421311) >> 18)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 562949953421311) >> 26)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 562949953421311) >> 34)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 562949953421311) >> 42)                | ((src[7] & 562949953421311) << 7)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 562949953421311) >> 1)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 562949953421311) >> 9)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 562949953421311) >> 17)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 562949953421311) >> 25)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 562949953421311) >> 33)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 562949953421311) >> 41)) & 255);
                        }
        private static void Unpack8LongValuesBE49(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 41) & 562949953421311) | ((((long)src[1]) << 33) & 2199023255551) | ((((long)src[2]) << 25) & 8589934591) | ((((long)src[3]) << 17) & 33554431) | ((((long)src[4]) << 9) & 131071) | ((((long)src[5]) << 1) & 511) | ((((long)src[6]) >> 7) & 1);            dest[1] = ((((long)src[6]) << 42) & 562949953421311) | ((((long)src[7]) << 34) & 4398046511103) | ((((long)src[8]) << 26) & 17179869183) | ((((long)src[9]) << 18) & 67108863) | ((((long)src[10]) << 10) & 262143) | ((((long)src[11]) << 2) & 1023) | ((((long)src[12]) >> 6) & 3);            dest[2] = ((((long)src[12]) << 43) & 562949953421311) | ((((long)src[13]) << 35) & 8796093022207) | ((((long)src[14]) << 27) & 34359738367) | ((((long)src[15]) << 19) & 134217727) | ((((long)src[16]) << 11) & 524287) | ((((long)src[17]) << 3) & 2047) | ((((long)src[18]) >> 5) & 7);            dest[3] = ((((long)src[18]) << 44) & 562949953421311) | ((((long)src[19]) << 36) & 17592186044415) | ((((long)src[20]) << 28) & 68719476735) | ((((long)src[21]) << 20) & 268435455) | ((((long)src[22]) << 12) & 1048575) | ((((long)src[23]) << 4) & 4095) | ((((long)src[24]) >> 4) & 15);            dest[4] = ((((long)src[24]) << 45) & 562949953421311) | ((((long)src[25]) << 37) & 35184372088831) | ((((long)src[26]) << 29) & 137438953471) | ((((long)src[27]) << 21) & 536870911) | ((((long)src[28]) << 13) & 2097151) | ((((long)src[29]) << 5) & 8191) | ((((long)src[30]) >> 3) & 31);            dest[5] = ((((long)src[30]) << 46) & 562949953421311) | ((((long)src[31]) << 38) & 70368744177663) | ((((long)src[32]) << 30) & 274877906943) | ((((long)src[33]) << 22) & 1073741823) | ((((long)src[34]) << 14) & 4194303) | ((((long)src[35]) << 6) & 16383) | ((((long)src[36]) >> 2) & 63);            dest[6] = ((((long)src[36]) << 47) & 562949953421311) | ((((long)src[37]) << 39) & 140737488355327) | ((((long)src[38]) << 31) & 549755813887) | ((((long)src[39]) << 23) & 2147483647) | ((((long)src[40]) << 15) & 8388607) | ((((long)src[41]) << 7) & 32767) | ((((long)src[42]) >> 1) & 127);            dest[7] = ((((long)src[42]) << 48) & 562949953421311) | ((((long)src[43]) << 40) & 281474976710655) | ((((long)src[44]) << 32) & 1099511627775) | ((((long)src[45]) << 24) & 4294967295) | ((((long)src[46]) << 16) & 16777215) | ((((long)src[47]) << 8) & 65535) | ((((long)src[48])) & 255);        }

        private static void Pack8LongValuesBE49(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 562949953421311) >> 41)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 562949953421311) >> 33)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 562949953421311) >> 25)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 562949953421311) >> 17)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 562949953421311) >> 9)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 562949953421311) >> 1)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 562949953421311) << 7)                | ((src[1] & 562949953421311) >> 42)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 562949953421311) >> 34)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 562949953421311) >> 26)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 562949953421311) >> 18)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 562949953421311) >> 10)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 562949953421311) >> 2)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 562949953421311) << 6)                | ((src[2] & 562949953421311) >> 43)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 562949953421311) >> 35)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 562949953421311) >> 27)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 562949953421311) >> 19)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 562949953421311) >> 11)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 562949953421311) >> 3)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 562949953421311) << 5)                | ((src[3] & 562949953421311) >> 44)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 562949953421311) >> 36)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 562949953421311) >> 28)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 562949953421311) >> 20)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 562949953421311) >> 12)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 562949953421311) >> 4)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 562949953421311) << 4)                | ((src[4] & 562949953421311) >> 45)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 562949953421311) >> 37)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 562949953421311) >> 29)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 562949953421311) >> 21)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 562949953421311) >> 13)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 562949953421311) >> 5)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 562949953421311) << 3)                | ((src[5] & 562949953421311) >> 46)) & 255);
                            dest[31] = 
                (byte)((((src[5] & 562949953421311) >> 38)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 562949953421311) >> 30)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 562949953421311) >> 22)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 562949953421311) >> 14)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 562949953421311) >> 6)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 562949953421311) << 2)                | ((src[6] & 562949953421311) >> 47)) & 255);
                            dest[37] = 
                (byte)((((src[6] & 562949953421311) >> 39)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 562949953421311) >> 31)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 562949953421311) >> 23)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 562949953421311) >> 15)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 562949953421311) >> 7)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 562949953421311) << 1)                | ((src[7] & 562949953421311) >> 48)) & 255);
                            dest[43] = 
                (byte)((((src[7] & 562949953421311) >> 40)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 562949953421311) >> 32)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 562949953421311) >> 24)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 562949953421311) >> 16)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 562949953421311) >> 8)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 562949953421311))) & 255);
                        }
        private static void Unpack8LongValuesLE50(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 1125899906842623);            dest[1] = ((((long)src[6]) >> 2) & 63) | ((((long)src[7]) << 6) & 16383) | ((((long)src[8]) << 14) & 4194303) | ((((long)src[9]) << 22) & 1073741823) | ((((long)src[10]) << 30) & 274877906943) | ((((long)src[11]) << 38) & 70368744177663) | ((((long)src[12]) << 46) & 1125899906842623);            dest[2] = ((((long)src[12]) >> 4) & 15) | ((((long)src[13]) << 4) & 4095) | ((((long)src[14]) << 12) & 1048575) | ((((long)src[15]) << 20) & 268435455) | ((((long)src[16]) << 28) & 68719476735) | ((((long)src[17]) << 36) & 17592186044415) | ((((long)src[18]) << 44) & 1125899906842623);            dest[3] = ((((long)src[18]) >> 6) & 3) | ((((long)src[19]) << 2) & 1023) | ((((long)src[20]) << 10) & 262143) | ((((long)src[21]) << 18) & 67108863) | ((((long)src[22]) << 26) & 17179869183) | ((((long)src[23]) << 34) & 4398046511103) | ((((long)src[24]) << 42) & 1125899906842623);            dest[4] = ((((long)src[25])) & 255) | ((((long)src[26]) << 8) & 65535) | ((((long)src[27]) << 16) & 16777215) | ((((long)src[28]) << 24) & 4294967295) | ((((long)src[29]) << 32) & 1099511627775) | ((((long)src[30]) << 40) & 281474976710655) | ((((long)src[31]) << 48) & 1125899906842623);            dest[5] = ((((long)src[31]) >> 2) & 63) | ((((long)src[32]) << 6) & 16383) | ((((long)src[33]) << 14) & 4194303) | ((((long)src[34]) << 22) & 1073741823) | ((((long)src[35]) << 30) & 274877906943) | ((((long)src[36]) << 38) & 70368744177663) | ((((long)src[37]) << 46) & 1125899906842623);            dest[6] = ((((long)src[37]) >> 4) & 15) | ((((long)src[38]) << 4) & 4095) | ((((long)src[39]) << 12) & 1048575) | ((((long)src[40]) << 20) & 268435455) | ((((long)src[41]) << 28) & 68719476735) | ((((long)src[42]) << 36) & 17592186044415) | ((((long)src[43]) << 44) & 1125899906842623);            dest[7] = ((((long)src[43]) >> 6) & 3) | ((((long)src[44]) << 2) & 1023) | ((((long)src[45]) << 10) & 262143) | ((((long)src[46]) << 18) & 67108863) | ((((long)src[47]) << 26) & 17179869183) | ((((long)src[48]) << 34) & 4398046511103) | ((((long)src[49]) << 42) & 1125899906842623);        }

        private static void Pack8LongValuesLE50(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1125899906842623))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1125899906842623) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1125899906842623) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1125899906842623) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 1125899906842623) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 1125899906842623) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 1125899906842623) >> 48)                | ((src[1] & 1125899906842623) << 2)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 1125899906842623) >> 6)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 1125899906842623) >> 14)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 1125899906842623) >> 22)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 1125899906842623) >> 30)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 1125899906842623) >> 38)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 1125899906842623) >> 46)                | ((src[2] & 1125899906842623) << 4)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 1125899906842623) >> 4)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 1125899906842623) >> 12)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 1125899906842623) >> 20)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 1125899906842623) >> 28)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 1125899906842623) >> 36)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 1125899906842623) >> 44)                | ((src[3] & 1125899906842623) << 6)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 1125899906842623) >> 2)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 1125899906842623) >> 10)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 1125899906842623) >> 18)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 1125899906842623) >> 26)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 1125899906842623) >> 34)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 1125899906842623) >> 42)) & 255);
                            dest[25] = 
                (byte)((((src[4] & 1125899906842623))) & 255);
                            dest[26] = 
                (byte)((((src[4] & 1125899906842623) >> 8)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 1125899906842623) >> 16)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 1125899906842623) >> 24)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 1125899906842623) >> 32)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 1125899906842623) >> 40)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 1125899906842623) >> 48)                | ((src[5] & 1125899906842623) << 2)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 1125899906842623) >> 6)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 1125899906842623) >> 14)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 1125899906842623) >> 22)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 1125899906842623) >> 30)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 1125899906842623) >> 38)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 1125899906842623) >> 46)                | ((src[6] & 1125899906842623) << 4)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 1125899906842623) >> 4)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 1125899906842623) >> 12)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 1125899906842623) >> 20)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 1125899906842623) >> 28)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 1125899906842623) >> 36)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 1125899906842623) >> 44)                | ((src[7] & 1125899906842623) << 6)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 1125899906842623) >> 2)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 1125899906842623) >> 10)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 1125899906842623) >> 18)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 1125899906842623) >> 26)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 1125899906842623) >> 34)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 1125899906842623) >> 42)) & 255);
                        }
        private static void Unpack8LongValuesBE50(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 42) & 1125899906842623) | ((((long)src[1]) << 34) & 4398046511103) | ((((long)src[2]) << 26) & 17179869183) | ((((long)src[3]) << 18) & 67108863) | ((((long)src[4]) << 10) & 262143) | ((((long)src[5]) << 2) & 1023) | ((((long)src[6]) >> 6) & 3);            dest[1] = ((((long)src[6]) << 44) & 1125899906842623) | ((((long)src[7]) << 36) & 17592186044415) | ((((long)src[8]) << 28) & 68719476735) | ((((long)src[9]) << 20) & 268435455) | ((((long)src[10]) << 12) & 1048575) | ((((long)src[11]) << 4) & 4095) | ((((long)src[12]) >> 4) & 15);            dest[2] = ((((long)src[12]) << 46) & 1125899906842623) | ((((long)src[13]) << 38) & 70368744177663) | ((((long)src[14]) << 30) & 274877906943) | ((((long)src[15]) << 22) & 1073741823) | ((((long)src[16]) << 14) & 4194303) | ((((long)src[17]) << 6) & 16383) | ((((long)src[18]) >> 2) & 63);            dest[3] = ((((long)src[18]) << 48) & 1125899906842623) | ((((long)src[19]) << 40) & 281474976710655) | ((((long)src[20]) << 32) & 1099511627775) | ((((long)src[21]) << 24) & 4294967295) | ((((long)src[22]) << 16) & 16777215) | ((((long)src[23]) << 8) & 65535) | ((((long)src[24])) & 255);            dest[4] = ((((long)src[25]) << 42) & 1125899906842623) | ((((long)src[26]) << 34) & 4398046511103) | ((((long)src[27]) << 26) & 17179869183) | ((((long)src[28]) << 18) & 67108863) | ((((long)src[29]) << 10) & 262143) | ((((long)src[30]) << 2) & 1023) | ((((long)src[31]) >> 6) & 3);            dest[5] = ((((long)src[31]) << 44) & 1125899906842623) | ((((long)src[32]) << 36) & 17592186044415) | ((((long)src[33]) << 28) & 68719476735) | ((((long)src[34]) << 20) & 268435455) | ((((long)src[35]) << 12) & 1048575) | ((((long)src[36]) << 4) & 4095) | ((((long)src[37]) >> 4) & 15);            dest[6] = ((((long)src[37]) << 46) & 1125899906842623) | ((((long)src[38]) << 38) & 70368744177663) | ((((long)src[39]) << 30) & 274877906943) | ((((long)src[40]) << 22) & 1073741823) | ((((long)src[41]) << 14) & 4194303) | ((((long)src[42]) << 6) & 16383) | ((((long)src[43]) >> 2) & 63);            dest[7] = ((((long)src[43]) << 48) & 1125899906842623) | ((((long)src[44]) << 40) & 281474976710655) | ((((long)src[45]) << 32) & 1099511627775) | ((((long)src[46]) << 24) & 4294967295) | ((((long)src[47]) << 16) & 16777215) | ((((long)src[48]) << 8) & 65535) | ((((long)src[49])) & 255);        }

        private static void Pack8LongValuesBE50(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1125899906842623) >> 42)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1125899906842623) >> 34)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1125899906842623) >> 26)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1125899906842623) >> 18)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 1125899906842623) >> 10)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 1125899906842623) >> 2)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 1125899906842623) << 6)                | ((src[1] & 1125899906842623) >> 44)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 1125899906842623) >> 36)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 1125899906842623) >> 28)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 1125899906842623) >> 20)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 1125899906842623) >> 12)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 1125899906842623) >> 4)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 1125899906842623) << 4)                | ((src[2] & 1125899906842623) >> 46)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 1125899906842623) >> 38)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 1125899906842623) >> 30)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 1125899906842623) >> 22)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 1125899906842623) >> 14)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 1125899906842623) >> 6)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 1125899906842623) << 2)                | ((src[3] & 1125899906842623) >> 48)) & 255);
                            dest[19] = 
                (byte)((((src[3] & 1125899906842623) >> 40)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 1125899906842623) >> 32)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 1125899906842623) >> 24)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 1125899906842623) >> 16)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 1125899906842623) >> 8)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 1125899906842623))) & 255);
                            dest[25] = 
                (byte)((((src[4] & 1125899906842623) >> 42)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 1125899906842623) >> 34)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 1125899906842623) >> 26)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 1125899906842623) >> 18)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 1125899906842623) >> 10)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 1125899906842623) >> 2)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 1125899906842623) << 6)                | ((src[5] & 1125899906842623) >> 44)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 1125899906842623) >> 36)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 1125899906842623) >> 28)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 1125899906842623) >> 20)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 1125899906842623) >> 12)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 1125899906842623) >> 4)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 1125899906842623) << 4)                | ((src[6] & 1125899906842623) >> 46)) & 255);
                            dest[38] = 
                (byte)((((src[6] & 1125899906842623) >> 38)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 1125899906842623) >> 30)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 1125899906842623) >> 22)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 1125899906842623) >> 14)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 1125899906842623) >> 6)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 1125899906842623) << 2)                | ((src[7] & 1125899906842623) >> 48)) & 255);
                            dest[44] = 
                (byte)((((src[7] & 1125899906842623) >> 40)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 1125899906842623) >> 32)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 1125899906842623) >> 24)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 1125899906842623) >> 16)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 1125899906842623) >> 8)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 1125899906842623))) & 255);
                        }
        private static void Unpack8LongValuesLE51(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 2251799813685247);            dest[1] = ((((long)src[6]) >> 3) & 31) | ((((long)src[7]) << 5) & 8191) | ((((long)src[8]) << 13) & 2097151) | ((((long)src[9]) << 21) & 536870911) | ((((long)src[10]) << 29) & 137438953471) | ((((long)src[11]) << 37) & 35184372088831) | ((((long)src[12]) << 45) & 2251799813685247);            dest[2] = ((((long)src[12]) >> 6) & 3) | ((((long)src[13]) << 2) & 1023) | ((((long)src[14]) << 10) & 262143) | ((((long)src[15]) << 18) & 67108863) | ((((long)src[16]) << 26) & 17179869183) | ((((long)src[17]) << 34) & 4398046511103) | ((((long)src[18]) << 42) & 1125899906842623) | ((((long)src[19]) << 50) & 2251799813685247);            dest[3] = ((((long)src[19]) >> 1) & 127) | ((((long)src[20]) << 7) & 32767) | ((((long)src[21]) << 15) & 8388607) | ((((long)src[22]) << 23) & 2147483647) | ((((long)src[23]) << 31) & 549755813887) | ((((long)src[24]) << 39) & 140737488355327) | ((((long)src[25]) << 47) & 2251799813685247);            dest[4] = ((((long)src[25]) >> 4) & 15) | ((((long)src[26]) << 4) & 4095) | ((((long)src[27]) << 12) & 1048575) | ((((long)src[28]) << 20) & 268435455) | ((((long)src[29]) << 28) & 68719476735) | ((((long)src[30]) << 36) & 17592186044415) | ((((long)src[31]) << 44) & 2251799813685247);            dest[5] = ((((long)src[31]) >> 7) & 1) | ((((long)src[32]) << 1) & 511) | ((((long)src[33]) << 9) & 131071) | ((((long)src[34]) << 17) & 33554431) | ((((long)src[35]) << 25) & 8589934591) | ((((long)src[36]) << 33) & 2199023255551) | ((((long)src[37]) << 41) & 562949953421311) | ((((long)src[38]) << 49) & 2251799813685247);            dest[6] = ((((long)src[38]) >> 2) & 63) | ((((long)src[39]) << 6) & 16383) | ((((long)src[40]) << 14) & 4194303) | ((((long)src[41]) << 22) & 1073741823) | ((((long)src[42]) << 30) & 274877906943) | ((((long)src[43]) << 38) & 70368744177663) | ((((long)src[44]) << 46) & 2251799813685247);            dest[7] = ((((long)src[44]) >> 5) & 7) | ((((long)src[45]) << 3) & 2047) | ((((long)src[46]) << 11) & 524287) | ((((long)src[47]) << 19) & 134217727) | ((((long)src[48]) << 27) & 34359738367) | ((((long)src[49]) << 35) & 8796093022207) | ((((long)src[50]) << 43) & 2251799813685247);        }

        private static void Pack8LongValuesLE51(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2251799813685247))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2251799813685247) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2251799813685247) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2251799813685247) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 2251799813685247) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 2251799813685247) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 2251799813685247) >> 48)                | ((src[1] & 2251799813685247) << 3)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 2251799813685247) >> 5)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 2251799813685247) >> 13)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 2251799813685247) >> 21)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 2251799813685247) >> 29)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 2251799813685247) >> 37)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 2251799813685247) >> 45)                | ((src[2] & 2251799813685247) << 6)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 2251799813685247) >> 2)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 2251799813685247) >> 10)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 2251799813685247) >> 18)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 2251799813685247) >> 26)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 2251799813685247) >> 34)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 2251799813685247) >> 42)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 2251799813685247) >> 50)                | ((src[3] & 2251799813685247) << 1)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 2251799813685247) >> 7)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 2251799813685247) >> 15)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 2251799813685247) >> 23)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 2251799813685247) >> 31)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 2251799813685247) >> 39)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 2251799813685247) >> 47)                | ((src[4] & 2251799813685247) << 4)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 2251799813685247) >> 4)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 2251799813685247) >> 12)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 2251799813685247) >> 20)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 2251799813685247) >> 28)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 2251799813685247) >> 36)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 2251799813685247) >> 44)                | ((src[5] & 2251799813685247) << 7)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 2251799813685247) >> 1)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 2251799813685247) >> 9)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 2251799813685247) >> 17)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 2251799813685247) >> 25)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 2251799813685247) >> 33)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 2251799813685247) >> 41)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 2251799813685247) >> 49)                | ((src[6] & 2251799813685247) << 2)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 2251799813685247) >> 6)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 2251799813685247) >> 14)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 2251799813685247) >> 22)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 2251799813685247) >> 30)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 2251799813685247) >> 38)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 2251799813685247) >> 46)                | ((src[7] & 2251799813685247) << 5)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 2251799813685247) >> 3)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 2251799813685247) >> 11)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 2251799813685247) >> 19)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 2251799813685247) >> 27)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 2251799813685247) >> 35)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 2251799813685247) >> 43)) & 255);
                        }
        private static void Unpack8LongValuesBE51(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 43) & 2251799813685247) | ((((long)src[1]) << 35) & 8796093022207) | ((((long)src[2]) << 27) & 34359738367) | ((((long)src[3]) << 19) & 134217727) | ((((long)src[4]) << 11) & 524287) | ((((long)src[5]) << 3) & 2047) | ((((long)src[6]) >> 5) & 7);            dest[1] = ((((long)src[6]) << 46) & 2251799813685247) | ((((long)src[7]) << 38) & 70368744177663) | ((((long)src[8]) << 30) & 274877906943) | ((((long)src[9]) << 22) & 1073741823) | ((((long)src[10]) << 14) & 4194303) | ((((long)src[11]) << 6) & 16383) | ((((long)src[12]) >> 2) & 63);            dest[2] = ((((long)src[12]) << 49) & 2251799813685247) | ((((long)src[13]) << 41) & 562949953421311) | ((((long)src[14]) << 33) & 2199023255551) | ((((long)src[15]) << 25) & 8589934591) | ((((long)src[16]) << 17) & 33554431) | ((((long)src[17]) << 9) & 131071) | ((((long)src[18]) << 1) & 511) | ((((long)src[19]) >> 7) & 1);            dest[3] = ((((long)src[19]) << 44) & 2251799813685247) | ((((long)src[20]) << 36) & 17592186044415) | ((((long)src[21]) << 28) & 68719476735) | ((((long)src[22]) << 20) & 268435455) | ((((long)src[23]) << 12) & 1048575) | ((((long)src[24]) << 4) & 4095) | ((((long)src[25]) >> 4) & 15);            dest[4] = ((((long)src[25]) << 47) & 2251799813685247) | ((((long)src[26]) << 39) & 140737488355327) | ((((long)src[27]) << 31) & 549755813887) | ((((long)src[28]) << 23) & 2147483647) | ((((long)src[29]) << 15) & 8388607) | ((((long)src[30]) << 7) & 32767) | ((((long)src[31]) >> 1) & 127);            dest[5] = ((((long)src[31]) << 50) & 2251799813685247) | ((((long)src[32]) << 42) & 1125899906842623) | ((((long)src[33]) << 34) & 4398046511103) | ((((long)src[34]) << 26) & 17179869183) | ((((long)src[35]) << 18) & 67108863) | ((((long)src[36]) << 10) & 262143) | ((((long)src[37]) << 2) & 1023) | ((((long)src[38]) >> 6) & 3);            dest[6] = ((((long)src[38]) << 45) & 2251799813685247) | ((((long)src[39]) << 37) & 35184372088831) | ((((long)src[40]) << 29) & 137438953471) | ((((long)src[41]) << 21) & 536870911) | ((((long)src[42]) << 13) & 2097151) | ((((long)src[43]) << 5) & 8191) | ((((long)src[44]) >> 3) & 31);            dest[7] = ((((long)src[44]) << 48) & 2251799813685247) | ((((long)src[45]) << 40) & 281474976710655) | ((((long)src[46]) << 32) & 1099511627775) | ((((long)src[47]) << 24) & 4294967295) | ((((long)src[48]) << 16) & 16777215) | ((((long)src[49]) << 8) & 65535) | ((((long)src[50])) & 255);        }

        private static void Pack8LongValuesBE51(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2251799813685247) >> 43)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2251799813685247) >> 35)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2251799813685247) >> 27)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2251799813685247) >> 19)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 2251799813685247) >> 11)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 2251799813685247) >> 3)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 2251799813685247) << 5)                | ((src[1] & 2251799813685247) >> 46)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 2251799813685247) >> 38)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 2251799813685247) >> 30)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 2251799813685247) >> 22)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 2251799813685247) >> 14)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 2251799813685247) >> 6)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 2251799813685247) << 2)                | ((src[2] & 2251799813685247) >> 49)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 2251799813685247) >> 41)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 2251799813685247) >> 33)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 2251799813685247) >> 25)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 2251799813685247) >> 17)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 2251799813685247) >> 9)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 2251799813685247) >> 1)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 2251799813685247) << 7)                | ((src[3] & 2251799813685247) >> 44)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 2251799813685247) >> 36)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 2251799813685247) >> 28)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 2251799813685247) >> 20)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 2251799813685247) >> 12)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 2251799813685247) >> 4)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 2251799813685247) << 4)                | ((src[4] & 2251799813685247) >> 47)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 2251799813685247) >> 39)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 2251799813685247) >> 31)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 2251799813685247) >> 23)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 2251799813685247) >> 15)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 2251799813685247) >> 7)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 2251799813685247) << 1)                | ((src[5] & 2251799813685247) >> 50)) & 255);
                            dest[32] = 
                (byte)((((src[5] & 2251799813685247) >> 42)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 2251799813685247) >> 34)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 2251799813685247) >> 26)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 2251799813685247) >> 18)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 2251799813685247) >> 10)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 2251799813685247) >> 2)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 2251799813685247) << 6)                | ((src[6] & 2251799813685247) >> 45)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 2251799813685247) >> 37)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 2251799813685247) >> 29)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 2251799813685247) >> 21)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 2251799813685247) >> 13)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 2251799813685247) >> 5)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 2251799813685247) << 3)                | ((src[7] & 2251799813685247) >> 48)) & 255);
                            dest[45] = 
                (byte)((((src[7] & 2251799813685247) >> 40)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 2251799813685247) >> 32)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 2251799813685247) >> 24)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 2251799813685247) >> 16)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 2251799813685247) >> 8)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 2251799813685247))) & 255);
                        }
        private static void Unpack8LongValuesLE52(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 4503599627370495);            dest[1] = ((((long)src[6]) >> 4) & 15) | ((((long)src[7]) << 4) & 4095) | ((((long)src[8]) << 12) & 1048575) | ((((long)src[9]) << 20) & 268435455) | ((((long)src[10]) << 28) & 68719476735) | ((((long)src[11]) << 36) & 17592186044415) | ((((long)src[12]) << 44) & 4503599627370495);            dest[2] = ((((long)src[13])) & 255) | ((((long)src[14]) << 8) & 65535) | ((((long)src[15]) << 16) & 16777215) | ((((long)src[16]) << 24) & 4294967295) | ((((long)src[17]) << 32) & 1099511627775) | ((((long)src[18]) << 40) & 281474976710655) | ((((long)src[19]) << 48) & 4503599627370495);            dest[3] = ((((long)src[19]) >> 4) & 15) | ((((long)src[20]) << 4) & 4095) | ((((long)src[21]) << 12) & 1048575) | ((((long)src[22]) << 20) & 268435455) | ((((long)src[23]) << 28) & 68719476735) | ((((long)src[24]) << 36) & 17592186044415) | ((((long)src[25]) << 44) & 4503599627370495);            dest[4] = ((((long)src[26])) & 255) | ((((long)src[27]) << 8) & 65535) | ((((long)src[28]) << 16) & 16777215) | ((((long)src[29]) << 24) & 4294967295) | ((((long)src[30]) << 32) & 1099511627775) | ((((long)src[31]) << 40) & 281474976710655) | ((((long)src[32]) << 48) & 4503599627370495);            dest[5] = ((((long)src[32]) >> 4) & 15) | ((((long)src[33]) << 4) & 4095) | ((((long)src[34]) << 12) & 1048575) | ((((long)src[35]) << 20) & 268435455) | ((((long)src[36]) << 28) & 68719476735) | ((((long)src[37]) << 36) & 17592186044415) | ((((long)src[38]) << 44) & 4503599627370495);            dest[6] = ((((long)src[39])) & 255) | ((((long)src[40]) << 8) & 65535) | ((((long)src[41]) << 16) & 16777215) | ((((long)src[42]) << 24) & 4294967295) | ((((long)src[43]) << 32) & 1099511627775) | ((((long)src[44]) << 40) & 281474976710655) | ((((long)src[45]) << 48) & 4503599627370495);            dest[7] = ((((long)src[45]) >> 4) & 15) | ((((long)src[46]) << 4) & 4095) | ((((long)src[47]) << 12) & 1048575) | ((((long)src[48]) << 20) & 268435455) | ((((long)src[49]) << 28) & 68719476735) | ((((long)src[50]) << 36) & 17592186044415) | ((((long)src[51]) << 44) & 4503599627370495);        }

        private static void Pack8LongValuesLE52(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4503599627370495))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4503599627370495) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4503599627370495) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4503599627370495) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 4503599627370495) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 4503599627370495) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 4503599627370495) >> 48)                | ((src[1] & 4503599627370495) << 4)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 4503599627370495) >> 4)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 4503599627370495) >> 12)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 4503599627370495) >> 20)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 4503599627370495) >> 28)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 4503599627370495) >> 36)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 4503599627370495) >> 44)) & 255);
                            dest[13] = 
                (byte)((((src[2] & 4503599627370495))) & 255);
                            dest[14] = 
                (byte)((((src[2] & 4503599627370495) >> 8)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 4503599627370495) >> 16)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 4503599627370495) >> 24)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 4503599627370495) >> 32)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 4503599627370495) >> 40)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 4503599627370495) >> 48)                | ((src[3] & 4503599627370495) << 4)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 4503599627370495) >> 4)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 4503599627370495) >> 12)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 4503599627370495) >> 20)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 4503599627370495) >> 28)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 4503599627370495) >> 36)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 4503599627370495) >> 44)) & 255);
                            dest[26] = 
                (byte)((((src[4] & 4503599627370495))) & 255);
                            dest[27] = 
                (byte)((((src[4] & 4503599627370495) >> 8)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 4503599627370495) >> 16)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 4503599627370495) >> 24)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 4503599627370495) >> 32)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 4503599627370495) >> 40)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 4503599627370495) >> 48)                | ((src[5] & 4503599627370495) << 4)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 4503599627370495) >> 4)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 4503599627370495) >> 12)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 4503599627370495) >> 20)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 4503599627370495) >> 28)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 4503599627370495) >> 36)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 4503599627370495) >> 44)) & 255);
                            dest[39] = 
                (byte)((((src[6] & 4503599627370495))) & 255);
                            dest[40] = 
                (byte)((((src[6] & 4503599627370495) >> 8)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 4503599627370495) >> 16)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 4503599627370495) >> 24)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 4503599627370495) >> 32)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 4503599627370495) >> 40)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 4503599627370495) >> 48)                | ((src[7] & 4503599627370495) << 4)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 4503599627370495) >> 4)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 4503599627370495) >> 12)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 4503599627370495) >> 20)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 4503599627370495) >> 28)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 4503599627370495) >> 36)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 4503599627370495) >> 44)) & 255);
                        }
        private static void Unpack8LongValuesBE52(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 44) & 4503599627370495) | ((((long)src[1]) << 36) & 17592186044415) | ((((long)src[2]) << 28) & 68719476735) | ((((long)src[3]) << 20) & 268435455) | ((((long)src[4]) << 12) & 1048575) | ((((long)src[5]) << 4) & 4095) | ((((long)src[6]) >> 4) & 15);            dest[1] = ((((long)src[6]) << 48) & 4503599627370495) | ((((long)src[7]) << 40) & 281474976710655) | ((((long)src[8]) << 32) & 1099511627775) | ((((long)src[9]) << 24) & 4294967295) | ((((long)src[10]) << 16) & 16777215) | ((((long)src[11]) << 8) & 65535) | ((((long)src[12])) & 255);            dest[2] = ((((long)src[13]) << 44) & 4503599627370495) | ((((long)src[14]) << 36) & 17592186044415) | ((((long)src[15]) << 28) & 68719476735) | ((((long)src[16]) << 20) & 268435455) | ((((long)src[17]) << 12) & 1048575) | ((((long)src[18]) << 4) & 4095) | ((((long)src[19]) >> 4) & 15);            dest[3] = ((((long)src[19]) << 48) & 4503599627370495) | ((((long)src[20]) << 40) & 281474976710655) | ((((long)src[21]) << 32) & 1099511627775) | ((((long)src[22]) << 24) & 4294967295) | ((((long)src[23]) << 16) & 16777215) | ((((long)src[24]) << 8) & 65535) | ((((long)src[25])) & 255);            dest[4] = ((((long)src[26]) << 44) & 4503599627370495) | ((((long)src[27]) << 36) & 17592186044415) | ((((long)src[28]) << 28) & 68719476735) | ((((long)src[29]) << 20) & 268435455) | ((((long)src[30]) << 12) & 1048575) | ((((long)src[31]) << 4) & 4095) | ((((long)src[32]) >> 4) & 15);            dest[5] = ((((long)src[32]) << 48) & 4503599627370495) | ((((long)src[33]) << 40) & 281474976710655) | ((((long)src[34]) << 32) & 1099511627775) | ((((long)src[35]) << 24) & 4294967295) | ((((long)src[36]) << 16) & 16777215) | ((((long)src[37]) << 8) & 65535) | ((((long)src[38])) & 255);            dest[6] = ((((long)src[39]) << 44) & 4503599627370495) | ((((long)src[40]) << 36) & 17592186044415) | ((((long)src[41]) << 28) & 68719476735) | ((((long)src[42]) << 20) & 268435455) | ((((long)src[43]) << 12) & 1048575) | ((((long)src[44]) << 4) & 4095) | ((((long)src[45]) >> 4) & 15);            dest[7] = ((((long)src[45]) << 48) & 4503599627370495) | ((((long)src[46]) << 40) & 281474976710655) | ((((long)src[47]) << 32) & 1099511627775) | ((((long)src[48]) << 24) & 4294967295) | ((((long)src[49]) << 16) & 16777215) | ((((long)src[50]) << 8) & 65535) | ((((long)src[51])) & 255);        }

        private static void Pack8LongValuesBE52(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4503599627370495) >> 44)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4503599627370495) >> 36)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4503599627370495) >> 28)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4503599627370495) >> 20)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 4503599627370495) >> 12)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 4503599627370495) >> 4)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 4503599627370495) << 4)                | ((src[1] & 4503599627370495) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 4503599627370495) >> 40)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 4503599627370495) >> 32)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 4503599627370495) >> 24)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 4503599627370495) >> 16)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 4503599627370495) >> 8)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 4503599627370495))) & 255);
                            dest[13] = 
                (byte)((((src[2] & 4503599627370495) >> 44)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 4503599627370495) >> 36)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 4503599627370495) >> 28)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 4503599627370495) >> 20)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 4503599627370495) >> 12)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 4503599627370495) >> 4)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 4503599627370495) << 4)                | ((src[3] & 4503599627370495) >> 48)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 4503599627370495) >> 40)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 4503599627370495) >> 32)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 4503599627370495) >> 24)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 4503599627370495) >> 16)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 4503599627370495) >> 8)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 4503599627370495))) & 255);
                            dest[26] = 
                (byte)((((src[4] & 4503599627370495) >> 44)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 4503599627370495) >> 36)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 4503599627370495) >> 28)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 4503599627370495) >> 20)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 4503599627370495) >> 12)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 4503599627370495) >> 4)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 4503599627370495) << 4)                | ((src[5] & 4503599627370495) >> 48)) & 255);
                            dest[33] = 
                (byte)((((src[5] & 4503599627370495) >> 40)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 4503599627370495) >> 32)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 4503599627370495) >> 24)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 4503599627370495) >> 16)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 4503599627370495) >> 8)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 4503599627370495))) & 255);
                            dest[39] = 
                (byte)((((src[6] & 4503599627370495) >> 44)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 4503599627370495) >> 36)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 4503599627370495) >> 28)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 4503599627370495) >> 20)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 4503599627370495) >> 12)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 4503599627370495) >> 4)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 4503599627370495) << 4)                | ((src[7] & 4503599627370495) >> 48)) & 255);
                            dest[46] = 
                (byte)((((src[7] & 4503599627370495) >> 40)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 4503599627370495) >> 32)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 4503599627370495) >> 24)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 4503599627370495) >> 16)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 4503599627370495) >> 8)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 4503599627370495))) & 255);
                        }
        private static void Unpack8LongValuesLE53(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 9007199254740991);            dest[1] = ((((long)src[6]) >> 5) & 7) | ((((long)src[7]) << 3) & 2047) | ((((long)src[8]) << 11) & 524287) | ((((long)src[9]) << 19) & 134217727) | ((((long)src[10]) << 27) & 34359738367) | ((((long)src[11]) << 35) & 8796093022207) | ((((long)src[12]) << 43) & 2251799813685247) | ((((long)src[13]) << 51) & 9007199254740991);            dest[2] = ((((long)src[13]) >> 2) & 63) | ((((long)src[14]) << 6) & 16383) | ((((long)src[15]) << 14) & 4194303) | ((((long)src[16]) << 22) & 1073741823) | ((((long)src[17]) << 30) & 274877906943) | ((((long)src[18]) << 38) & 70368744177663) | ((((long)src[19]) << 46) & 9007199254740991);            dest[3] = ((((long)src[19]) >> 7) & 1) | ((((long)src[20]) << 1) & 511) | ((((long)src[21]) << 9) & 131071) | ((((long)src[22]) << 17) & 33554431) | ((((long)src[23]) << 25) & 8589934591) | ((((long)src[24]) << 33) & 2199023255551) | ((((long)src[25]) << 41) & 562949953421311) | ((((long)src[26]) << 49) & 9007199254740991);            dest[4] = ((((long)src[26]) >> 4) & 15) | ((((long)src[27]) << 4) & 4095) | ((((long)src[28]) << 12) & 1048575) | ((((long)src[29]) << 20) & 268435455) | ((((long)src[30]) << 28) & 68719476735) | ((((long)src[31]) << 36) & 17592186044415) | ((((long)src[32]) << 44) & 4503599627370495) | ((((long)src[33]) << 52) & 9007199254740991);            dest[5] = ((((long)src[33]) >> 1) & 127) | ((((long)src[34]) << 7) & 32767) | ((((long)src[35]) << 15) & 8388607) | ((((long)src[36]) << 23) & 2147483647) | ((((long)src[37]) << 31) & 549755813887) | ((((long)src[38]) << 39) & 140737488355327) | ((((long)src[39]) << 47) & 9007199254740991);            dest[6] = ((((long)src[39]) >> 6) & 3) | ((((long)src[40]) << 2) & 1023) | ((((long)src[41]) << 10) & 262143) | ((((long)src[42]) << 18) & 67108863) | ((((long)src[43]) << 26) & 17179869183) | ((((long)src[44]) << 34) & 4398046511103) | ((((long)src[45]) << 42) & 1125899906842623) | ((((long)src[46]) << 50) & 9007199254740991);            dest[7] = ((((long)src[46]) >> 3) & 31) | ((((long)src[47]) << 5) & 8191) | ((((long)src[48]) << 13) & 2097151) | ((((long)src[49]) << 21) & 536870911) | ((((long)src[50]) << 29) & 137438953471) | ((((long)src[51]) << 37) & 35184372088831) | ((((long)src[52]) << 45) & 9007199254740991);        }

        private static void Pack8LongValuesLE53(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 9007199254740991))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 9007199254740991) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 9007199254740991) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 9007199254740991) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 9007199254740991) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 9007199254740991) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 9007199254740991) >> 48)                | ((src[1] & 9007199254740991) << 5)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 9007199254740991) >> 3)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 9007199254740991) >> 11)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 9007199254740991) >> 19)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 9007199254740991) >> 27)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 9007199254740991) >> 35)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 9007199254740991) >> 43)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 9007199254740991) >> 51)                | ((src[2] & 9007199254740991) << 2)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 9007199254740991) >> 6)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 9007199254740991) >> 14)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 9007199254740991) >> 22)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 9007199254740991) >> 30)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 9007199254740991) >> 38)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 9007199254740991) >> 46)                | ((src[3] & 9007199254740991) << 7)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 9007199254740991) >> 1)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 9007199254740991) >> 9)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 9007199254740991) >> 17)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 9007199254740991) >> 25)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 9007199254740991) >> 33)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 9007199254740991) >> 41)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 9007199254740991) >> 49)                | ((src[4] & 9007199254740991) << 4)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 9007199254740991) >> 4)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 9007199254740991) >> 12)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 9007199254740991) >> 20)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 9007199254740991) >> 28)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 9007199254740991) >> 36)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 9007199254740991) >> 44)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 9007199254740991) >> 52)                | ((src[5] & 9007199254740991) << 1)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 9007199254740991) >> 7)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 9007199254740991) >> 15)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 9007199254740991) >> 23)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 9007199254740991) >> 31)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 9007199254740991) >> 39)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 9007199254740991) >> 47)                | ((src[6] & 9007199254740991) << 6)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 9007199254740991) >> 2)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 9007199254740991) >> 10)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 9007199254740991) >> 18)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 9007199254740991) >> 26)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 9007199254740991) >> 34)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 9007199254740991) >> 42)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 9007199254740991) >> 50)                | ((src[7] & 9007199254740991) << 3)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 9007199254740991) >> 5)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 9007199254740991) >> 13)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 9007199254740991) >> 21)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 9007199254740991) >> 29)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 9007199254740991) >> 37)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 9007199254740991) >> 45)) & 255);
                        }
        private static void Unpack8LongValuesBE53(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 45) & 9007199254740991) | ((((long)src[1]) << 37) & 35184372088831) | ((((long)src[2]) << 29) & 137438953471) | ((((long)src[3]) << 21) & 536870911) | ((((long)src[4]) << 13) & 2097151) | ((((long)src[5]) << 5) & 8191) | ((((long)src[6]) >> 3) & 31);            dest[1] = ((((long)src[6]) << 50) & 9007199254740991) | ((((long)src[7]) << 42) & 1125899906842623) | ((((long)src[8]) << 34) & 4398046511103) | ((((long)src[9]) << 26) & 17179869183) | ((((long)src[10]) << 18) & 67108863) | ((((long)src[11]) << 10) & 262143) | ((((long)src[12]) << 2) & 1023) | ((((long)src[13]) >> 6) & 3);            dest[2] = ((((long)src[13]) << 47) & 9007199254740991) | ((((long)src[14]) << 39) & 140737488355327) | ((((long)src[15]) << 31) & 549755813887) | ((((long)src[16]) << 23) & 2147483647) | ((((long)src[17]) << 15) & 8388607) | ((((long)src[18]) << 7) & 32767) | ((((long)src[19]) >> 1) & 127);            dest[3] = ((((long)src[19]) << 52) & 9007199254740991) | ((((long)src[20]) << 44) & 4503599627370495) | ((((long)src[21]) << 36) & 17592186044415) | ((((long)src[22]) << 28) & 68719476735) | ((((long)src[23]) << 20) & 268435455) | ((((long)src[24]) << 12) & 1048575) | ((((long)src[25]) << 4) & 4095) | ((((long)src[26]) >> 4) & 15);            dest[4] = ((((long)src[26]) << 49) & 9007199254740991) | ((((long)src[27]) << 41) & 562949953421311) | ((((long)src[28]) << 33) & 2199023255551) | ((((long)src[29]) << 25) & 8589934591) | ((((long)src[30]) << 17) & 33554431) | ((((long)src[31]) << 9) & 131071) | ((((long)src[32]) << 1) & 511) | ((((long)src[33]) >> 7) & 1);            dest[5] = ((((long)src[33]) << 46) & 9007199254740991) | ((((long)src[34]) << 38) & 70368744177663) | ((((long)src[35]) << 30) & 274877906943) | ((((long)src[36]) << 22) & 1073741823) | ((((long)src[37]) << 14) & 4194303) | ((((long)src[38]) << 6) & 16383) | ((((long)src[39]) >> 2) & 63);            dest[6] = ((((long)src[39]) << 51) & 9007199254740991) | ((((long)src[40]) << 43) & 2251799813685247) | ((((long)src[41]) << 35) & 8796093022207) | ((((long)src[42]) << 27) & 34359738367) | ((((long)src[43]) << 19) & 134217727) | ((((long)src[44]) << 11) & 524287) | ((((long)src[45]) << 3) & 2047) | ((((long)src[46]) >> 5) & 7);            dest[7] = ((((long)src[46]) << 48) & 9007199254740991) | ((((long)src[47]) << 40) & 281474976710655) | ((((long)src[48]) << 32) & 1099511627775) | ((((long)src[49]) << 24) & 4294967295) | ((((long)src[50]) << 16) & 16777215) | ((((long)src[51]) << 8) & 65535) | ((((long)src[52])) & 255);        }

        private static void Pack8LongValuesBE53(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 9007199254740991) >> 45)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 9007199254740991) >> 37)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 9007199254740991) >> 29)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 9007199254740991) >> 21)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 9007199254740991) >> 13)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 9007199254740991) >> 5)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 9007199254740991) << 3)                | ((src[1] & 9007199254740991) >> 50)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 9007199254740991) >> 42)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 9007199254740991) >> 34)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 9007199254740991) >> 26)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 9007199254740991) >> 18)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 9007199254740991) >> 10)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 9007199254740991) >> 2)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 9007199254740991) << 6)                | ((src[2] & 9007199254740991) >> 47)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 9007199254740991) >> 39)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 9007199254740991) >> 31)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 9007199254740991) >> 23)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 9007199254740991) >> 15)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 9007199254740991) >> 7)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 9007199254740991) << 1)                | ((src[3] & 9007199254740991) >> 52)) & 255);
                            dest[20] = 
                (byte)((((src[3] & 9007199254740991) >> 44)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 9007199254740991) >> 36)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 9007199254740991) >> 28)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 9007199254740991) >> 20)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 9007199254740991) >> 12)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 9007199254740991) >> 4)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 9007199254740991) << 4)                | ((src[4] & 9007199254740991) >> 49)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 9007199254740991) >> 41)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 9007199254740991) >> 33)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 9007199254740991) >> 25)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 9007199254740991) >> 17)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 9007199254740991) >> 9)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 9007199254740991) >> 1)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 9007199254740991) << 7)                | ((src[5] & 9007199254740991) >> 46)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 9007199254740991) >> 38)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 9007199254740991) >> 30)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 9007199254740991) >> 22)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 9007199254740991) >> 14)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 9007199254740991) >> 6)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 9007199254740991) << 2)                | ((src[6] & 9007199254740991) >> 51)) & 255);
                            dest[40] = 
                (byte)((((src[6] & 9007199254740991) >> 43)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 9007199254740991) >> 35)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 9007199254740991) >> 27)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 9007199254740991) >> 19)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 9007199254740991) >> 11)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 9007199254740991) >> 3)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 9007199254740991) << 5)                | ((src[7] & 9007199254740991) >> 48)) & 255);
                            dest[47] = 
                (byte)((((src[7] & 9007199254740991) >> 40)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 9007199254740991) >> 32)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 9007199254740991) >> 24)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 9007199254740991) >> 16)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 9007199254740991) >> 8)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 9007199254740991))) & 255);
                        }
        private static void Unpack8LongValuesLE54(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 18014398509481983);            dest[1] = ((((long)src[6]) >> 6) & 3) | ((((long)src[7]) << 2) & 1023) | ((((long)src[8]) << 10) & 262143) | ((((long)src[9]) << 18) & 67108863) | ((((long)src[10]) << 26) & 17179869183) | ((((long)src[11]) << 34) & 4398046511103) | ((((long)src[12]) << 42) & 1125899906842623) | ((((long)src[13]) << 50) & 18014398509481983);            dest[2] = ((((long)src[13]) >> 4) & 15) | ((((long)src[14]) << 4) & 4095) | ((((long)src[15]) << 12) & 1048575) | ((((long)src[16]) << 20) & 268435455) | ((((long)src[17]) << 28) & 68719476735) | ((((long)src[18]) << 36) & 17592186044415) | ((((long)src[19]) << 44) & 4503599627370495) | ((((long)src[20]) << 52) & 18014398509481983);            dest[3] = ((((long)src[20]) >> 2) & 63) | ((((long)src[21]) << 6) & 16383) | ((((long)src[22]) << 14) & 4194303) | ((((long)src[23]) << 22) & 1073741823) | ((((long)src[24]) << 30) & 274877906943) | ((((long)src[25]) << 38) & 70368744177663) | ((((long)src[26]) << 46) & 18014398509481983);            dest[4] = ((((long)src[27])) & 255) | ((((long)src[28]) << 8) & 65535) | ((((long)src[29]) << 16) & 16777215) | ((((long)src[30]) << 24) & 4294967295) | ((((long)src[31]) << 32) & 1099511627775) | ((((long)src[32]) << 40) & 281474976710655) | ((((long)src[33]) << 48) & 18014398509481983);            dest[5] = ((((long)src[33]) >> 6) & 3) | ((((long)src[34]) << 2) & 1023) | ((((long)src[35]) << 10) & 262143) | ((((long)src[36]) << 18) & 67108863) | ((((long)src[37]) << 26) & 17179869183) | ((((long)src[38]) << 34) & 4398046511103) | ((((long)src[39]) << 42) & 1125899906842623) | ((((long)src[40]) << 50) & 18014398509481983);            dest[6] = ((((long)src[40]) >> 4) & 15) | ((((long)src[41]) << 4) & 4095) | ((((long)src[42]) << 12) & 1048575) | ((((long)src[43]) << 20) & 268435455) | ((((long)src[44]) << 28) & 68719476735) | ((((long)src[45]) << 36) & 17592186044415) | ((((long)src[46]) << 44) & 4503599627370495) | ((((long)src[47]) << 52) & 18014398509481983);            dest[7] = ((((long)src[47]) >> 2) & 63) | ((((long)src[48]) << 6) & 16383) | ((((long)src[49]) << 14) & 4194303) | ((((long)src[50]) << 22) & 1073741823) | ((((long)src[51]) << 30) & 274877906943) | ((((long)src[52]) << 38) & 70368744177663) | ((((long)src[53]) << 46) & 18014398509481983);        }

        private static void Pack8LongValuesLE54(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 18014398509481983))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 18014398509481983) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 18014398509481983) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 18014398509481983) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 18014398509481983) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 18014398509481983) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 18014398509481983) >> 48)                | ((src[1] & 18014398509481983) << 6)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 18014398509481983) >> 2)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 18014398509481983) >> 10)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 18014398509481983) >> 18)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 18014398509481983) >> 26)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 18014398509481983) >> 34)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 18014398509481983) >> 42)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 18014398509481983) >> 50)                | ((src[2] & 18014398509481983) << 4)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 18014398509481983) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 18014398509481983) >> 12)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 18014398509481983) >> 20)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 18014398509481983) >> 28)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 18014398509481983) >> 36)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 18014398509481983) >> 44)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 18014398509481983) >> 52)                | ((src[3] & 18014398509481983) << 2)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 18014398509481983) >> 6)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 18014398509481983) >> 14)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 18014398509481983) >> 22)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 18014398509481983) >> 30)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 18014398509481983) >> 38)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 18014398509481983) >> 46)) & 255);
                            dest[27] = 
                (byte)((((src[4] & 18014398509481983))) & 255);
                            dest[28] = 
                (byte)((((src[4] & 18014398509481983) >> 8)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 18014398509481983) >> 16)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 18014398509481983) >> 24)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 18014398509481983) >> 32)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 18014398509481983) >> 40)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 18014398509481983) >> 48)                | ((src[5] & 18014398509481983) << 6)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 18014398509481983) >> 2)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 18014398509481983) >> 10)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 18014398509481983) >> 18)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 18014398509481983) >> 26)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 18014398509481983) >> 34)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 18014398509481983) >> 42)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 18014398509481983) >> 50)                | ((src[6] & 18014398509481983) << 4)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 18014398509481983) >> 4)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 18014398509481983) >> 12)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 18014398509481983) >> 20)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 18014398509481983) >> 28)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 18014398509481983) >> 36)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 18014398509481983) >> 44)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 18014398509481983) >> 52)                | ((src[7] & 18014398509481983) << 2)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 18014398509481983) >> 6)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 18014398509481983) >> 14)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 18014398509481983) >> 22)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 18014398509481983) >> 30)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 18014398509481983) >> 38)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 18014398509481983) >> 46)) & 255);
                        }
        private static void Unpack8LongValuesBE54(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 46) & 18014398509481983) | ((((long)src[1]) << 38) & 70368744177663) | ((((long)src[2]) << 30) & 274877906943) | ((((long)src[3]) << 22) & 1073741823) | ((((long)src[4]) << 14) & 4194303) | ((((long)src[5]) << 6) & 16383) | ((((long)src[6]) >> 2) & 63);            dest[1] = ((((long)src[6]) << 52) & 18014398509481983) | ((((long)src[7]) << 44) & 4503599627370495) | ((((long)src[8]) << 36) & 17592186044415) | ((((long)src[9]) << 28) & 68719476735) | ((((long)src[10]) << 20) & 268435455) | ((((long)src[11]) << 12) & 1048575) | ((((long)src[12]) << 4) & 4095) | ((((long)src[13]) >> 4) & 15);            dest[2] = ((((long)src[13]) << 50) & 18014398509481983) | ((((long)src[14]) << 42) & 1125899906842623) | ((((long)src[15]) << 34) & 4398046511103) | ((((long)src[16]) << 26) & 17179869183) | ((((long)src[17]) << 18) & 67108863) | ((((long)src[18]) << 10) & 262143) | ((((long)src[19]) << 2) & 1023) | ((((long)src[20]) >> 6) & 3);            dest[3] = ((((long)src[20]) << 48) & 18014398509481983) | ((((long)src[21]) << 40) & 281474976710655) | ((((long)src[22]) << 32) & 1099511627775) | ((((long)src[23]) << 24) & 4294967295) | ((((long)src[24]) << 16) & 16777215) | ((((long)src[25]) << 8) & 65535) | ((((long)src[26])) & 255);            dest[4] = ((((long)src[27]) << 46) & 18014398509481983) | ((((long)src[28]) << 38) & 70368744177663) | ((((long)src[29]) << 30) & 274877906943) | ((((long)src[30]) << 22) & 1073741823) | ((((long)src[31]) << 14) & 4194303) | ((((long)src[32]) << 6) & 16383) | ((((long)src[33]) >> 2) & 63);            dest[5] = ((((long)src[33]) << 52) & 18014398509481983) | ((((long)src[34]) << 44) & 4503599627370495) | ((((long)src[35]) << 36) & 17592186044415) | ((((long)src[36]) << 28) & 68719476735) | ((((long)src[37]) << 20) & 268435455) | ((((long)src[38]) << 12) & 1048575) | ((((long)src[39]) << 4) & 4095) | ((((long)src[40]) >> 4) & 15);            dest[6] = ((((long)src[40]) << 50) & 18014398509481983) | ((((long)src[41]) << 42) & 1125899906842623) | ((((long)src[42]) << 34) & 4398046511103) | ((((long)src[43]) << 26) & 17179869183) | ((((long)src[44]) << 18) & 67108863) | ((((long)src[45]) << 10) & 262143) | ((((long)src[46]) << 2) & 1023) | ((((long)src[47]) >> 6) & 3);            dest[7] = ((((long)src[47]) << 48) & 18014398509481983) | ((((long)src[48]) << 40) & 281474976710655) | ((((long)src[49]) << 32) & 1099511627775) | ((((long)src[50]) << 24) & 4294967295) | ((((long)src[51]) << 16) & 16777215) | ((((long)src[52]) << 8) & 65535) | ((((long)src[53])) & 255);        }

        private static void Pack8LongValuesBE54(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 18014398509481983) >> 46)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 18014398509481983) >> 38)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 18014398509481983) >> 30)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 18014398509481983) >> 22)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 18014398509481983) >> 14)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 18014398509481983) >> 6)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 18014398509481983) << 2)                | ((src[1] & 18014398509481983) >> 52)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 18014398509481983) >> 44)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 18014398509481983) >> 36)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 18014398509481983) >> 28)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 18014398509481983) >> 20)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 18014398509481983) >> 12)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 18014398509481983) >> 4)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 18014398509481983) << 4)                | ((src[2] & 18014398509481983) >> 50)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 18014398509481983) >> 42)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 18014398509481983) >> 34)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 18014398509481983) >> 26)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 18014398509481983) >> 18)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 18014398509481983) >> 10)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 18014398509481983) >> 2)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 18014398509481983) << 6)                | ((src[3] & 18014398509481983) >> 48)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 18014398509481983) >> 40)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 18014398509481983) >> 32)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 18014398509481983) >> 24)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 18014398509481983) >> 16)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 18014398509481983) >> 8)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 18014398509481983))) & 255);
                            dest[27] = 
                (byte)((((src[4] & 18014398509481983) >> 46)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 18014398509481983) >> 38)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 18014398509481983) >> 30)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 18014398509481983) >> 22)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 18014398509481983) >> 14)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 18014398509481983) >> 6)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 18014398509481983) << 2)                | ((src[5] & 18014398509481983) >> 52)) & 255);
                            dest[34] = 
                (byte)((((src[5] & 18014398509481983) >> 44)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 18014398509481983) >> 36)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 18014398509481983) >> 28)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 18014398509481983) >> 20)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 18014398509481983) >> 12)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 18014398509481983) >> 4)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 18014398509481983) << 4)                | ((src[6] & 18014398509481983) >> 50)) & 255);
                            dest[41] = 
                (byte)((((src[6] & 18014398509481983) >> 42)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 18014398509481983) >> 34)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 18014398509481983) >> 26)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 18014398509481983) >> 18)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 18014398509481983) >> 10)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 18014398509481983) >> 2)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 18014398509481983) << 6)                | ((src[7] & 18014398509481983) >> 48)) & 255);
                            dest[48] = 
                (byte)((((src[7] & 18014398509481983) >> 40)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 18014398509481983) >> 32)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 18014398509481983) >> 24)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 18014398509481983) >> 16)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 18014398509481983) >> 8)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 18014398509481983))) & 255);
                        }
        private static void Unpack8LongValuesLE55(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 36028797018963967);            dest[1] = ((((long)src[6]) >> 7) & 1) | ((((long)src[7]) << 1) & 511) | ((((long)src[8]) << 9) & 131071) | ((((long)src[9]) << 17) & 33554431) | ((((long)src[10]) << 25) & 8589934591) | ((((long)src[11]) << 33) & 2199023255551) | ((((long)src[12]) << 41) & 562949953421311) | ((((long)src[13]) << 49) & 36028797018963967);            dest[2] = ((((long)src[13]) >> 6) & 3) | ((((long)src[14]) << 2) & 1023) | ((((long)src[15]) << 10) & 262143) | ((((long)src[16]) << 18) & 67108863) | ((((long)src[17]) << 26) & 17179869183) | ((((long)src[18]) << 34) & 4398046511103) | ((((long)src[19]) << 42) & 1125899906842623) | ((((long)src[20]) << 50) & 36028797018963967);            dest[3] = ((((long)src[20]) >> 5) & 7) | ((((long)src[21]) << 3) & 2047) | ((((long)src[22]) << 11) & 524287) | ((((long)src[23]) << 19) & 134217727) | ((((long)src[24]) << 27) & 34359738367) | ((((long)src[25]) << 35) & 8796093022207) | ((((long)src[26]) << 43) & 2251799813685247) | ((((long)src[27]) << 51) & 36028797018963967);            dest[4] = ((((long)src[27]) >> 4) & 15) | ((((long)src[28]) << 4) & 4095) | ((((long)src[29]) << 12) & 1048575) | ((((long)src[30]) << 20) & 268435455) | ((((long)src[31]) << 28) & 68719476735) | ((((long)src[32]) << 36) & 17592186044415) | ((((long)src[33]) << 44) & 4503599627370495) | ((((long)src[34]) << 52) & 36028797018963967);            dest[5] = ((((long)src[34]) >> 3) & 31) | ((((long)src[35]) << 5) & 8191) | ((((long)src[36]) << 13) & 2097151) | ((((long)src[37]) << 21) & 536870911) | ((((long)src[38]) << 29) & 137438953471) | ((((long)src[39]) << 37) & 35184372088831) | ((((long)src[40]) << 45) & 9007199254740991) | ((((long)src[41]) << 53) & 36028797018963967);            dest[6] = ((((long)src[41]) >> 2) & 63) | ((((long)src[42]) << 6) & 16383) | ((((long)src[43]) << 14) & 4194303) | ((((long)src[44]) << 22) & 1073741823) | ((((long)src[45]) << 30) & 274877906943) | ((((long)src[46]) << 38) & 70368744177663) | ((((long)src[47]) << 46) & 18014398509481983) | ((((long)src[48]) << 54) & 36028797018963967);            dest[7] = ((((long)src[48]) >> 1) & 127) | ((((long)src[49]) << 7) & 32767) | ((((long)src[50]) << 15) & 8388607) | ((((long)src[51]) << 23) & 2147483647) | ((((long)src[52]) << 31) & 549755813887) | ((((long)src[53]) << 39) & 140737488355327) | ((((long)src[54]) << 47) & 36028797018963967);        }

        private static void Pack8LongValuesLE55(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 36028797018963967))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 36028797018963967) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 36028797018963967) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 36028797018963967) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 36028797018963967) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 36028797018963967) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 36028797018963967) >> 48)                | ((src[1] & 36028797018963967) << 7)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 36028797018963967) >> 1)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 36028797018963967) >> 9)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 36028797018963967) >> 17)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 36028797018963967) >> 25)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 36028797018963967) >> 33)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 36028797018963967) >> 41)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 36028797018963967) >> 49)                | ((src[2] & 36028797018963967) << 6)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 36028797018963967) >> 2)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 36028797018963967) >> 10)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 36028797018963967) >> 18)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 36028797018963967) >> 26)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 36028797018963967) >> 34)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 36028797018963967) >> 42)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 36028797018963967) >> 50)                | ((src[3] & 36028797018963967) << 5)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 36028797018963967) >> 3)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 36028797018963967) >> 11)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 36028797018963967) >> 19)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 36028797018963967) >> 27)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 36028797018963967) >> 35)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 36028797018963967) >> 43)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 36028797018963967) >> 51)                | ((src[4] & 36028797018963967) << 4)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 36028797018963967) >> 4)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 36028797018963967) >> 12)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 36028797018963967) >> 20)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 36028797018963967) >> 28)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 36028797018963967) >> 36)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 36028797018963967) >> 44)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 36028797018963967) >> 52)                | ((src[5] & 36028797018963967) << 3)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 36028797018963967) >> 5)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 36028797018963967) >> 13)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 36028797018963967) >> 21)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 36028797018963967) >> 29)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 36028797018963967) >> 37)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 36028797018963967) >> 45)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 36028797018963967) >> 53)                | ((src[6] & 36028797018963967) << 2)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 36028797018963967) >> 6)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 36028797018963967) >> 14)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 36028797018963967) >> 22)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 36028797018963967) >> 30)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 36028797018963967) >> 38)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 36028797018963967) >> 46)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 36028797018963967) >> 54)                | ((src[7] & 36028797018963967) << 1)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 36028797018963967) >> 7)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 36028797018963967) >> 15)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 36028797018963967) >> 23)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 36028797018963967) >> 31)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 36028797018963967) >> 39)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 36028797018963967) >> 47)) & 255);
                        }
        private static void Unpack8LongValuesBE55(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 47) & 36028797018963967) | ((((long)src[1]) << 39) & 140737488355327) | ((((long)src[2]) << 31) & 549755813887) | ((((long)src[3]) << 23) & 2147483647) | ((((long)src[4]) << 15) & 8388607) | ((((long)src[5]) << 7) & 32767) | ((((long)src[6]) >> 1) & 127);            dest[1] = ((((long)src[6]) << 54) & 36028797018963967) | ((((long)src[7]) << 46) & 18014398509481983) | ((((long)src[8]) << 38) & 70368744177663) | ((((long)src[9]) << 30) & 274877906943) | ((((long)src[10]) << 22) & 1073741823) | ((((long)src[11]) << 14) & 4194303) | ((((long)src[12]) << 6) & 16383) | ((((long)src[13]) >> 2) & 63);            dest[2] = ((((long)src[13]) << 53) & 36028797018963967) | ((((long)src[14]) << 45) & 9007199254740991) | ((((long)src[15]) << 37) & 35184372088831) | ((((long)src[16]) << 29) & 137438953471) | ((((long)src[17]) << 21) & 536870911) | ((((long)src[18]) << 13) & 2097151) | ((((long)src[19]) << 5) & 8191) | ((((long)src[20]) >> 3) & 31);            dest[3] = ((((long)src[20]) << 52) & 36028797018963967) | ((((long)src[21]) << 44) & 4503599627370495) | ((((long)src[22]) << 36) & 17592186044415) | ((((long)src[23]) << 28) & 68719476735) | ((((long)src[24]) << 20) & 268435455) | ((((long)src[25]) << 12) & 1048575) | ((((long)src[26]) << 4) & 4095) | ((((long)src[27]) >> 4) & 15);            dest[4] = ((((long)src[27]) << 51) & 36028797018963967) | ((((long)src[28]) << 43) & 2251799813685247) | ((((long)src[29]) << 35) & 8796093022207) | ((((long)src[30]) << 27) & 34359738367) | ((((long)src[31]) << 19) & 134217727) | ((((long)src[32]) << 11) & 524287) | ((((long)src[33]) << 3) & 2047) | ((((long)src[34]) >> 5) & 7);            dest[5] = ((((long)src[34]) << 50) & 36028797018963967) | ((((long)src[35]) << 42) & 1125899906842623) | ((((long)src[36]) << 34) & 4398046511103) | ((((long)src[37]) << 26) & 17179869183) | ((((long)src[38]) << 18) & 67108863) | ((((long)src[39]) << 10) & 262143) | ((((long)src[40]) << 2) & 1023) | ((((long)src[41]) >> 6) & 3);            dest[6] = ((((long)src[41]) << 49) & 36028797018963967) | ((((long)src[42]) << 41) & 562949953421311) | ((((long)src[43]) << 33) & 2199023255551) | ((((long)src[44]) << 25) & 8589934591) | ((((long)src[45]) << 17) & 33554431) | ((((long)src[46]) << 9) & 131071) | ((((long)src[47]) << 1) & 511) | ((((long)src[48]) >> 7) & 1);            dest[7] = ((((long)src[48]) << 48) & 36028797018963967) | ((((long)src[49]) << 40) & 281474976710655) | ((((long)src[50]) << 32) & 1099511627775) | ((((long)src[51]) << 24) & 4294967295) | ((((long)src[52]) << 16) & 16777215) | ((((long)src[53]) << 8) & 65535) | ((((long)src[54])) & 255);        }

        private static void Pack8LongValuesBE55(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 36028797018963967) >> 47)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 36028797018963967) >> 39)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 36028797018963967) >> 31)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 36028797018963967) >> 23)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 36028797018963967) >> 15)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 36028797018963967) >> 7)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 36028797018963967) << 1)                | ((src[1] & 36028797018963967) >> 54)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 36028797018963967) >> 46)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 36028797018963967) >> 38)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 36028797018963967) >> 30)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 36028797018963967) >> 22)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 36028797018963967) >> 14)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 36028797018963967) >> 6)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 36028797018963967) << 2)                | ((src[2] & 36028797018963967) >> 53)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 36028797018963967) >> 45)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 36028797018963967) >> 37)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 36028797018963967) >> 29)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 36028797018963967) >> 21)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 36028797018963967) >> 13)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 36028797018963967) >> 5)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 36028797018963967) << 3)                | ((src[3] & 36028797018963967) >> 52)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 36028797018963967) >> 44)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 36028797018963967) >> 36)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 36028797018963967) >> 28)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 36028797018963967) >> 20)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 36028797018963967) >> 12)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 36028797018963967) >> 4)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 36028797018963967) << 4)                | ((src[4] & 36028797018963967) >> 51)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 36028797018963967) >> 43)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 36028797018963967) >> 35)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 36028797018963967) >> 27)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 36028797018963967) >> 19)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 36028797018963967) >> 11)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 36028797018963967) >> 3)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 36028797018963967) << 5)                | ((src[5] & 36028797018963967) >> 50)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 36028797018963967) >> 42)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 36028797018963967) >> 34)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 36028797018963967) >> 26)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 36028797018963967) >> 18)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 36028797018963967) >> 10)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 36028797018963967) >> 2)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 36028797018963967) << 6)                | ((src[6] & 36028797018963967) >> 49)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 36028797018963967) >> 41)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 36028797018963967) >> 33)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 36028797018963967) >> 25)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 36028797018963967) >> 17)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 36028797018963967) >> 9)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 36028797018963967) >> 1)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 36028797018963967) << 7)                | ((src[7] & 36028797018963967) >> 48)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 36028797018963967) >> 40)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 36028797018963967) >> 32)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 36028797018963967) >> 24)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 36028797018963967) >> 16)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 36028797018963967) >> 8)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 36028797018963967))) & 255);
                        }
        private static void Unpack8LongValuesLE56(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935);            dest[1] = ((((long)src[7])) & 255) | ((((long)src[8]) << 8) & 65535) | ((((long)src[9]) << 16) & 16777215) | ((((long)src[10]) << 24) & 4294967295) | ((((long)src[11]) << 32) & 1099511627775) | ((((long)src[12]) << 40) & 281474976710655) | ((((long)src[13]) << 48) & 72057594037927935);            dest[2] = ((((long)src[14])) & 255) | ((((long)src[15]) << 8) & 65535) | ((((long)src[16]) << 16) & 16777215) | ((((long)src[17]) << 24) & 4294967295) | ((((long)src[18]) << 32) & 1099511627775) | ((((long)src[19]) << 40) & 281474976710655) | ((((long)src[20]) << 48) & 72057594037927935);            dest[3] = ((((long)src[21])) & 255) | ((((long)src[22]) << 8) & 65535) | ((((long)src[23]) << 16) & 16777215) | ((((long)src[24]) << 24) & 4294967295) | ((((long)src[25]) << 32) & 1099511627775) | ((((long)src[26]) << 40) & 281474976710655) | ((((long)src[27]) << 48) & 72057594037927935);            dest[4] = ((((long)src[28])) & 255) | ((((long)src[29]) << 8) & 65535) | ((((long)src[30]) << 16) & 16777215) | ((((long)src[31]) << 24) & 4294967295) | ((((long)src[32]) << 32) & 1099511627775) | ((((long)src[33]) << 40) & 281474976710655) | ((((long)src[34]) << 48) & 72057594037927935);            dest[5] = ((((long)src[35])) & 255) | ((((long)src[36]) << 8) & 65535) | ((((long)src[37]) << 16) & 16777215) | ((((long)src[38]) << 24) & 4294967295) | ((((long)src[39]) << 32) & 1099511627775) | ((((long)src[40]) << 40) & 281474976710655) | ((((long)src[41]) << 48) & 72057594037927935);            dest[6] = ((((long)src[42])) & 255) | ((((long)src[43]) << 8) & 65535) | ((((long)src[44]) << 16) & 16777215) | ((((long)src[45]) << 24) & 4294967295) | ((((long)src[46]) << 32) & 1099511627775) | ((((long)src[47]) << 40) & 281474976710655) | ((((long)src[48]) << 48) & 72057594037927935);            dest[7] = ((((long)src[49])) & 255) | ((((long)src[50]) << 8) & 65535) | ((((long)src[51]) << 16) & 16777215) | ((((long)src[52]) << 24) & 4294967295) | ((((long)src[53]) << 32) & 1099511627775) | ((((long)src[54]) << 40) & 281474976710655) | ((((long)src[55]) << 48) & 72057594037927935);        }

        private static void Pack8LongValuesLE56(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 72057594037927935))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 72057594037927935) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 72057594037927935) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 72057594037927935) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 72057594037927935) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 72057594037927935) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 72057594037927935) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[1] & 72057594037927935))) & 255);
                            dest[8] = 
                (byte)((((src[1] & 72057594037927935) >> 8)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 72057594037927935) >> 16)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 72057594037927935) >> 24)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 72057594037927935) >> 32)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 72057594037927935) >> 40)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 72057594037927935) >> 48)) & 255);
                            dest[14] = 
                (byte)((((src[2] & 72057594037927935))) & 255);
                            dest[15] = 
                (byte)((((src[2] & 72057594037927935) >> 8)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 72057594037927935) >> 16)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 72057594037927935) >> 24)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 72057594037927935) >> 32)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 72057594037927935) >> 40)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 72057594037927935) >> 48)) & 255);
                            dest[21] = 
                (byte)((((src[3] & 72057594037927935))) & 255);
                            dest[22] = 
                (byte)((((src[3] & 72057594037927935) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 72057594037927935) >> 16)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 72057594037927935) >> 24)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 72057594037927935) >> 32)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 72057594037927935) >> 40)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 72057594037927935) >> 48)) & 255);
                            dest[28] = 
                (byte)((((src[4] & 72057594037927935))) & 255);
                            dest[29] = 
                (byte)((((src[4] & 72057594037927935) >> 8)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 72057594037927935) >> 16)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 72057594037927935) >> 24)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 72057594037927935) >> 32)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 72057594037927935) >> 40)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 72057594037927935) >> 48)) & 255);
                            dest[35] = 
                (byte)((((src[5] & 72057594037927935))) & 255);
                            dest[36] = 
                (byte)((((src[5] & 72057594037927935) >> 8)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 72057594037927935) >> 16)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 72057594037927935) >> 24)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 72057594037927935) >> 32)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 72057594037927935) >> 40)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 72057594037927935) >> 48)) & 255);
                            dest[42] = 
                (byte)((((src[6] & 72057594037927935))) & 255);
                            dest[43] = 
                (byte)((((src[6] & 72057594037927935) >> 8)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 72057594037927935) >> 16)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 72057594037927935) >> 24)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 72057594037927935) >> 32)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 72057594037927935) >> 40)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 72057594037927935) >> 48)) & 255);
                            dest[49] = 
                (byte)((((src[7] & 72057594037927935))) & 255);
                            dest[50] = 
                (byte)((((src[7] & 72057594037927935) >> 8)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 72057594037927935) >> 16)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 72057594037927935) >> 24)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 72057594037927935) >> 32)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 72057594037927935) >> 40)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 72057594037927935) >> 48)) & 255);
                        }
        private static void Unpack8LongValuesBE56(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 48) & 72057594037927935) | ((((long)src[1]) << 40) & 281474976710655) | ((((long)src[2]) << 32) & 1099511627775) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 16) & 16777215) | ((((long)src[5]) << 8) & 65535) | ((((long)src[6])) & 255);            dest[1] = ((((long)src[7]) << 48) & 72057594037927935) | ((((long)src[8]) << 40) & 281474976710655) | ((((long)src[9]) << 32) & 1099511627775) | ((((long)src[10]) << 24) & 4294967295) | ((((long)src[11]) << 16) & 16777215) | ((((long)src[12]) << 8) & 65535) | ((((long)src[13])) & 255);            dest[2] = ((((long)src[14]) << 48) & 72057594037927935) | ((((long)src[15]) << 40) & 281474976710655) | ((((long)src[16]) << 32) & 1099511627775) | ((((long)src[17]) << 24) & 4294967295) | ((((long)src[18]) << 16) & 16777215) | ((((long)src[19]) << 8) & 65535) | ((((long)src[20])) & 255);            dest[3] = ((((long)src[21]) << 48) & 72057594037927935) | ((((long)src[22]) << 40) & 281474976710655) | ((((long)src[23]) << 32) & 1099511627775) | ((((long)src[24]) << 24) & 4294967295) | ((((long)src[25]) << 16) & 16777215) | ((((long)src[26]) << 8) & 65535) | ((((long)src[27])) & 255);            dest[4] = ((((long)src[28]) << 48) & 72057594037927935) | ((((long)src[29]) << 40) & 281474976710655) | ((((long)src[30]) << 32) & 1099511627775) | ((((long)src[31]) << 24) & 4294967295) | ((((long)src[32]) << 16) & 16777215) | ((((long)src[33]) << 8) & 65535) | ((((long)src[34])) & 255);            dest[5] = ((((long)src[35]) << 48) & 72057594037927935) | ((((long)src[36]) << 40) & 281474976710655) | ((((long)src[37]) << 32) & 1099511627775) | ((((long)src[38]) << 24) & 4294967295) | ((((long)src[39]) << 16) & 16777215) | ((((long)src[40]) << 8) & 65535) | ((((long)src[41])) & 255);            dest[6] = ((((long)src[42]) << 48) & 72057594037927935) | ((((long)src[43]) << 40) & 281474976710655) | ((((long)src[44]) << 32) & 1099511627775) | ((((long)src[45]) << 24) & 4294967295) | ((((long)src[46]) << 16) & 16777215) | ((((long)src[47]) << 8) & 65535) | ((((long)src[48])) & 255);            dest[7] = ((((long)src[49]) << 48) & 72057594037927935) | ((((long)src[50]) << 40) & 281474976710655) | ((((long)src[51]) << 32) & 1099511627775) | ((((long)src[52]) << 24) & 4294967295) | ((((long)src[53]) << 16) & 16777215) | ((((long)src[54]) << 8) & 65535) | ((((long)src[55])) & 255);        }

        private static void Pack8LongValuesBE56(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 72057594037927935) >> 48)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 72057594037927935) >> 40)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 72057594037927935) >> 32)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 72057594037927935) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 72057594037927935) >> 16)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 72057594037927935) >> 8)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 72057594037927935))) & 255);
                            dest[7] = 
                (byte)((((src[1] & 72057594037927935) >> 48)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 72057594037927935) >> 40)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 72057594037927935) >> 32)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 72057594037927935) >> 24)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 72057594037927935) >> 16)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 72057594037927935) >> 8)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 72057594037927935))) & 255);
                            dest[14] = 
                (byte)((((src[2] & 72057594037927935) >> 48)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 72057594037927935) >> 40)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 72057594037927935) >> 32)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 72057594037927935) >> 24)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 72057594037927935) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 72057594037927935) >> 8)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 72057594037927935))) & 255);
                            dest[21] = 
                (byte)((((src[3] & 72057594037927935) >> 48)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 72057594037927935) >> 40)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 72057594037927935) >> 32)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 72057594037927935) >> 24)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 72057594037927935) >> 16)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 72057594037927935) >> 8)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 72057594037927935))) & 255);
                            dest[28] = 
                (byte)((((src[4] & 72057594037927935) >> 48)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 72057594037927935) >> 40)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 72057594037927935) >> 32)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 72057594037927935) >> 24)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 72057594037927935) >> 16)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 72057594037927935) >> 8)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 72057594037927935))) & 255);
                            dest[35] = 
                (byte)((((src[5] & 72057594037927935) >> 48)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 72057594037927935) >> 40)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 72057594037927935) >> 32)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 72057594037927935) >> 24)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 72057594037927935) >> 16)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 72057594037927935) >> 8)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 72057594037927935))) & 255);
                            dest[42] = 
                (byte)((((src[6] & 72057594037927935) >> 48)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 72057594037927935) >> 40)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 72057594037927935) >> 32)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 72057594037927935) >> 24)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 72057594037927935) >> 16)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 72057594037927935) >> 8)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 72057594037927935))) & 255);
                            dest[49] = 
                (byte)((((src[7] & 72057594037927935) >> 48)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 72057594037927935) >> 40)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 72057594037927935) >> 32)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 72057594037927935) >> 24)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 72057594037927935) >> 16)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 72057594037927935) >> 8)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 72057594037927935))) & 255);
                        }
        private static void Unpack8LongValuesLE57(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935) | ((((long)src[7]) << 56) & 144115188075855871);            dest[1] = ((((long)src[7]) >> 1) & 127) | ((((long)src[8]) << 7) & 32767) | ((((long)src[9]) << 15) & 8388607) | ((((long)src[10]) << 23) & 2147483647) | ((((long)src[11]) << 31) & 549755813887) | ((((long)src[12]) << 39) & 140737488355327) | ((((long)src[13]) << 47) & 36028797018963967) | ((((long)src[14]) << 55) & 144115188075855871);            dest[2] = ((((long)src[14]) >> 2) & 63) | ((((long)src[15]) << 6) & 16383) | ((((long)src[16]) << 14) & 4194303) | ((((long)src[17]) << 22) & 1073741823) | ((((long)src[18]) << 30) & 274877906943) | ((((long)src[19]) << 38) & 70368744177663) | ((((long)src[20]) << 46) & 18014398509481983) | ((((long)src[21]) << 54) & 144115188075855871);            dest[3] = ((((long)src[21]) >> 3) & 31) | ((((long)src[22]) << 5) & 8191) | ((((long)src[23]) << 13) & 2097151) | ((((long)src[24]) << 21) & 536870911) | ((((long)src[25]) << 29) & 137438953471) | ((((long)src[26]) << 37) & 35184372088831) | ((((long)src[27]) << 45) & 9007199254740991) | ((((long)src[28]) << 53) & 144115188075855871);            dest[4] = ((((long)src[28]) >> 4) & 15) | ((((long)src[29]) << 4) & 4095) | ((((long)src[30]) << 12) & 1048575) | ((((long)src[31]) << 20) & 268435455) | ((((long)src[32]) << 28) & 68719476735) | ((((long)src[33]) << 36) & 17592186044415) | ((((long)src[34]) << 44) & 4503599627370495) | ((((long)src[35]) << 52) & 144115188075855871);            dest[5] = ((((long)src[35]) >> 5) & 7) | ((((long)src[36]) << 3) & 2047) | ((((long)src[37]) << 11) & 524287) | ((((long)src[38]) << 19) & 134217727) | ((((long)src[39]) << 27) & 34359738367) | ((((long)src[40]) << 35) & 8796093022207) | ((((long)src[41]) << 43) & 2251799813685247) | ((((long)src[42]) << 51) & 144115188075855871);            dest[6] = ((((long)src[42]) >> 6) & 3) | ((((long)src[43]) << 2) & 1023) | ((((long)src[44]) << 10) & 262143) | ((((long)src[45]) << 18) & 67108863) | ((((long)src[46]) << 26) & 17179869183) | ((((long)src[47]) << 34) & 4398046511103) | ((((long)src[48]) << 42) & 1125899906842623) | ((((long)src[49]) << 50) & 144115188075855871);            dest[7] = ((((long)src[49]) >> 7) & 1) | ((((long)src[50]) << 1) & 511) | ((((long)src[51]) << 9) & 131071) | ((((long)src[52]) << 17) & 33554431) | ((((long)src[53]) << 25) & 8589934591) | ((((long)src[54]) << 33) & 2199023255551) | ((((long)src[55]) << 41) & 562949953421311) | ((((long)src[56]) << 49) & 144115188075855871);        }

        private static void Pack8LongValuesLE57(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 144115188075855871))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 144115188075855871) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 144115188075855871) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 144115188075855871) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 144115188075855871) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 144115188075855871) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 144115188075855871) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 144115188075855871) >> 56)                | ((src[1] & 144115188075855871) << 1)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 144115188075855871) >> 7)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 144115188075855871) >> 15)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 144115188075855871) >> 23)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 144115188075855871) >> 31)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 144115188075855871) >> 39)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 144115188075855871) >> 47)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 144115188075855871) >> 55)                | ((src[2] & 144115188075855871) << 2)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 144115188075855871) >> 6)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 144115188075855871) >> 14)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 144115188075855871) >> 22)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 144115188075855871) >> 30)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 144115188075855871) >> 38)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 144115188075855871) >> 46)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 144115188075855871) >> 54)                | ((src[3] & 144115188075855871) << 3)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 144115188075855871) >> 5)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 144115188075855871) >> 13)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 144115188075855871) >> 21)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 144115188075855871) >> 29)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 144115188075855871) >> 37)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 144115188075855871) >> 45)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 144115188075855871) >> 53)                | ((src[4] & 144115188075855871) << 4)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 144115188075855871) >> 4)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 144115188075855871) >> 12)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 144115188075855871) >> 20)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 144115188075855871) >> 28)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 144115188075855871) >> 36)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 144115188075855871) >> 44)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 144115188075855871) >> 52)                | ((src[5] & 144115188075855871) << 5)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 144115188075855871) >> 3)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 144115188075855871) >> 11)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 144115188075855871) >> 19)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 144115188075855871) >> 27)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 144115188075855871) >> 35)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 144115188075855871) >> 43)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 144115188075855871) >> 51)                | ((src[6] & 144115188075855871) << 6)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 144115188075855871) >> 2)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 144115188075855871) >> 10)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 144115188075855871) >> 18)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 144115188075855871) >> 26)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 144115188075855871) >> 34)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 144115188075855871) >> 42)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 144115188075855871) >> 50)                | ((src[7] & 144115188075855871) << 7)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 144115188075855871) >> 1)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 144115188075855871) >> 9)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 144115188075855871) >> 17)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 144115188075855871) >> 25)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 144115188075855871) >> 33)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 144115188075855871) >> 41)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 144115188075855871) >> 49)) & 255);
                        }
        private static void Unpack8LongValuesBE57(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 49) & 144115188075855871) | ((((long)src[1]) << 41) & 562949953421311) | ((((long)src[2]) << 33) & 2199023255551) | ((((long)src[3]) << 25) & 8589934591) | ((((long)src[4]) << 17) & 33554431) | ((((long)src[5]) << 9) & 131071) | ((((long)src[6]) << 1) & 511) | ((((long)src[7]) >> 7) & 1);            dest[1] = ((((long)src[7]) << 50) & 144115188075855871) | ((((long)src[8]) << 42) & 1125899906842623) | ((((long)src[9]) << 34) & 4398046511103) | ((((long)src[10]) << 26) & 17179869183) | ((((long)src[11]) << 18) & 67108863) | ((((long)src[12]) << 10) & 262143) | ((((long)src[13]) << 2) & 1023) | ((((long)src[14]) >> 6) & 3);            dest[2] = ((((long)src[14]) << 51) & 144115188075855871) | ((((long)src[15]) << 43) & 2251799813685247) | ((((long)src[16]) << 35) & 8796093022207) | ((((long)src[17]) << 27) & 34359738367) | ((((long)src[18]) << 19) & 134217727) | ((((long)src[19]) << 11) & 524287) | ((((long)src[20]) << 3) & 2047) | ((((long)src[21]) >> 5) & 7);            dest[3] = ((((long)src[21]) << 52) & 144115188075855871) | ((((long)src[22]) << 44) & 4503599627370495) | ((((long)src[23]) << 36) & 17592186044415) | ((((long)src[24]) << 28) & 68719476735) | ((((long)src[25]) << 20) & 268435455) | ((((long)src[26]) << 12) & 1048575) | ((((long)src[27]) << 4) & 4095) | ((((long)src[28]) >> 4) & 15);            dest[4] = ((((long)src[28]) << 53) & 144115188075855871) | ((((long)src[29]) << 45) & 9007199254740991) | ((((long)src[30]) << 37) & 35184372088831) | ((((long)src[31]) << 29) & 137438953471) | ((((long)src[32]) << 21) & 536870911) | ((((long)src[33]) << 13) & 2097151) | ((((long)src[34]) << 5) & 8191) | ((((long)src[35]) >> 3) & 31);            dest[5] = ((((long)src[35]) << 54) & 144115188075855871) | ((((long)src[36]) << 46) & 18014398509481983) | ((((long)src[37]) << 38) & 70368744177663) | ((((long)src[38]) << 30) & 274877906943) | ((((long)src[39]) << 22) & 1073741823) | ((((long)src[40]) << 14) & 4194303) | ((((long)src[41]) << 6) & 16383) | ((((long)src[42]) >> 2) & 63);            dest[6] = ((((long)src[42]) << 55) & 144115188075855871) | ((((long)src[43]) << 47) & 36028797018963967) | ((((long)src[44]) << 39) & 140737488355327) | ((((long)src[45]) << 31) & 549755813887) | ((((long)src[46]) << 23) & 2147483647) | ((((long)src[47]) << 15) & 8388607) | ((((long)src[48]) << 7) & 32767) | ((((long)src[49]) >> 1) & 127);            dest[7] = ((((long)src[49]) << 56) & 144115188075855871) | ((((long)src[50]) << 48) & 72057594037927935) | ((((long)src[51]) << 40) & 281474976710655) | ((((long)src[52]) << 32) & 1099511627775) | ((((long)src[53]) << 24) & 4294967295) | ((((long)src[54]) << 16) & 16777215) | ((((long)src[55]) << 8) & 65535) | ((((long)src[56])) & 255);        }

        private static void Pack8LongValuesBE57(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 144115188075855871) >> 49)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 144115188075855871) >> 41)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 144115188075855871) >> 33)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 144115188075855871) >> 25)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 144115188075855871) >> 17)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 144115188075855871) >> 9)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 144115188075855871) >> 1)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 144115188075855871) << 7)                | ((src[1] & 144115188075855871) >> 50)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 144115188075855871) >> 42)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 144115188075855871) >> 34)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 144115188075855871) >> 26)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 144115188075855871) >> 18)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 144115188075855871) >> 10)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 144115188075855871) >> 2)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 144115188075855871) << 6)                | ((src[2] & 144115188075855871) >> 51)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 144115188075855871) >> 43)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 144115188075855871) >> 35)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 144115188075855871) >> 27)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 144115188075855871) >> 19)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 144115188075855871) >> 11)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 144115188075855871) >> 3)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 144115188075855871) << 5)                | ((src[3] & 144115188075855871) >> 52)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 144115188075855871) >> 44)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 144115188075855871) >> 36)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 144115188075855871) >> 28)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 144115188075855871) >> 20)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 144115188075855871) >> 12)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 144115188075855871) >> 4)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 144115188075855871) << 4)                | ((src[4] & 144115188075855871) >> 53)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 144115188075855871) >> 45)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 144115188075855871) >> 37)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 144115188075855871) >> 29)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 144115188075855871) >> 21)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 144115188075855871) >> 13)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 144115188075855871) >> 5)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 144115188075855871) << 3)                | ((src[5] & 144115188075855871) >> 54)) & 255);
                            dest[36] = 
                (byte)((((src[5] & 144115188075855871) >> 46)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 144115188075855871) >> 38)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 144115188075855871) >> 30)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 144115188075855871) >> 22)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 144115188075855871) >> 14)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 144115188075855871) >> 6)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 144115188075855871) << 2)                | ((src[6] & 144115188075855871) >> 55)) & 255);
                            dest[43] = 
                (byte)((((src[6] & 144115188075855871) >> 47)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 144115188075855871) >> 39)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 144115188075855871) >> 31)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 144115188075855871) >> 23)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 144115188075855871) >> 15)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 144115188075855871) >> 7)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 144115188075855871) << 1)                | ((src[7] & 144115188075855871) >> 56)) & 255);
                            dest[50] = 
                (byte)((((src[7] & 144115188075855871) >> 48)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 144115188075855871) >> 40)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 144115188075855871) >> 32)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 144115188075855871) >> 24)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 144115188075855871) >> 16)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 144115188075855871) >> 8)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 144115188075855871))) & 255);
                        }
        private static void Unpack8LongValuesLE58(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935) | ((((long)src[7]) << 56) & 288230376151711743);            dest[1] = ((((long)src[7]) >> 2) & 63) | ((((long)src[8]) << 6) & 16383) | ((((long)src[9]) << 14) & 4194303) | ((((long)src[10]) << 22) & 1073741823) | ((((long)src[11]) << 30) & 274877906943) | ((((long)src[12]) << 38) & 70368744177663) | ((((long)src[13]) << 46) & 18014398509481983) | ((((long)src[14]) << 54) & 288230376151711743);            dest[2] = ((((long)src[14]) >> 4) & 15) | ((((long)src[15]) << 4) & 4095) | ((((long)src[16]) << 12) & 1048575) | ((((long)src[17]) << 20) & 268435455) | ((((long)src[18]) << 28) & 68719476735) | ((((long)src[19]) << 36) & 17592186044415) | ((((long)src[20]) << 44) & 4503599627370495) | ((((long)src[21]) << 52) & 288230376151711743);            dest[3] = ((((long)src[21]) >> 6) & 3) | ((((long)src[22]) << 2) & 1023) | ((((long)src[23]) << 10) & 262143) | ((((long)src[24]) << 18) & 67108863) | ((((long)src[25]) << 26) & 17179869183) | ((((long)src[26]) << 34) & 4398046511103) | ((((long)src[27]) << 42) & 1125899906842623) | ((((long)src[28]) << 50) & 288230376151711743);            dest[4] = ((((long)src[29])) & 255) | ((((long)src[30]) << 8) & 65535) | ((((long)src[31]) << 16) & 16777215) | ((((long)src[32]) << 24) & 4294967295) | ((((long)src[33]) << 32) & 1099511627775) | ((((long)src[34]) << 40) & 281474976710655) | ((((long)src[35]) << 48) & 72057594037927935) | ((((long)src[36]) << 56) & 288230376151711743);            dest[5] = ((((long)src[36]) >> 2) & 63) | ((((long)src[37]) << 6) & 16383) | ((((long)src[38]) << 14) & 4194303) | ((((long)src[39]) << 22) & 1073741823) | ((((long)src[40]) << 30) & 274877906943) | ((((long)src[41]) << 38) & 70368744177663) | ((((long)src[42]) << 46) & 18014398509481983) | ((((long)src[43]) << 54) & 288230376151711743);            dest[6] = ((((long)src[43]) >> 4) & 15) | ((((long)src[44]) << 4) & 4095) | ((((long)src[45]) << 12) & 1048575) | ((((long)src[46]) << 20) & 268435455) | ((((long)src[47]) << 28) & 68719476735) | ((((long)src[48]) << 36) & 17592186044415) | ((((long)src[49]) << 44) & 4503599627370495) | ((((long)src[50]) << 52) & 288230376151711743);            dest[7] = ((((long)src[50]) >> 6) & 3) | ((((long)src[51]) << 2) & 1023) | ((((long)src[52]) << 10) & 262143) | ((((long)src[53]) << 18) & 67108863) | ((((long)src[54]) << 26) & 17179869183) | ((((long)src[55]) << 34) & 4398046511103) | ((((long)src[56]) << 42) & 1125899906842623) | ((((long)src[57]) << 50) & 288230376151711743);        }

        private static void Pack8LongValuesLE58(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 288230376151711743))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 288230376151711743) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 288230376151711743) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 288230376151711743) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 288230376151711743) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 288230376151711743) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 288230376151711743) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 288230376151711743) >> 56)                | ((src[1] & 288230376151711743) << 2)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 288230376151711743) >> 6)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 288230376151711743) >> 14)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 288230376151711743) >> 22)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 288230376151711743) >> 30)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 288230376151711743) >> 38)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 288230376151711743) >> 46)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 288230376151711743) >> 54)                | ((src[2] & 288230376151711743) << 4)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 288230376151711743) >> 4)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 288230376151711743) >> 12)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 288230376151711743) >> 20)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 288230376151711743) >> 28)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 288230376151711743) >> 36)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 288230376151711743) >> 44)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 288230376151711743) >> 52)                | ((src[3] & 288230376151711743) << 6)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 288230376151711743) >> 2)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 288230376151711743) >> 10)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 288230376151711743) >> 18)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 288230376151711743) >> 26)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 288230376151711743) >> 34)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 288230376151711743) >> 42)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 288230376151711743) >> 50)) & 255);
                            dest[29] = 
                (byte)((((src[4] & 288230376151711743))) & 255);
                            dest[30] = 
                (byte)((((src[4] & 288230376151711743) >> 8)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 288230376151711743) >> 16)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 288230376151711743) >> 24)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 288230376151711743) >> 32)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 288230376151711743) >> 40)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 288230376151711743) >> 48)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 288230376151711743) >> 56)                | ((src[5] & 288230376151711743) << 2)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 288230376151711743) >> 6)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 288230376151711743) >> 14)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 288230376151711743) >> 22)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 288230376151711743) >> 30)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 288230376151711743) >> 38)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 288230376151711743) >> 46)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 288230376151711743) >> 54)                | ((src[6] & 288230376151711743) << 4)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 288230376151711743) >> 4)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 288230376151711743) >> 12)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 288230376151711743) >> 20)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 288230376151711743) >> 28)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 288230376151711743) >> 36)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 288230376151711743) >> 44)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 288230376151711743) >> 52)                | ((src[7] & 288230376151711743) << 6)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 288230376151711743) >> 2)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 288230376151711743) >> 10)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 288230376151711743) >> 18)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 288230376151711743) >> 26)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 288230376151711743) >> 34)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 288230376151711743) >> 42)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 288230376151711743) >> 50)) & 255);
                        }
        private static void Unpack8LongValuesBE58(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 50) & 288230376151711743) | ((((long)src[1]) << 42) & 1125899906842623) | ((((long)src[2]) << 34) & 4398046511103) | ((((long)src[3]) << 26) & 17179869183) | ((((long)src[4]) << 18) & 67108863) | ((((long)src[5]) << 10) & 262143) | ((((long)src[6]) << 2) & 1023) | ((((long)src[7]) >> 6) & 3);            dest[1] = ((((long)src[7]) << 52) & 288230376151711743) | ((((long)src[8]) << 44) & 4503599627370495) | ((((long)src[9]) << 36) & 17592186044415) | ((((long)src[10]) << 28) & 68719476735) | ((((long)src[11]) << 20) & 268435455) | ((((long)src[12]) << 12) & 1048575) | ((((long)src[13]) << 4) & 4095) | ((((long)src[14]) >> 4) & 15);            dest[2] = ((((long)src[14]) << 54) & 288230376151711743) | ((((long)src[15]) << 46) & 18014398509481983) | ((((long)src[16]) << 38) & 70368744177663) | ((((long)src[17]) << 30) & 274877906943) | ((((long)src[18]) << 22) & 1073741823) | ((((long)src[19]) << 14) & 4194303) | ((((long)src[20]) << 6) & 16383) | ((((long)src[21]) >> 2) & 63);            dest[3] = ((((long)src[21]) << 56) & 288230376151711743) | ((((long)src[22]) << 48) & 72057594037927935) | ((((long)src[23]) << 40) & 281474976710655) | ((((long)src[24]) << 32) & 1099511627775) | ((((long)src[25]) << 24) & 4294967295) | ((((long)src[26]) << 16) & 16777215) | ((((long)src[27]) << 8) & 65535) | ((((long)src[28])) & 255);            dest[4] = ((((long)src[29]) << 50) & 288230376151711743) | ((((long)src[30]) << 42) & 1125899906842623) | ((((long)src[31]) << 34) & 4398046511103) | ((((long)src[32]) << 26) & 17179869183) | ((((long)src[33]) << 18) & 67108863) | ((((long)src[34]) << 10) & 262143) | ((((long)src[35]) << 2) & 1023) | ((((long)src[36]) >> 6) & 3);            dest[5] = ((((long)src[36]) << 52) & 288230376151711743) | ((((long)src[37]) << 44) & 4503599627370495) | ((((long)src[38]) << 36) & 17592186044415) | ((((long)src[39]) << 28) & 68719476735) | ((((long)src[40]) << 20) & 268435455) | ((((long)src[41]) << 12) & 1048575) | ((((long)src[42]) << 4) & 4095) | ((((long)src[43]) >> 4) & 15);            dest[6] = ((((long)src[43]) << 54) & 288230376151711743) | ((((long)src[44]) << 46) & 18014398509481983) | ((((long)src[45]) << 38) & 70368744177663) | ((((long)src[46]) << 30) & 274877906943) | ((((long)src[47]) << 22) & 1073741823) | ((((long)src[48]) << 14) & 4194303) | ((((long)src[49]) << 6) & 16383) | ((((long)src[50]) >> 2) & 63);            dest[7] = ((((long)src[50]) << 56) & 288230376151711743) | ((((long)src[51]) << 48) & 72057594037927935) | ((((long)src[52]) << 40) & 281474976710655) | ((((long)src[53]) << 32) & 1099511627775) | ((((long)src[54]) << 24) & 4294967295) | ((((long)src[55]) << 16) & 16777215) | ((((long)src[56]) << 8) & 65535) | ((((long)src[57])) & 255);        }

        private static void Pack8LongValuesBE58(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 288230376151711743) >> 50)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 288230376151711743) >> 42)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 288230376151711743) >> 34)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 288230376151711743) >> 26)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 288230376151711743) >> 18)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 288230376151711743) >> 10)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 288230376151711743) >> 2)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 288230376151711743) << 6)                | ((src[1] & 288230376151711743) >> 52)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 288230376151711743) >> 44)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 288230376151711743) >> 36)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 288230376151711743) >> 28)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 288230376151711743) >> 20)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 288230376151711743) >> 12)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 288230376151711743) >> 4)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 288230376151711743) << 4)                | ((src[2] & 288230376151711743) >> 54)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 288230376151711743) >> 46)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 288230376151711743) >> 38)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 288230376151711743) >> 30)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 288230376151711743) >> 22)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 288230376151711743) >> 14)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 288230376151711743) >> 6)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 288230376151711743) << 2)                | ((src[3] & 288230376151711743) >> 56)) & 255);
                            dest[22] = 
                (byte)((((src[3] & 288230376151711743) >> 48)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 288230376151711743) >> 40)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 288230376151711743) >> 32)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 288230376151711743) >> 24)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 288230376151711743) >> 16)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 288230376151711743) >> 8)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 288230376151711743))) & 255);
                            dest[29] = 
                (byte)((((src[4] & 288230376151711743) >> 50)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 288230376151711743) >> 42)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 288230376151711743) >> 34)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 288230376151711743) >> 26)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 288230376151711743) >> 18)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 288230376151711743) >> 10)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 288230376151711743) >> 2)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 288230376151711743) << 6)                | ((src[5] & 288230376151711743) >> 52)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 288230376151711743) >> 44)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 288230376151711743) >> 36)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 288230376151711743) >> 28)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 288230376151711743) >> 20)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 288230376151711743) >> 12)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 288230376151711743) >> 4)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 288230376151711743) << 4)                | ((src[6] & 288230376151711743) >> 54)) & 255);
                            dest[44] = 
                (byte)((((src[6] & 288230376151711743) >> 46)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 288230376151711743) >> 38)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 288230376151711743) >> 30)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 288230376151711743) >> 22)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 288230376151711743) >> 14)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 288230376151711743) >> 6)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 288230376151711743) << 2)                | ((src[7] & 288230376151711743) >> 56)) & 255);
                            dest[51] = 
                (byte)((((src[7] & 288230376151711743) >> 48)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 288230376151711743) >> 40)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 288230376151711743) >> 32)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 288230376151711743) >> 24)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 288230376151711743) >> 16)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 288230376151711743) >> 8)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 288230376151711743))) & 255);
                        }
        private static void Unpack8LongValuesLE59(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935) | ((((long)src[7]) << 56) & 576460752303423487);            dest[1] = ((((long)src[7]) >> 3) & 31) | ((((long)src[8]) << 5) & 8191) | ((((long)src[9]) << 13) & 2097151) | ((((long)src[10]) << 21) & 536870911) | ((((long)src[11]) << 29) & 137438953471) | ((((long)src[12]) << 37) & 35184372088831) | ((((long)src[13]) << 45) & 9007199254740991) | ((((long)src[14]) << 53) & 576460752303423487);            dest[2] = ((((long)src[14]) >> 6) & 3) | ((((long)src[15]) << 2) & 1023) | ((((long)src[16]) << 10) & 262143) | ((((long)src[17]) << 18) & 67108863) | ((((long)src[18]) << 26) & 17179869183) | ((((long)src[19]) << 34) & 4398046511103) | ((((long)src[20]) << 42) & 1125899906842623) | ((((long)src[21]) << 50) & 288230376151711743) | ((((long)src[22]) << 58) & 576460752303423487);            dest[3] = ((((long)src[22]) >> 1) & 127) | ((((long)src[23]) << 7) & 32767) | ((((long)src[24]) << 15) & 8388607) | ((((long)src[25]) << 23) & 2147483647) | ((((long)src[26]) << 31) & 549755813887) | ((((long)src[27]) << 39) & 140737488355327) | ((((long)src[28]) << 47) & 36028797018963967) | ((((long)src[29]) << 55) & 576460752303423487);            dest[4] = ((((long)src[29]) >> 4) & 15) | ((((long)src[30]) << 4) & 4095) | ((((long)src[31]) << 12) & 1048575) | ((((long)src[32]) << 20) & 268435455) | ((((long)src[33]) << 28) & 68719476735) | ((((long)src[34]) << 36) & 17592186044415) | ((((long)src[35]) << 44) & 4503599627370495) | ((((long)src[36]) << 52) & 576460752303423487);            dest[5] = ((((long)src[36]) >> 7) & 1) | ((((long)src[37]) << 1) & 511) | ((((long)src[38]) << 9) & 131071) | ((((long)src[39]) << 17) & 33554431) | ((((long)src[40]) << 25) & 8589934591) | ((((long)src[41]) << 33) & 2199023255551) | ((((long)src[42]) << 41) & 562949953421311) | ((((long)src[43]) << 49) & 144115188075855871) | ((((long)src[44]) << 57) & 576460752303423487);            dest[6] = ((((long)src[44]) >> 2) & 63) | ((((long)src[45]) << 6) & 16383) | ((((long)src[46]) << 14) & 4194303) | ((((long)src[47]) << 22) & 1073741823) | ((((long)src[48]) << 30) & 274877906943) | ((((long)src[49]) << 38) & 70368744177663) | ((((long)src[50]) << 46) & 18014398509481983) | ((((long)src[51]) << 54) & 576460752303423487);            dest[7] = ((((long)src[51]) >> 5) & 7) | ((((long)src[52]) << 3) & 2047) | ((((long)src[53]) << 11) & 524287) | ((((long)src[54]) << 19) & 134217727) | ((((long)src[55]) << 27) & 34359738367) | ((((long)src[56]) << 35) & 8796093022207) | ((((long)src[57]) << 43) & 2251799813685247) | ((((long)src[58]) << 51) & 576460752303423487);        }

        private static void Pack8LongValuesLE59(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 576460752303423487))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 576460752303423487) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 576460752303423487) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 576460752303423487) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 576460752303423487) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 576460752303423487) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 576460752303423487) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 576460752303423487) >> 56)                | ((src[1] & 576460752303423487) << 3)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 576460752303423487) >> 5)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 576460752303423487) >> 13)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 576460752303423487) >> 21)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 576460752303423487) >> 29)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 576460752303423487) >> 37)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 576460752303423487) >> 45)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 576460752303423487) >> 53)                | ((src[2] & 576460752303423487) << 6)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 576460752303423487) >> 2)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 576460752303423487) >> 10)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 576460752303423487) >> 18)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 576460752303423487) >> 26)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 576460752303423487) >> 34)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 576460752303423487) >> 42)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 576460752303423487) >> 50)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 576460752303423487) >> 58)                | ((src[3] & 576460752303423487) << 1)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 576460752303423487) >> 7)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 576460752303423487) >> 15)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 576460752303423487) >> 23)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 576460752303423487) >> 31)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 576460752303423487) >> 39)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 576460752303423487) >> 47)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 576460752303423487) >> 55)                | ((src[4] & 576460752303423487) << 4)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 576460752303423487) >> 4)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 576460752303423487) >> 12)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 576460752303423487) >> 20)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 576460752303423487) >> 28)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 576460752303423487) >> 36)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 576460752303423487) >> 44)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 576460752303423487) >> 52)                | ((src[5] & 576460752303423487) << 7)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 576460752303423487) >> 1)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 576460752303423487) >> 9)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 576460752303423487) >> 17)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 576460752303423487) >> 25)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 576460752303423487) >> 33)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 576460752303423487) >> 41)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 576460752303423487) >> 49)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 576460752303423487) >> 57)                | ((src[6] & 576460752303423487) << 2)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 576460752303423487) >> 6)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 576460752303423487) >> 14)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 576460752303423487) >> 22)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 576460752303423487) >> 30)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 576460752303423487) >> 38)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 576460752303423487) >> 46)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 576460752303423487) >> 54)                | ((src[7] & 576460752303423487) << 5)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 576460752303423487) >> 3)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 576460752303423487) >> 11)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 576460752303423487) >> 19)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 576460752303423487) >> 27)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 576460752303423487) >> 35)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 576460752303423487) >> 43)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 576460752303423487) >> 51)) & 255);
                        }
        private static void Unpack8LongValuesBE59(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 51) & 576460752303423487) | ((((long)src[1]) << 43) & 2251799813685247) | ((((long)src[2]) << 35) & 8796093022207) | ((((long)src[3]) << 27) & 34359738367) | ((((long)src[4]) << 19) & 134217727) | ((((long)src[5]) << 11) & 524287) | ((((long)src[6]) << 3) & 2047) | ((((long)src[7]) >> 5) & 7);            dest[1] = ((((long)src[7]) << 54) & 576460752303423487) | ((((long)src[8]) << 46) & 18014398509481983) | ((((long)src[9]) << 38) & 70368744177663) | ((((long)src[10]) << 30) & 274877906943) | ((((long)src[11]) << 22) & 1073741823) | ((((long)src[12]) << 14) & 4194303) | ((((long)src[13]) << 6) & 16383) | ((((long)src[14]) >> 2) & 63);            dest[2] = ((((long)src[14]) << 57) & 576460752303423487) | ((((long)src[15]) << 49) & 144115188075855871) | ((((long)src[16]) << 41) & 562949953421311) | ((((long)src[17]) << 33) & 2199023255551) | ((((long)src[18]) << 25) & 8589934591) | ((((long)src[19]) << 17) & 33554431) | ((((long)src[20]) << 9) & 131071) | ((((long)src[21]) << 1) & 511) | ((((long)src[22]) >> 7) & 1);            dest[3] = ((((long)src[22]) << 52) & 576460752303423487) | ((((long)src[23]) << 44) & 4503599627370495) | ((((long)src[24]) << 36) & 17592186044415) | ((((long)src[25]) << 28) & 68719476735) | ((((long)src[26]) << 20) & 268435455) | ((((long)src[27]) << 12) & 1048575) | ((((long)src[28]) << 4) & 4095) | ((((long)src[29]) >> 4) & 15);            dest[4] = ((((long)src[29]) << 55) & 576460752303423487) | ((((long)src[30]) << 47) & 36028797018963967) | ((((long)src[31]) << 39) & 140737488355327) | ((((long)src[32]) << 31) & 549755813887) | ((((long)src[33]) << 23) & 2147483647) | ((((long)src[34]) << 15) & 8388607) | ((((long)src[35]) << 7) & 32767) | ((((long)src[36]) >> 1) & 127);            dest[5] = ((((long)src[36]) << 58) & 576460752303423487) | ((((long)src[37]) << 50) & 288230376151711743) | ((((long)src[38]) << 42) & 1125899906842623) | ((((long)src[39]) << 34) & 4398046511103) | ((((long)src[40]) << 26) & 17179869183) | ((((long)src[41]) << 18) & 67108863) | ((((long)src[42]) << 10) & 262143) | ((((long)src[43]) << 2) & 1023) | ((((long)src[44]) >> 6) & 3);            dest[6] = ((((long)src[44]) << 53) & 576460752303423487) | ((((long)src[45]) << 45) & 9007199254740991) | ((((long)src[46]) << 37) & 35184372088831) | ((((long)src[47]) << 29) & 137438953471) | ((((long)src[48]) << 21) & 536870911) | ((((long)src[49]) << 13) & 2097151) | ((((long)src[50]) << 5) & 8191) | ((((long)src[51]) >> 3) & 31);            dest[7] = ((((long)src[51]) << 56) & 576460752303423487) | ((((long)src[52]) << 48) & 72057594037927935) | ((((long)src[53]) << 40) & 281474976710655) | ((((long)src[54]) << 32) & 1099511627775) | ((((long)src[55]) << 24) & 4294967295) | ((((long)src[56]) << 16) & 16777215) | ((((long)src[57]) << 8) & 65535) | ((((long)src[58])) & 255);        }

        private static void Pack8LongValuesBE59(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 576460752303423487) >> 51)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 576460752303423487) >> 43)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 576460752303423487) >> 35)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 576460752303423487) >> 27)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 576460752303423487) >> 19)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 576460752303423487) >> 11)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 576460752303423487) >> 3)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 576460752303423487) << 5)                | ((src[1] & 576460752303423487) >> 54)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 576460752303423487) >> 46)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 576460752303423487) >> 38)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 576460752303423487) >> 30)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 576460752303423487) >> 22)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 576460752303423487) >> 14)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 576460752303423487) >> 6)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 576460752303423487) << 2)                | ((src[2] & 576460752303423487) >> 57)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 576460752303423487) >> 49)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 576460752303423487) >> 41)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 576460752303423487) >> 33)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 576460752303423487) >> 25)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 576460752303423487) >> 17)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 576460752303423487) >> 9)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 576460752303423487) >> 1)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 576460752303423487) << 7)                | ((src[3] & 576460752303423487) >> 52)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 576460752303423487) >> 44)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 576460752303423487) >> 36)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 576460752303423487) >> 28)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 576460752303423487) >> 20)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 576460752303423487) >> 12)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 576460752303423487) >> 4)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 576460752303423487) << 4)                | ((src[4] & 576460752303423487) >> 55)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 576460752303423487) >> 47)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 576460752303423487) >> 39)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 576460752303423487) >> 31)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 576460752303423487) >> 23)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 576460752303423487) >> 15)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 576460752303423487) >> 7)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 576460752303423487) << 1)                | ((src[5] & 576460752303423487) >> 58)) & 255);
                            dest[37] = 
                (byte)((((src[5] & 576460752303423487) >> 50)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 576460752303423487) >> 42)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 576460752303423487) >> 34)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 576460752303423487) >> 26)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 576460752303423487) >> 18)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 576460752303423487) >> 10)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 576460752303423487) >> 2)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 576460752303423487) << 6)                | ((src[6] & 576460752303423487) >> 53)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 576460752303423487) >> 45)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 576460752303423487) >> 37)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 576460752303423487) >> 29)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 576460752303423487) >> 21)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 576460752303423487) >> 13)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 576460752303423487) >> 5)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 576460752303423487) << 3)                | ((src[7] & 576460752303423487) >> 56)) & 255);
                            dest[52] = 
                (byte)((((src[7] & 576460752303423487) >> 48)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 576460752303423487) >> 40)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 576460752303423487) >> 32)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 576460752303423487) >> 24)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 576460752303423487) >> 16)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 576460752303423487) >> 8)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 576460752303423487))) & 255);
                        }
        private static void Unpack8LongValuesLE60(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935) | ((((long)src[7]) << 56) & 1152921504606846975);            dest[1] = ((((long)src[7]) >> 4) & 15) | ((((long)src[8]) << 4) & 4095) | ((((long)src[9]) << 12) & 1048575) | ((((long)src[10]) << 20) & 268435455) | ((((long)src[11]) << 28) & 68719476735) | ((((long)src[12]) << 36) & 17592186044415) | ((((long)src[13]) << 44) & 4503599627370495) | ((((long)src[14]) << 52) & 1152921504606846975);            dest[2] = ((((long)src[15])) & 255) | ((((long)src[16]) << 8) & 65535) | ((((long)src[17]) << 16) & 16777215) | ((((long)src[18]) << 24) & 4294967295) | ((((long)src[19]) << 32) & 1099511627775) | ((((long)src[20]) << 40) & 281474976710655) | ((((long)src[21]) << 48) & 72057594037927935) | ((((long)src[22]) << 56) & 1152921504606846975);            dest[3] = ((((long)src[22]) >> 4) & 15) | ((((long)src[23]) << 4) & 4095) | ((((long)src[24]) << 12) & 1048575) | ((((long)src[25]) << 20) & 268435455) | ((((long)src[26]) << 28) & 68719476735) | ((((long)src[27]) << 36) & 17592186044415) | ((((long)src[28]) << 44) & 4503599627370495) | ((((long)src[29]) << 52) & 1152921504606846975);            dest[4] = ((((long)src[30])) & 255) | ((((long)src[31]) << 8) & 65535) | ((((long)src[32]) << 16) & 16777215) | ((((long)src[33]) << 24) & 4294967295) | ((((long)src[34]) << 32) & 1099511627775) | ((((long)src[35]) << 40) & 281474976710655) | ((((long)src[36]) << 48) & 72057594037927935) | ((((long)src[37]) << 56) & 1152921504606846975);            dest[5] = ((((long)src[37]) >> 4) & 15) | ((((long)src[38]) << 4) & 4095) | ((((long)src[39]) << 12) & 1048575) | ((((long)src[40]) << 20) & 268435455) | ((((long)src[41]) << 28) & 68719476735) | ((((long)src[42]) << 36) & 17592186044415) | ((((long)src[43]) << 44) & 4503599627370495) | ((((long)src[44]) << 52) & 1152921504606846975);            dest[6] = ((((long)src[45])) & 255) | ((((long)src[46]) << 8) & 65535) | ((((long)src[47]) << 16) & 16777215) | ((((long)src[48]) << 24) & 4294967295) | ((((long)src[49]) << 32) & 1099511627775) | ((((long)src[50]) << 40) & 281474976710655) | ((((long)src[51]) << 48) & 72057594037927935) | ((((long)src[52]) << 56) & 1152921504606846975);            dest[7] = ((((long)src[52]) >> 4) & 15) | ((((long)src[53]) << 4) & 4095) | ((((long)src[54]) << 12) & 1048575) | ((((long)src[55]) << 20) & 268435455) | ((((long)src[56]) << 28) & 68719476735) | ((((long)src[57]) << 36) & 17592186044415) | ((((long)src[58]) << 44) & 4503599627370495) | ((((long)src[59]) << 52) & 1152921504606846975);        }

        private static void Pack8LongValuesLE60(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1152921504606846975))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1152921504606846975) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1152921504606846975) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1152921504606846975) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 1152921504606846975) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 1152921504606846975) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 1152921504606846975) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 1152921504606846975) >> 56)                | ((src[1] & 1152921504606846975) << 4)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 1152921504606846975) >> 4)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 1152921504606846975) >> 12)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 1152921504606846975) >> 20)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 1152921504606846975) >> 28)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 1152921504606846975) >> 36)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 1152921504606846975) >> 44)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 1152921504606846975) >> 52)) & 255);
                            dest[15] = 
                (byte)((((src[2] & 1152921504606846975))) & 255);
                            dest[16] = 
                (byte)((((src[2] & 1152921504606846975) >> 8)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 1152921504606846975) >> 16)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 1152921504606846975) >> 24)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 1152921504606846975) >> 32)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 1152921504606846975) >> 40)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 1152921504606846975) >> 48)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 1152921504606846975) >> 56)                | ((src[3] & 1152921504606846975) << 4)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 1152921504606846975) >> 4)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 1152921504606846975) >> 12)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 1152921504606846975) >> 20)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 1152921504606846975) >> 28)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 1152921504606846975) >> 36)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 1152921504606846975) >> 44)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 1152921504606846975) >> 52)) & 255);
                            dest[30] = 
                (byte)((((src[4] & 1152921504606846975))) & 255);
                            dest[31] = 
                (byte)((((src[4] & 1152921504606846975) >> 8)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 1152921504606846975) >> 16)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 1152921504606846975) >> 24)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 1152921504606846975) >> 32)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 1152921504606846975) >> 40)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 1152921504606846975) >> 48)) & 255);
                            dest[37] = 
                (byte)((((src[4] & 1152921504606846975) >> 56)                | ((src[5] & 1152921504606846975) << 4)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 1152921504606846975) >> 4)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 1152921504606846975) >> 12)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 1152921504606846975) >> 20)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 1152921504606846975) >> 28)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 1152921504606846975) >> 36)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 1152921504606846975) >> 44)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 1152921504606846975) >> 52)) & 255);
                            dest[45] = 
                (byte)((((src[6] & 1152921504606846975))) & 255);
                            dest[46] = 
                (byte)((((src[6] & 1152921504606846975) >> 8)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 1152921504606846975) >> 16)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 1152921504606846975) >> 24)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 1152921504606846975) >> 32)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 1152921504606846975) >> 40)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 1152921504606846975) >> 48)) & 255);
                            dest[52] = 
                (byte)((((src[6] & 1152921504606846975) >> 56)                | ((src[7] & 1152921504606846975) << 4)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 1152921504606846975) >> 4)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 1152921504606846975) >> 12)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 1152921504606846975) >> 20)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 1152921504606846975) >> 28)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 1152921504606846975) >> 36)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 1152921504606846975) >> 44)) & 255);
                            dest[59] = 
                (byte)((((src[7] & 1152921504606846975) >> 52)) & 255);
                        }
        private static void Unpack8LongValuesBE60(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 52) & 1152921504606846975) | ((((long)src[1]) << 44) & 4503599627370495) | ((((long)src[2]) << 36) & 17592186044415) | ((((long)src[3]) << 28) & 68719476735) | ((((long)src[4]) << 20) & 268435455) | ((((long)src[5]) << 12) & 1048575) | ((((long)src[6]) << 4) & 4095) | ((((long)src[7]) >> 4) & 15);            dest[1] = ((((long)src[7]) << 56) & 1152921504606846975) | ((((long)src[8]) << 48) & 72057594037927935) | ((((long)src[9]) << 40) & 281474976710655) | ((((long)src[10]) << 32) & 1099511627775) | ((((long)src[11]) << 24) & 4294967295) | ((((long)src[12]) << 16) & 16777215) | ((((long)src[13]) << 8) & 65535) | ((((long)src[14])) & 255);            dest[2] = ((((long)src[15]) << 52) & 1152921504606846975) | ((((long)src[16]) << 44) & 4503599627370495) | ((((long)src[17]) << 36) & 17592186044415) | ((((long)src[18]) << 28) & 68719476735) | ((((long)src[19]) << 20) & 268435455) | ((((long)src[20]) << 12) & 1048575) | ((((long)src[21]) << 4) & 4095) | ((((long)src[22]) >> 4) & 15);            dest[3] = ((((long)src[22]) << 56) & 1152921504606846975) | ((((long)src[23]) << 48) & 72057594037927935) | ((((long)src[24]) << 40) & 281474976710655) | ((((long)src[25]) << 32) & 1099511627775) | ((((long)src[26]) << 24) & 4294967295) | ((((long)src[27]) << 16) & 16777215) | ((((long)src[28]) << 8) & 65535) | ((((long)src[29])) & 255);            dest[4] = ((((long)src[30]) << 52) & 1152921504606846975) | ((((long)src[31]) << 44) & 4503599627370495) | ((((long)src[32]) << 36) & 17592186044415) | ((((long)src[33]) << 28) & 68719476735) | ((((long)src[34]) << 20) & 268435455) | ((((long)src[35]) << 12) & 1048575) | ((((long)src[36]) << 4) & 4095) | ((((long)src[37]) >> 4) & 15);            dest[5] = ((((long)src[37]) << 56) & 1152921504606846975) | ((((long)src[38]) << 48) & 72057594037927935) | ((((long)src[39]) << 40) & 281474976710655) | ((((long)src[40]) << 32) & 1099511627775) | ((((long)src[41]) << 24) & 4294967295) | ((((long)src[42]) << 16) & 16777215) | ((((long)src[43]) << 8) & 65535) | ((((long)src[44])) & 255);            dest[6] = ((((long)src[45]) << 52) & 1152921504606846975) | ((((long)src[46]) << 44) & 4503599627370495) | ((((long)src[47]) << 36) & 17592186044415) | ((((long)src[48]) << 28) & 68719476735) | ((((long)src[49]) << 20) & 268435455) | ((((long)src[50]) << 12) & 1048575) | ((((long)src[51]) << 4) & 4095) | ((((long)src[52]) >> 4) & 15);            dest[7] = ((((long)src[52]) << 56) & 1152921504606846975) | ((((long)src[53]) << 48) & 72057594037927935) | ((((long)src[54]) << 40) & 281474976710655) | ((((long)src[55]) << 32) & 1099511627775) | ((((long)src[56]) << 24) & 4294967295) | ((((long)src[57]) << 16) & 16777215) | ((((long)src[58]) << 8) & 65535) | ((((long)src[59])) & 255);        }

        private static void Pack8LongValuesBE60(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 1152921504606846975) >> 52)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 1152921504606846975) >> 44)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 1152921504606846975) >> 36)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 1152921504606846975) >> 28)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 1152921504606846975) >> 20)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 1152921504606846975) >> 12)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 1152921504606846975) >> 4)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 1152921504606846975) << 4)                | ((src[1] & 1152921504606846975) >> 56)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 1152921504606846975) >> 48)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 1152921504606846975) >> 40)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 1152921504606846975) >> 32)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 1152921504606846975) >> 24)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 1152921504606846975) >> 16)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 1152921504606846975) >> 8)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 1152921504606846975))) & 255);
                            dest[15] = 
                (byte)((((src[2] & 1152921504606846975) >> 52)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 1152921504606846975) >> 44)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 1152921504606846975) >> 36)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 1152921504606846975) >> 28)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 1152921504606846975) >> 20)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 1152921504606846975) >> 12)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 1152921504606846975) >> 4)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 1152921504606846975) << 4)                | ((src[3] & 1152921504606846975) >> 56)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 1152921504606846975) >> 48)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 1152921504606846975) >> 40)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 1152921504606846975) >> 32)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 1152921504606846975) >> 24)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 1152921504606846975) >> 16)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 1152921504606846975) >> 8)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 1152921504606846975))) & 255);
                            dest[30] = 
                (byte)((((src[4] & 1152921504606846975) >> 52)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 1152921504606846975) >> 44)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 1152921504606846975) >> 36)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 1152921504606846975) >> 28)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 1152921504606846975) >> 20)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 1152921504606846975) >> 12)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 1152921504606846975) >> 4)) & 255);
                            dest[37] = 
                (byte)((((src[4] & 1152921504606846975) << 4)                | ((src[5] & 1152921504606846975) >> 56)) & 255);
                            dest[38] = 
                (byte)((((src[5] & 1152921504606846975) >> 48)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 1152921504606846975) >> 40)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 1152921504606846975) >> 32)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 1152921504606846975) >> 24)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 1152921504606846975) >> 16)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 1152921504606846975) >> 8)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 1152921504606846975))) & 255);
                            dest[45] = 
                (byte)((((src[6] & 1152921504606846975) >> 52)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 1152921504606846975) >> 44)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 1152921504606846975) >> 36)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 1152921504606846975) >> 28)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 1152921504606846975) >> 20)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 1152921504606846975) >> 12)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 1152921504606846975) >> 4)) & 255);
                            dest[52] = 
                (byte)((((src[6] & 1152921504606846975) << 4)                | ((src[7] & 1152921504606846975) >> 56)) & 255);
                            dest[53] = 
                (byte)((((src[7] & 1152921504606846975) >> 48)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 1152921504606846975) >> 40)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 1152921504606846975) >> 32)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 1152921504606846975) >> 24)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 1152921504606846975) >> 16)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 1152921504606846975) >> 8)) & 255);
                            dest[59] = 
                (byte)((((src[7] & 1152921504606846975))) & 255);
                        }
        private static void Unpack8LongValuesLE61(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935) | ((((long)src[7]) << 56) & 2305843009213693951);            dest[1] = ((((long)src[7]) >> 5) & 7) | ((((long)src[8]) << 3) & 2047) | ((((long)src[9]) << 11) & 524287) | ((((long)src[10]) << 19) & 134217727) | ((((long)src[11]) << 27) & 34359738367) | ((((long)src[12]) << 35) & 8796093022207) | ((((long)src[13]) << 43) & 2251799813685247) | ((((long)src[14]) << 51) & 576460752303423487) | ((((long)src[15]) << 59) & 2305843009213693951);            dest[2] = ((((long)src[15]) >> 2) & 63) | ((((long)src[16]) << 6) & 16383) | ((((long)src[17]) << 14) & 4194303) | ((((long)src[18]) << 22) & 1073741823) | ((((long)src[19]) << 30) & 274877906943) | ((((long)src[20]) << 38) & 70368744177663) | ((((long)src[21]) << 46) & 18014398509481983) | ((((long)src[22]) << 54) & 2305843009213693951);            dest[3] = ((((long)src[22]) >> 7) & 1) | ((((long)src[23]) << 1) & 511) | ((((long)src[24]) << 9) & 131071) | ((((long)src[25]) << 17) & 33554431) | ((((long)src[26]) << 25) & 8589934591) | ((((long)src[27]) << 33) & 2199023255551) | ((((long)src[28]) << 41) & 562949953421311) | ((((long)src[29]) << 49) & 144115188075855871) | ((((long)src[30]) << 57) & 2305843009213693951);            dest[4] = ((((long)src[30]) >> 4) & 15) | ((((long)src[31]) << 4) & 4095) | ((((long)src[32]) << 12) & 1048575) | ((((long)src[33]) << 20) & 268435455) | ((((long)src[34]) << 28) & 68719476735) | ((((long)src[35]) << 36) & 17592186044415) | ((((long)src[36]) << 44) & 4503599627370495) | ((((long)src[37]) << 52) & 1152921504606846975) | ((((long)src[38]) << 60) & 2305843009213693951);            dest[5] = ((((long)src[38]) >> 1) & 127) | ((((long)src[39]) << 7) & 32767) | ((((long)src[40]) << 15) & 8388607) | ((((long)src[41]) << 23) & 2147483647) | ((((long)src[42]) << 31) & 549755813887) | ((((long)src[43]) << 39) & 140737488355327) | ((((long)src[44]) << 47) & 36028797018963967) | ((((long)src[45]) << 55) & 2305843009213693951);            dest[6] = ((((long)src[45]) >> 6) & 3) | ((((long)src[46]) << 2) & 1023) | ((((long)src[47]) << 10) & 262143) | ((((long)src[48]) << 18) & 67108863) | ((((long)src[49]) << 26) & 17179869183) | ((((long)src[50]) << 34) & 4398046511103) | ((((long)src[51]) << 42) & 1125899906842623) | ((((long)src[52]) << 50) & 288230376151711743) | ((((long)src[53]) << 58) & 2305843009213693951);            dest[7] = ((((long)src[53]) >> 3) & 31) | ((((long)src[54]) << 5) & 8191) | ((((long)src[55]) << 13) & 2097151) | ((((long)src[56]) << 21) & 536870911) | ((((long)src[57]) << 29) & 137438953471) | ((((long)src[58]) << 37) & 35184372088831) | ((((long)src[59]) << 45) & 9007199254740991) | ((((long)src[60]) << 53) & 2305843009213693951);        }

        private static void Pack8LongValuesLE61(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2305843009213693951))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2305843009213693951) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2305843009213693951) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2305843009213693951) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 2305843009213693951) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 2305843009213693951) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 2305843009213693951) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 2305843009213693951) >> 56)                | ((src[1] & 2305843009213693951) << 5)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 2305843009213693951) >> 3)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 2305843009213693951) >> 11)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 2305843009213693951) >> 19)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 2305843009213693951) >> 27)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 2305843009213693951) >> 35)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 2305843009213693951) >> 43)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 2305843009213693951) >> 51)) & 255);
                            dest[15] = 
                (byte)((((src[1] & 2305843009213693951) >> 59)                | ((src[2] & 2305843009213693951) << 2)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 2305843009213693951) >> 6)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 2305843009213693951) >> 14)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 2305843009213693951) >> 22)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 2305843009213693951) >> 30)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 2305843009213693951) >> 38)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 2305843009213693951) >> 46)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 2305843009213693951) >> 54)                | ((src[3] & 2305843009213693951) << 7)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 2305843009213693951) >> 1)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 2305843009213693951) >> 9)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 2305843009213693951) >> 17)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 2305843009213693951) >> 25)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 2305843009213693951) >> 33)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 2305843009213693951) >> 41)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 2305843009213693951) >> 49)) & 255);
                            dest[30] = 
                (byte)((((src[3] & 2305843009213693951) >> 57)                | ((src[4] & 2305843009213693951) << 4)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 2305843009213693951) >> 4)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 2305843009213693951) >> 12)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 2305843009213693951) >> 20)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 2305843009213693951) >> 28)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 2305843009213693951) >> 36)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 2305843009213693951) >> 44)) & 255);
                            dest[37] = 
                (byte)((((src[4] & 2305843009213693951) >> 52)) & 255);
                            dest[38] = 
                (byte)((((src[4] & 2305843009213693951) >> 60)                | ((src[5] & 2305843009213693951) << 1)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 2305843009213693951) >> 7)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 2305843009213693951) >> 15)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 2305843009213693951) >> 23)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 2305843009213693951) >> 31)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 2305843009213693951) >> 39)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 2305843009213693951) >> 47)) & 255);
                            dest[45] = 
                (byte)((((src[5] & 2305843009213693951) >> 55)                | ((src[6] & 2305843009213693951) << 6)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 2305843009213693951) >> 2)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 2305843009213693951) >> 10)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 2305843009213693951) >> 18)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 2305843009213693951) >> 26)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 2305843009213693951) >> 34)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 2305843009213693951) >> 42)) & 255);
                            dest[52] = 
                (byte)((((src[6] & 2305843009213693951) >> 50)) & 255);
                            dest[53] = 
                (byte)((((src[6] & 2305843009213693951) >> 58)                | ((src[7] & 2305843009213693951) << 3)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 2305843009213693951) >> 5)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 2305843009213693951) >> 13)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 2305843009213693951) >> 21)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 2305843009213693951) >> 29)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 2305843009213693951) >> 37)) & 255);
                            dest[59] = 
                (byte)((((src[7] & 2305843009213693951) >> 45)) & 255);
                            dest[60] = 
                (byte)((((src[7] & 2305843009213693951) >> 53)) & 255);
                        }
        private static void Unpack8LongValuesBE61(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 53) & 2305843009213693951) | ((((long)src[1]) << 45) & 9007199254740991) | ((((long)src[2]) << 37) & 35184372088831) | ((((long)src[3]) << 29) & 137438953471) | ((((long)src[4]) << 21) & 536870911) | ((((long)src[5]) << 13) & 2097151) | ((((long)src[6]) << 5) & 8191) | ((((long)src[7]) >> 3) & 31);            dest[1] = ((((long)src[7]) << 58) & 2305843009213693951) | ((((long)src[8]) << 50) & 288230376151711743) | ((((long)src[9]) << 42) & 1125899906842623) | ((((long)src[10]) << 34) & 4398046511103) | ((((long)src[11]) << 26) & 17179869183) | ((((long)src[12]) << 18) & 67108863) | ((((long)src[13]) << 10) & 262143) | ((((long)src[14]) << 2) & 1023) | ((((long)src[15]) >> 6) & 3);            dest[2] = ((((long)src[15]) << 55) & 2305843009213693951) | ((((long)src[16]) << 47) & 36028797018963967) | ((((long)src[17]) << 39) & 140737488355327) | ((((long)src[18]) << 31) & 549755813887) | ((((long)src[19]) << 23) & 2147483647) | ((((long)src[20]) << 15) & 8388607) | ((((long)src[21]) << 7) & 32767) | ((((long)src[22]) >> 1) & 127);            dest[3] = ((((long)src[22]) << 60) & 2305843009213693951) | ((((long)src[23]) << 52) & 1152921504606846975) | ((((long)src[24]) << 44) & 4503599627370495) | ((((long)src[25]) << 36) & 17592186044415) | ((((long)src[26]) << 28) & 68719476735) | ((((long)src[27]) << 20) & 268435455) | ((((long)src[28]) << 12) & 1048575) | ((((long)src[29]) << 4) & 4095) | ((((long)src[30]) >> 4) & 15);            dest[4] = ((((long)src[30]) << 57) & 2305843009213693951) | ((((long)src[31]) << 49) & 144115188075855871) | ((((long)src[32]) << 41) & 562949953421311) | ((((long)src[33]) << 33) & 2199023255551) | ((((long)src[34]) << 25) & 8589934591) | ((((long)src[35]) << 17) & 33554431) | ((((long)src[36]) << 9) & 131071) | ((((long)src[37]) << 1) & 511) | ((((long)src[38]) >> 7) & 1);            dest[5] = ((((long)src[38]) << 54) & 2305843009213693951) | ((((long)src[39]) << 46) & 18014398509481983) | ((((long)src[40]) << 38) & 70368744177663) | ((((long)src[41]) << 30) & 274877906943) | ((((long)src[42]) << 22) & 1073741823) | ((((long)src[43]) << 14) & 4194303) | ((((long)src[44]) << 6) & 16383) | ((((long)src[45]) >> 2) & 63);            dest[6] = ((((long)src[45]) << 59) & 2305843009213693951) | ((((long)src[46]) << 51) & 576460752303423487) | ((((long)src[47]) << 43) & 2251799813685247) | ((((long)src[48]) << 35) & 8796093022207) | ((((long)src[49]) << 27) & 34359738367) | ((((long)src[50]) << 19) & 134217727) | ((((long)src[51]) << 11) & 524287) | ((((long)src[52]) << 3) & 2047) | ((((long)src[53]) >> 5) & 7);            dest[7] = ((((long)src[53]) << 56) & 2305843009213693951) | ((((long)src[54]) << 48) & 72057594037927935) | ((((long)src[55]) << 40) & 281474976710655) | ((((long)src[56]) << 32) & 1099511627775) | ((((long)src[57]) << 24) & 4294967295) | ((((long)src[58]) << 16) & 16777215) | ((((long)src[59]) << 8) & 65535) | ((((long)src[60])) & 255);        }

        private static void Pack8LongValuesBE61(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 2305843009213693951) >> 53)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 2305843009213693951) >> 45)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 2305843009213693951) >> 37)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 2305843009213693951) >> 29)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 2305843009213693951) >> 21)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 2305843009213693951) >> 13)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 2305843009213693951) >> 5)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 2305843009213693951) << 3)                | ((src[1] & 2305843009213693951) >> 58)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 2305843009213693951) >> 50)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 2305843009213693951) >> 42)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 2305843009213693951) >> 34)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 2305843009213693951) >> 26)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 2305843009213693951) >> 18)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 2305843009213693951) >> 10)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 2305843009213693951) >> 2)) & 255);
                            dest[15] = 
                (byte)((((src[1] & 2305843009213693951) << 6)                | ((src[2] & 2305843009213693951) >> 55)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 2305843009213693951) >> 47)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 2305843009213693951) >> 39)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 2305843009213693951) >> 31)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 2305843009213693951) >> 23)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 2305843009213693951) >> 15)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 2305843009213693951) >> 7)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 2305843009213693951) << 1)                | ((src[3] & 2305843009213693951) >> 60)) & 255);
                            dest[23] = 
                (byte)((((src[3] & 2305843009213693951) >> 52)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 2305843009213693951) >> 44)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 2305843009213693951) >> 36)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 2305843009213693951) >> 28)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 2305843009213693951) >> 20)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 2305843009213693951) >> 12)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 2305843009213693951) >> 4)) & 255);
                            dest[30] = 
                (byte)((((src[3] & 2305843009213693951) << 4)                | ((src[4] & 2305843009213693951) >> 57)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 2305843009213693951) >> 49)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 2305843009213693951) >> 41)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 2305843009213693951) >> 33)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 2305843009213693951) >> 25)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 2305843009213693951) >> 17)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 2305843009213693951) >> 9)) & 255);
                            dest[37] = 
                (byte)((((src[4] & 2305843009213693951) >> 1)) & 255);
                            dest[38] = 
                (byte)((((src[4] & 2305843009213693951) << 7)                | ((src[5] & 2305843009213693951) >> 54)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 2305843009213693951) >> 46)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 2305843009213693951) >> 38)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 2305843009213693951) >> 30)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 2305843009213693951) >> 22)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 2305843009213693951) >> 14)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 2305843009213693951) >> 6)) & 255);
                            dest[45] = 
                (byte)((((src[5] & 2305843009213693951) << 2)                | ((src[6] & 2305843009213693951) >> 59)) & 255);
                            dest[46] = 
                (byte)((((src[6] & 2305843009213693951) >> 51)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 2305843009213693951) >> 43)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 2305843009213693951) >> 35)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 2305843009213693951) >> 27)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 2305843009213693951) >> 19)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 2305843009213693951) >> 11)) & 255);
                            dest[52] = 
                (byte)((((src[6] & 2305843009213693951) >> 3)) & 255);
                            dest[53] = 
                (byte)((((src[6] & 2305843009213693951) << 5)                | ((src[7] & 2305843009213693951) >> 56)) & 255);
                            dest[54] = 
                (byte)((((src[7] & 2305843009213693951) >> 48)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 2305843009213693951) >> 40)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 2305843009213693951) >> 32)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 2305843009213693951) >> 24)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 2305843009213693951) >> 16)) & 255);
                            dest[59] = 
                (byte)((((src[7] & 2305843009213693951) >> 8)) & 255);
                            dest[60] = 
                (byte)((((src[7] & 2305843009213693951))) & 255);
                        }
        private static void Unpack8LongValuesLE62(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935) | ((((long)src[7]) << 56) & 4611686018427387903);            dest[1] = ((((long)src[7]) >> 6) & 3) | ((((long)src[8]) << 2) & 1023) | ((((long)src[9]) << 10) & 262143) | ((((long)src[10]) << 18) & 67108863) | ((((long)src[11]) << 26) & 17179869183) | ((((long)src[12]) << 34) & 4398046511103) | ((((long)src[13]) << 42) & 1125899906842623) | ((((long)src[14]) << 50) & 288230376151711743) | ((((long)src[15]) << 58) & 4611686018427387903);            dest[2] = ((((long)src[15]) >> 4) & 15) | ((((long)src[16]) << 4) & 4095) | ((((long)src[17]) << 12) & 1048575) | ((((long)src[18]) << 20) & 268435455) | ((((long)src[19]) << 28) & 68719476735) | ((((long)src[20]) << 36) & 17592186044415) | ((((long)src[21]) << 44) & 4503599627370495) | ((((long)src[22]) << 52) & 1152921504606846975) | ((((long)src[23]) << 60) & 4611686018427387903);            dest[3] = ((((long)src[23]) >> 2) & 63) | ((((long)src[24]) << 6) & 16383) | ((((long)src[25]) << 14) & 4194303) | ((((long)src[26]) << 22) & 1073741823) | ((((long)src[27]) << 30) & 274877906943) | ((((long)src[28]) << 38) & 70368744177663) | ((((long)src[29]) << 46) & 18014398509481983) | ((((long)src[30]) << 54) & 4611686018427387903);            dest[4] = ((((long)src[31])) & 255) | ((((long)src[32]) << 8) & 65535) | ((((long)src[33]) << 16) & 16777215) | ((((long)src[34]) << 24) & 4294967295) | ((((long)src[35]) << 32) & 1099511627775) | ((((long)src[36]) << 40) & 281474976710655) | ((((long)src[37]) << 48) & 72057594037927935) | ((((long)src[38]) << 56) & 4611686018427387903);            dest[5] = ((((long)src[38]) >> 6) & 3) | ((((long)src[39]) << 2) & 1023) | ((((long)src[40]) << 10) & 262143) | ((((long)src[41]) << 18) & 67108863) | ((((long)src[42]) << 26) & 17179869183) | ((((long)src[43]) << 34) & 4398046511103) | ((((long)src[44]) << 42) & 1125899906842623) | ((((long)src[45]) << 50) & 288230376151711743) | ((((long)src[46]) << 58) & 4611686018427387903);            dest[6] = ((((long)src[46]) >> 4) & 15) | ((((long)src[47]) << 4) & 4095) | ((((long)src[48]) << 12) & 1048575) | ((((long)src[49]) << 20) & 268435455) | ((((long)src[50]) << 28) & 68719476735) | ((((long)src[51]) << 36) & 17592186044415) | ((((long)src[52]) << 44) & 4503599627370495) | ((((long)src[53]) << 52) & 1152921504606846975) | ((((long)src[54]) << 60) & 4611686018427387903);            dest[7] = ((((long)src[54]) >> 2) & 63) | ((((long)src[55]) << 6) & 16383) | ((((long)src[56]) << 14) & 4194303) | ((((long)src[57]) << 22) & 1073741823) | ((((long)src[58]) << 30) & 274877906943) | ((((long)src[59]) << 38) & 70368744177663) | ((((long)src[60]) << 46) & 18014398509481983) | ((((long)src[61]) << 54) & 4611686018427387903);        }

        private static void Pack8LongValuesLE62(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4611686018427387903))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4611686018427387903) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4611686018427387903) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4611686018427387903) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 4611686018427387903) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 4611686018427387903) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 4611686018427387903) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 4611686018427387903) >> 56)                | ((src[1] & 4611686018427387903) << 6)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 4611686018427387903) >> 2)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 4611686018427387903) >> 10)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 4611686018427387903) >> 18)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 4611686018427387903) >> 26)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 4611686018427387903) >> 34)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 4611686018427387903) >> 42)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 4611686018427387903) >> 50)) & 255);
                            dest[15] = 
                (byte)((((src[1] & 4611686018427387903) >> 58)                | ((src[2] & 4611686018427387903) << 4)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 4611686018427387903) >> 4)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 4611686018427387903) >> 12)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 4611686018427387903) >> 20)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 4611686018427387903) >> 28)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 4611686018427387903) >> 36)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 4611686018427387903) >> 44)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 4611686018427387903) >> 52)) & 255);
                            dest[23] = 
                (byte)((((src[2] & 4611686018427387903) >> 60)                | ((src[3] & 4611686018427387903) << 2)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 4611686018427387903) >> 6)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 4611686018427387903) >> 14)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 4611686018427387903) >> 22)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 4611686018427387903) >> 30)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 4611686018427387903) >> 38)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 4611686018427387903) >> 46)) & 255);
                            dest[30] = 
                (byte)((((src[3] & 4611686018427387903) >> 54)) & 255);
                            dest[31] = 
                (byte)((((src[4] & 4611686018427387903))) & 255);
                            dest[32] = 
                (byte)((((src[4] & 4611686018427387903) >> 8)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 4611686018427387903) >> 16)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 4611686018427387903) >> 24)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 4611686018427387903) >> 32)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 4611686018427387903) >> 40)) & 255);
                            dest[37] = 
                (byte)((((src[4] & 4611686018427387903) >> 48)) & 255);
                            dest[38] = 
                (byte)((((src[4] & 4611686018427387903) >> 56)                | ((src[5] & 4611686018427387903) << 6)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 4611686018427387903) >> 2)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 4611686018427387903) >> 10)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 4611686018427387903) >> 18)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 4611686018427387903) >> 26)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 4611686018427387903) >> 34)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 4611686018427387903) >> 42)) & 255);
                            dest[45] = 
                (byte)((((src[5] & 4611686018427387903) >> 50)) & 255);
                            dest[46] = 
                (byte)((((src[5] & 4611686018427387903) >> 58)                | ((src[6] & 4611686018427387903) << 4)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 4611686018427387903) >> 4)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 4611686018427387903) >> 12)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 4611686018427387903) >> 20)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 4611686018427387903) >> 28)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 4611686018427387903) >> 36)) & 255);
                            dest[52] = 
                (byte)((((src[6] & 4611686018427387903) >> 44)) & 255);
                            dest[53] = 
                (byte)((((src[6] & 4611686018427387903) >> 52)) & 255);
                            dest[54] = 
                (byte)((((src[6] & 4611686018427387903) >> 60)                | ((src[7] & 4611686018427387903) << 2)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 4611686018427387903) >> 6)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 4611686018427387903) >> 14)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 4611686018427387903) >> 22)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 4611686018427387903) >> 30)) & 255);
                            dest[59] = 
                (byte)((((src[7] & 4611686018427387903) >> 38)) & 255);
                            dest[60] = 
                (byte)((((src[7] & 4611686018427387903) >> 46)) & 255);
                            dest[61] = 
                (byte)((((src[7] & 4611686018427387903) >> 54)) & 255);
                        }
        private static void Unpack8LongValuesBE62(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 54) & 4611686018427387903) | ((((long)src[1]) << 46) & 18014398509481983) | ((((long)src[2]) << 38) & 70368744177663) | ((((long)src[3]) << 30) & 274877906943) | ((((long)src[4]) << 22) & 1073741823) | ((((long)src[5]) << 14) & 4194303) | ((((long)src[6]) << 6) & 16383) | ((((long)src[7]) >> 2) & 63);            dest[1] = ((((long)src[7]) << 60) & 4611686018427387903) | ((((long)src[8]) << 52) & 1152921504606846975) | ((((long)src[9]) << 44) & 4503599627370495) | ((((long)src[10]) << 36) & 17592186044415) | ((((long)src[11]) << 28) & 68719476735) | ((((long)src[12]) << 20) & 268435455) | ((((long)src[13]) << 12) & 1048575) | ((((long)src[14]) << 4) & 4095) | ((((long)src[15]) >> 4) & 15);            dest[2] = ((((long)src[15]) << 58) & 4611686018427387903) | ((((long)src[16]) << 50) & 288230376151711743) | ((((long)src[17]) << 42) & 1125899906842623) | ((((long)src[18]) << 34) & 4398046511103) | ((((long)src[19]) << 26) & 17179869183) | ((((long)src[20]) << 18) & 67108863) | ((((long)src[21]) << 10) & 262143) | ((((long)src[22]) << 2) & 1023) | ((((long)src[23]) >> 6) & 3);            dest[3] = ((((long)src[23]) << 56) & 4611686018427387903) | ((((long)src[24]) << 48) & 72057594037927935) | ((((long)src[25]) << 40) & 281474976710655) | ((((long)src[26]) << 32) & 1099511627775) | ((((long)src[27]) << 24) & 4294967295) | ((((long)src[28]) << 16) & 16777215) | ((((long)src[29]) << 8) & 65535) | ((((long)src[30])) & 255);            dest[4] = ((((long)src[31]) << 54) & 4611686018427387903) | ((((long)src[32]) << 46) & 18014398509481983) | ((((long)src[33]) << 38) & 70368744177663) | ((((long)src[34]) << 30) & 274877906943) | ((((long)src[35]) << 22) & 1073741823) | ((((long)src[36]) << 14) & 4194303) | ((((long)src[37]) << 6) & 16383) | ((((long)src[38]) >> 2) & 63);            dest[5] = ((((long)src[38]) << 60) & 4611686018427387903) | ((((long)src[39]) << 52) & 1152921504606846975) | ((((long)src[40]) << 44) & 4503599627370495) | ((((long)src[41]) << 36) & 17592186044415) | ((((long)src[42]) << 28) & 68719476735) | ((((long)src[43]) << 20) & 268435455) | ((((long)src[44]) << 12) & 1048575) | ((((long)src[45]) << 4) & 4095) | ((((long)src[46]) >> 4) & 15);            dest[6] = ((((long)src[46]) << 58) & 4611686018427387903) | ((((long)src[47]) << 50) & 288230376151711743) | ((((long)src[48]) << 42) & 1125899906842623) | ((((long)src[49]) << 34) & 4398046511103) | ((((long)src[50]) << 26) & 17179869183) | ((((long)src[51]) << 18) & 67108863) | ((((long)src[52]) << 10) & 262143) | ((((long)src[53]) << 2) & 1023) | ((((long)src[54]) >> 6) & 3);            dest[7] = ((((long)src[54]) << 56) & 4611686018427387903) | ((((long)src[55]) << 48) & 72057594037927935) | ((((long)src[56]) << 40) & 281474976710655) | ((((long)src[57]) << 32) & 1099511627775) | ((((long)src[58]) << 24) & 4294967295) | ((((long)src[59]) << 16) & 16777215) | ((((long)src[60]) << 8) & 65535) | ((((long)src[61])) & 255);        }

        private static void Pack8LongValuesBE62(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 4611686018427387903) >> 54)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 4611686018427387903) >> 46)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 4611686018427387903) >> 38)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 4611686018427387903) >> 30)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 4611686018427387903) >> 22)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 4611686018427387903) >> 14)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 4611686018427387903) >> 6)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 4611686018427387903) << 2)                | ((src[1] & 4611686018427387903) >> 60)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 4611686018427387903) >> 52)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 4611686018427387903) >> 44)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 4611686018427387903) >> 36)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 4611686018427387903) >> 28)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 4611686018427387903) >> 20)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 4611686018427387903) >> 12)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 4611686018427387903) >> 4)) & 255);
                            dest[15] = 
                (byte)((((src[1] & 4611686018427387903) << 4)                | ((src[2] & 4611686018427387903) >> 58)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 4611686018427387903) >> 50)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 4611686018427387903) >> 42)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 4611686018427387903) >> 34)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 4611686018427387903) >> 26)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 4611686018427387903) >> 18)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 4611686018427387903) >> 10)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 4611686018427387903) >> 2)) & 255);
                            dest[23] = 
                (byte)((((src[2] & 4611686018427387903) << 6)                | ((src[3] & 4611686018427387903) >> 56)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 4611686018427387903) >> 48)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 4611686018427387903) >> 40)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 4611686018427387903) >> 32)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 4611686018427387903) >> 24)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 4611686018427387903) >> 16)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 4611686018427387903) >> 8)) & 255);
                            dest[30] = 
                (byte)((((src[3] & 4611686018427387903))) & 255);
                            dest[31] = 
                (byte)((((src[4] & 4611686018427387903) >> 54)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 4611686018427387903) >> 46)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 4611686018427387903) >> 38)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 4611686018427387903) >> 30)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 4611686018427387903) >> 22)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 4611686018427387903) >> 14)) & 255);
                            dest[37] = 
                (byte)((((src[4] & 4611686018427387903) >> 6)) & 255);
                            dest[38] = 
                (byte)((((src[4] & 4611686018427387903) << 2)                | ((src[5] & 4611686018427387903) >> 60)) & 255);
                            dest[39] = 
                (byte)((((src[5] & 4611686018427387903) >> 52)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 4611686018427387903) >> 44)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 4611686018427387903) >> 36)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 4611686018427387903) >> 28)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 4611686018427387903) >> 20)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 4611686018427387903) >> 12)) & 255);
                            dest[45] = 
                (byte)((((src[5] & 4611686018427387903) >> 4)) & 255);
                            dest[46] = 
                (byte)((((src[5] & 4611686018427387903) << 4)                | ((src[6] & 4611686018427387903) >> 58)) & 255);
                            dest[47] = 
                (byte)((((src[6] & 4611686018427387903) >> 50)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 4611686018427387903) >> 42)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 4611686018427387903) >> 34)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 4611686018427387903) >> 26)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 4611686018427387903) >> 18)) & 255);
                            dest[52] = 
                (byte)((((src[6] & 4611686018427387903) >> 10)) & 255);
                            dest[53] = 
                (byte)((((src[6] & 4611686018427387903) >> 2)) & 255);
                            dest[54] = 
                (byte)((((src[6] & 4611686018427387903) << 6)                | ((src[7] & 4611686018427387903) >> 56)) & 255);
                            dest[55] = 
                (byte)((((src[7] & 4611686018427387903) >> 48)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 4611686018427387903) >> 40)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 4611686018427387903) >> 32)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 4611686018427387903) >> 24)) & 255);
                            dest[59] = 
                (byte)((((src[7] & 4611686018427387903) >> 16)) & 255);
                            dest[60] = 
                (byte)((((src[7] & 4611686018427387903) >> 8)) & 255);
                            dest[61] = 
                (byte)((((src[7] & 4611686018427387903))) & 255);
                        }
        private static void Unpack8LongValuesLE63(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935) | ((((long)src[7]) << 56) & 9223372036854775807);            dest[1] = ((((long)src[7]) >> 7) & 1) | ((((long)src[8]) << 1) & 511) | ((((long)src[9]) << 9) & 131071) | ((((long)src[10]) << 17) & 33554431) | ((((long)src[11]) << 25) & 8589934591) | ((((long)src[12]) << 33) & 2199023255551) | ((((long)src[13]) << 41) & 562949953421311) | ((((long)src[14]) << 49) & 144115188075855871) | ((((long)src[15]) << 57) & 9223372036854775807);            dest[2] = ((((long)src[15]) >> 6) & 3) | ((((long)src[16]) << 2) & 1023) | ((((long)src[17]) << 10) & 262143) | ((((long)src[18]) << 18) & 67108863) | ((((long)src[19]) << 26) & 17179869183) | ((((long)src[20]) << 34) & 4398046511103) | ((((long)src[21]) << 42) & 1125899906842623) | ((((long)src[22]) << 50) & 288230376151711743) | ((((long)src[23]) << 58) & 9223372036854775807);            dest[3] = ((((long)src[23]) >> 5) & 7) | ((((long)src[24]) << 3) & 2047) | ((((long)src[25]) << 11) & 524287) | ((((long)src[26]) << 19) & 134217727) | ((((long)src[27]) << 27) & 34359738367) | ((((long)src[28]) << 35) & 8796093022207) | ((((long)src[29]) << 43) & 2251799813685247) | ((((long)src[30]) << 51) & 576460752303423487) | ((((long)src[31]) << 59) & 9223372036854775807);            dest[4] = ((((long)src[31]) >> 4) & 15) | ((((long)src[32]) << 4) & 4095) | ((((long)src[33]) << 12) & 1048575) | ((((long)src[34]) << 20) & 268435455) | ((((long)src[35]) << 28) & 68719476735) | ((((long)src[36]) << 36) & 17592186044415) | ((((long)src[37]) << 44) & 4503599627370495) | ((((long)src[38]) << 52) & 1152921504606846975) | ((((long)src[39]) << 60) & 9223372036854775807);            dest[5] = ((((long)src[39]) >> 3) & 31) | ((((long)src[40]) << 5) & 8191) | ((((long)src[41]) << 13) & 2097151) | ((((long)src[42]) << 21) & 536870911) | ((((long)src[43]) << 29) & 137438953471) | ((((long)src[44]) << 37) & 35184372088831) | ((((long)src[45]) << 45) & 9007199254740991) | ((((long)src[46]) << 53) & 2305843009213693951) | ((((long)src[47]) << 61) & 9223372036854775807);            dest[6] = ((((long)src[47]) >> 2) & 63) | ((((long)src[48]) << 6) & 16383) | ((((long)src[49]) << 14) & 4194303) | ((((long)src[50]) << 22) & 1073741823) | ((((long)src[51]) << 30) & 274877906943) | ((((long)src[52]) << 38) & 70368744177663) | ((((long)src[53]) << 46) & 18014398509481983) | ((((long)src[54]) << 54) & 4611686018427387903) | ((((long)src[55]) << 62) & 9223372036854775807);            dest[7] = ((((long)src[55]) >> 1) & 127) | ((((long)src[56]) << 7) & 32767) | ((((long)src[57]) << 15) & 8388607) | ((((long)src[58]) << 23) & 2147483647) | ((((long)src[59]) << 31) & 549755813887) | ((((long)src[60]) << 39) & 140737488355327) | ((((long)src[61]) << 47) & 36028797018963967) | ((((long)src[62]) << 55) & 9223372036854775807);        }

        private static void Pack8LongValuesLE63(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 9223372036854775807))) & 255);
                            dest[1] = 
                (byte)((((src[0] & 9223372036854775807) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 9223372036854775807) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 9223372036854775807) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 9223372036854775807) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 9223372036854775807) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 9223372036854775807) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 9223372036854775807) >> 56)                | ((src[1] & 9223372036854775807) << 7)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 9223372036854775807) >> 1)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 9223372036854775807) >> 9)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 9223372036854775807) >> 17)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 9223372036854775807) >> 25)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 9223372036854775807) >> 33)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 9223372036854775807) >> 41)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 9223372036854775807) >> 49)) & 255);
                            dest[15] = 
                (byte)((((src[1] & 9223372036854775807) >> 57)                | ((src[2] & 9223372036854775807) << 6)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 9223372036854775807) >> 2)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 9223372036854775807) >> 10)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 9223372036854775807) >> 18)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 9223372036854775807) >> 26)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 9223372036854775807) >> 34)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 9223372036854775807) >> 42)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 9223372036854775807) >> 50)) & 255);
                            dest[23] = 
                (byte)((((src[2] & 9223372036854775807) >> 58)                | ((src[3] & 9223372036854775807) << 5)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 9223372036854775807) >> 3)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 9223372036854775807) >> 11)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 9223372036854775807) >> 19)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 9223372036854775807) >> 27)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 9223372036854775807) >> 35)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 9223372036854775807) >> 43)) & 255);
                            dest[30] = 
                (byte)((((src[3] & 9223372036854775807) >> 51)) & 255);
                            dest[31] = 
                (byte)((((src[3] & 9223372036854775807) >> 59)                | ((src[4] & 9223372036854775807) << 4)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 9223372036854775807) >> 4)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 9223372036854775807) >> 12)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 9223372036854775807) >> 20)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 9223372036854775807) >> 28)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 9223372036854775807) >> 36)) & 255);
                            dest[37] = 
                (byte)((((src[4] & 9223372036854775807) >> 44)) & 255);
                            dest[38] = 
                (byte)((((src[4] & 9223372036854775807) >> 52)) & 255);
                            dest[39] = 
                (byte)((((src[4] & 9223372036854775807) >> 60)                | ((src[5] & 9223372036854775807) << 3)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 9223372036854775807) >> 5)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 9223372036854775807) >> 13)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 9223372036854775807) >> 21)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 9223372036854775807) >> 29)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 9223372036854775807) >> 37)) & 255);
                            dest[45] = 
                (byte)((((src[5] & 9223372036854775807) >> 45)) & 255);
                            dest[46] = 
                (byte)((((src[5] & 9223372036854775807) >> 53)) & 255);
                            dest[47] = 
                (byte)((((src[5] & 9223372036854775807) >> 61)                | ((src[6] & 9223372036854775807) << 2)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 9223372036854775807) >> 6)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 9223372036854775807) >> 14)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 9223372036854775807) >> 22)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 9223372036854775807) >> 30)) & 255);
                            dest[52] = 
                (byte)((((src[6] & 9223372036854775807) >> 38)) & 255);
                            dest[53] = 
                (byte)((((src[6] & 9223372036854775807) >> 46)) & 255);
                            dest[54] = 
                (byte)((((src[6] & 9223372036854775807) >> 54)) & 255);
                            dest[55] = 
                (byte)((((src[6] & 9223372036854775807) >> 62)                | ((src[7] & 9223372036854775807) << 1)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 9223372036854775807) >> 7)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 9223372036854775807) >> 15)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 9223372036854775807) >> 23)) & 255);
                            dest[59] = 
                (byte)((((src[7] & 9223372036854775807) >> 31)) & 255);
                            dest[60] = 
                (byte)((((src[7] & 9223372036854775807) >> 39)) & 255);
                            dest[61] = 
                (byte)((((src[7] & 9223372036854775807) >> 47)) & 255);
                            dest[62] = 
                (byte)((((src[7] & 9223372036854775807) >> 55)) & 255);
                        }
        private static void Unpack8LongValuesBE63(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 55) & 9223372036854775807) | ((((long)src[1]) << 47) & 36028797018963967) | ((((long)src[2]) << 39) & 140737488355327) | ((((long)src[3]) << 31) & 549755813887) | ((((long)src[4]) << 23) & 2147483647) | ((((long)src[5]) << 15) & 8388607) | ((((long)src[6]) << 7) & 32767) | ((((long)src[7]) >> 1) & 127);            dest[1] = ((((long)src[7]) << 62) & 9223372036854775807) | ((((long)src[8]) << 54) & 4611686018427387903) | ((((long)src[9]) << 46) & 18014398509481983) | ((((long)src[10]) << 38) & 70368744177663) | ((((long)src[11]) << 30) & 274877906943) | ((((long)src[12]) << 22) & 1073741823) | ((((long)src[13]) << 14) & 4194303) | ((((long)src[14]) << 6) & 16383) | ((((long)src[15]) >> 2) & 63);            dest[2] = ((((long)src[15]) << 61) & 9223372036854775807) | ((((long)src[16]) << 53) & 2305843009213693951) | ((((long)src[17]) << 45) & 9007199254740991) | ((((long)src[18]) << 37) & 35184372088831) | ((((long)src[19]) << 29) & 137438953471) | ((((long)src[20]) << 21) & 536870911) | ((((long)src[21]) << 13) & 2097151) | ((((long)src[22]) << 5) & 8191) | ((((long)src[23]) >> 3) & 31);            dest[3] = ((((long)src[23]) << 60) & 9223372036854775807) | ((((long)src[24]) << 52) & 1152921504606846975) | ((((long)src[25]) << 44) & 4503599627370495) | ((((long)src[26]) << 36) & 17592186044415) | ((((long)src[27]) << 28) & 68719476735) | ((((long)src[28]) << 20) & 268435455) | ((((long)src[29]) << 12) & 1048575) | ((((long)src[30]) << 4) & 4095) | ((((long)src[31]) >> 4) & 15);            dest[4] = ((((long)src[31]) << 59) & 9223372036854775807) | ((((long)src[32]) << 51) & 576460752303423487) | ((((long)src[33]) << 43) & 2251799813685247) | ((((long)src[34]) << 35) & 8796093022207) | ((((long)src[35]) << 27) & 34359738367) | ((((long)src[36]) << 19) & 134217727) | ((((long)src[37]) << 11) & 524287) | ((((long)src[38]) << 3) & 2047) | ((((long)src[39]) >> 5) & 7);            dest[5] = ((((long)src[39]) << 58) & 9223372036854775807) | ((((long)src[40]) << 50) & 288230376151711743) | ((((long)src[41]) << 42) & 1125899906842623) | ((((long)src[42]) << 34) & 4398046511103) | ((((long)src[43]) << 26) & 17179869183) | ((((long)src[44]) << 18) & 67108863) | ((((long)src[45]) << 10) & 262143) | ((((long)src[46]) << 2) & 1023) | ((((long)src[47]) >> 6) & 3);            dest[6] = ((((long)src[47]) << 57) & 9223372036854775807) | ((((long)src[48]) << 49) & 144115188075855871) | ((((long)src[49]) << 41) & 562949953421311) | ((((long)src[50]) << 33) & 2199023255551) | ((((long)src[51]) << 25) & 8589934591) | ((((long)src[52]) << 17) & 33554431) | ((((long)src[53]) << 9) & 131071) | ((((long)src[54]) << 1) & 511) | ((((long)src[55]) >> 7) & 1);            dest[7] = ((((long)src[55]) << 56) & 9223372036854775807) | ((((long)src[56]) << 48) & 72057594037927935) | ((((long)src[57]) << 40) & 281474976710655) | ((((long)src[58]) << 32) & 1099511627775) | ((((long)src[59]) << 24) & 4294967295) | ((((long)src[60]) << 16) & 16777215) | ((((long)src[61]) << 8) & 65535) | ((((long)src[62])) & 255);        }

        private static void Pack8LongValuesBE63(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & 9223372036854775807) >> 55)) & 255);
                            dest[1] = 
                (byte)((((src[0] & 9223372036854775807) >> 47)) & 255);
                            dest[2] = 
                (byte)((((src[0] & 9223372036854775807) >> 39)) & 255);
                            dest[3] = 
                (byte)((((src[0] & 9223372036854775807) >> 31)) & 255);
                            dest[4] = 
                (byte)((((src[0] & 9223372036854775807) >> 23)) & 255);
                            dest[5] = 
                (byte)((((src[0] & 9223372036854775807) >> 15)) & 255);
                            dest[6] = 
                (byte)((((src[0] & 9223372036854775807) >> 7)) & 255);
                            dest[7] = 
                (byte)((((src[0] & 9223372036854775807) << 1)                | ((src[1] & 9223372036854775807) >> 62)) & 255);
                            dest[8] = 
                (byte)((((src[1] & 9223372036854775807) >> 54)) & 255);
                            dest[9] = 
                (byte)((((src[1] & 9223372036854775807) >> 46)) & 255);
                            dest[10] = 
                (byte)((((src[1] & 9223372036854775807) >> 38)) & 255);
                            dest[11] = 
                (byte)((((src[1] & 9223372036854775807) >> 30)) & 255);
                            dest[12] = 
                (byte)((((src[1] & 9223372036854775807) >> 22)) & 255);
                            dest[13] = 
                (byte)((((src[1] & 9223372036854775807) >> 14)) & 255);
                            dest[14] = 
                (byte)((((src[1] & 9223372036854775807) >> 6)) & 255);
                            dest[15] = 
                (byte)((((src[1] & 9223372036854775807) << 2)                | ((src[2] & 9223372036854775807) >> 61)) & 255);
                            dest[16] = 
                (byte)((((src[2] & 9223372036854775807) >> 53)) & 255);
                            dest[17] = 
                (byte)((((src[2] & 9223372036854775807) >> 45)) & 255);
                            dest[18] = 
                (byte)((((src[2] & 9223372036854775807) >> 37)) & 255);
                            dest[19] = 
                (byte)((((src[2] & 9223372036854775807) >> 29)) & 255);
                            dest[20] = 
                (byte)((((src[2] & 9223372036854775807) >> 21)) & 255);
                            dest[21] = 
                (byte)((((src[2] & 9223372036854775807) >> 13)) & 255);
                            dest[22] = 
                (byte)((((src[2] & 9223372036854775807) >> 5)) & 255);
                            dest[23] = 
                (byte)((((src[2] & 9223372036854775807) << 3)                | ((src[3] & 9223372036854775807) >> 60)) & 255);
                            dest[24] = 
                (byte)((((src[3] & 9223372036854775807) >> 52)) & 255);
                            dest[25] = 
                (byte)((((src[3] & 9223372036854775807) >> 44)) & 255);
                            dest[26] = 
                (byte)((((src[3] & 9223372036854775807) >> 36)) & 255);
                            dest[27] = 
                (byte)((((src[3] & 9223372036854775807) >> 28)) & 255);
                            dest[28] = 
                (byte)((((src[3] & 9223372036854775807) >> 20)) & 255);
                            dest[29] = 
                (byte)((((src[3] & 9223372036854775807) >> 12)) & 255);
                            dest[30] = 
                (byte)((((src[3] & 9223372036854775807) >> 4)) & 255);
                            dest[31] = 
                (byte)((((src[3] & 9223372036854775807) << 4)                | ((src[4] & 9223372036854775807) >> 59)) & 255);
                            dest[32] = 
                (byte)((((src[4] & 9223372036854775807) >> 51)) & 255);
                            dest[33] = 
                (byte)((((src[4] & 9223372036854775807) >> 43)) & 255);
                            dest[34] = 
                (byte)((((src[4] & 9223372036854775807) >> 35)) & 255);
                            dest[35] = 
                (byte)((((src[4] & 9223372036854775807) >> 27)) & 255);
                            dest[36] = 
                (byte)((((src[4] & 9223372036854775807) >> 19)) & 255);
                            dest[37] = 
                (byte)((((src[4] & 9223372036854775807) >> 11)) & 255);
                            dest[38] = 
                (byte)((((src[4] & 9223372036854775807) >> 3)) & 255);
                            dest[39] = 
                (byte)((((src[4] & 9223372036854775807) << 5)                | ((src[5] & 9223372036854775807) >> 58)) & 255);
                            dest[40] = 
                (byte)((((src[5] & 9223372036854775807) >> 50)) & 255);
                            dest[41] = 
                (byte)((((src[5] & 9223372036854775807) >> 42)) & 255);
                            dest[42] = 
                (byte)((((src[5] & 9223372036854775807) >> 34)) & 255);
                            dest[43] = 
                (byte)((((src[5] & 9223372036854775807) >> 26)) & 255);
                            dest[44] = 
                (byte)((((src[5] & 9223372036854775807) >> 18)) & 255);
                            dest[45] = 
                (byte)((((src[5] & 9223372036854775807) >> 10)) & 255);
                            dest[46] = 
                (byte)((((src[5] & 9223372036854775807) >> 2)) & 255);
                            dest[47] = 
                (byte)((((src[5] & 9223372036854775807) << 6)                | ((src[6] & 9223372036854775807) >> 57)) & 255);
                            dest[48] = 
                (byte)((((src[6] & 9223372036854775807) >> 49)) & 255);
                            dest[49] = 
                (byte)((((src[6] & 9223372036854775807) >> 41)) & 255);
                            dest[50] = 
                (byte)((((src[6] & 9223372036854775807) >> 33)) & 255);
                            dest[51] = 
                (byte)((((src[6] & 9223372036854775807) >> 25)) & 255);
                            dest[52] = 
                (byte)((((src[6] & 9223372036854775807) >> 17)) & 255);
                            dest[53] = 
                (byte)((((src[6] & 9223372036854775807) >> 9)) & 255);
                            dest[54] = 
                (byte)((((src[6] & 9223372036854775807) >> 1)) & 255);
                            dest[55] = 
                (byte)((((src[6] & 9223372036854775807) << 7)                | ((src[7] & 9223372036854775807) >> 56)) & 255);
                            dest[56] = 
                (byte)((((src[7] & 9223372036854775807) >> 48)) & 255);
                            dest[57] = 
                (byte)((((src[7] & 9223372036854775807) >> 40)) & 255);
                            dest[58] = 
                (byte)((((src[7] & 9223372036854775807) >> 32)) & 255);
                            dest[59] = 
                (byte)((((src[7] & 9223372036854775807) >> 24)) & 255);
                            dest[60] = 
                (byte)((((src[7] & 9223372036854775807) >> 16)) & 255);
                            dest[61] = 
                (byte)((((src[7] & 9223372036854775807) >> 8)) & 255);
                            dest[62] = 
                (byte)((((src[7] & 9223372036854775807))) & 255);
                        }
        private static void Unpack8LongValuesLE64(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0])) & 255) | ((((long)src[1]) << 8) & 65535) | ((((long)src[2]) << 16) & 16777215) | ((((long)src[3]) << 24) & 4294967295) | ((((long)src[4]) << 32) & 1099511627775) | ((((long)src[5]) << 40) & 281474976710655) | ((((long)src[6]) << 48) & 72057594037927935) | ((((long)src[7]) << 56) & -1);            dest[1] = ((((long)src[8])) & 255) | ((((long)src[9]) << 8) & 65535) | ((((long)src[10]) << 16) & 16777215) | ((((long)src[11]) << 24) & 4294967295) | ((((long)src[12]) << 32) & 1099511627775) | ((((long)src[13]) << 40) & 281474976710655) | ((((long)src[14]) << 48) & 72057594037927935) | ((((long)src[15]) << 56) & -1);            dest[2] = ((((long)src[16])) & 255) | ((((long)src[17]) << 8) & 65535) | ((((long)src[18]) << 16) & 16777215) | ((((long)src[19]) << 24) & 4294967295) | ((((long)src[20]) << 32) & 1099511627775) | ((((long)src[21]) << 40) & 281474976710655) | ((((long)src[22]) << 48) & 72057594037927935) | ((((long)src[23]) << 56) & -1);            dest[3] = ((((long)src[24])) & 255) | ((((long)src[25]) << 8) & 65535) | ((((long)src[26]) << 16) & 16777215) | ((((long)src[27]) << 24) & 4294967295) | ((((long)src[28]) << 32) & 1099511627775) | ((((long)src[29]) << 40) & 281474976710655) | ((((long)src[30]) << 48) & 72057594037927935) | ((((long)src[31]) << 56) & -1);            dest[4] = ((((long)src[32])) & 255) | ((((long)src[33]) << 8) & 65535) | ((((long)src[34]) << 16) & 16777215) | ((((long)src[35]) << 24) & 4294967295) | ((((long)src[36]) << 32) & 1099511627775) | ((((long)src[37]) << 40) & 281474976710655) | ((((long)src[38]) << 48) & 72057594037927935) | ((((long)src[39]) << 56) & -1);            dest[5] = ((((long)src[40])) & 255) | ((((long)src[41]) << 8) & 65535) | ((((long)src[42]) << 16) & 16777215) | ((((long)src[43]) << 24) & 4294967295) | ((((long)src[44]) << 32) & 1099511627775) | ((((long)src[45]) << 40) & 281474976710655) | ((((long)src[46]) << 48) & 72057594037927935) | ((((long)src[47]) << 56) & -1);            dest[6] = ((((long)src[48])) & 255) | ((((long)src[49]) << 8) & 65535) | ((((long)src[50]) << 16) & 16777215) | ((((long)src[51]) << 24) & 4294967295) | ((((long)src[52]) << 32) & 1099511627775) | ((((long)src[53]) << 40) & 281474976710655) | ((((long)src[54]) << 48) & 72057594037927935) | ((((long)src[55]) << 56) & -1);            dest[7] = ((((long)src[56])) & 255) | ((((long)src[57]) << 8) & 65535) | ((((long)src[58]) << 16) & 16777215) | ((((long)src[59]) << 24) & 4294967295) | ((((long)src[60]) << 32) & 1099511627775) | ((((long)src[61]) << 40) & 281474976710655) | ((((long)src[62]) << 48) & 72057594037927935) | ((((long)src[63]) << 56) & -1);        }

        private static void Pack8LongValuesLE64(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & -1))) & 255);
                            dest[1] = 
                (byte)((((src[0] & -1) >> 8)) & 255);
                            dest[2] = 
                (byte)((((src[0] & -1) >> 16)) & 255);
                            dest[3] = 
                (byte)((((src[0] & -1) >> 24)) & 255);
                            dest[4] = 
                (byte)((((src[0] & -1) >> 32)) & 255);
                            dest[5] = 
                (byte)((((src[0] & -1) >> 40)) & 255);
                            dest[6] = 
                (byte)((((src[0] & -1) >> 48)) & 255);
                            dest[7] = 
                (byte)((((src[0] & -1) >> 56)) & 255);
                            dest[8] = 
                (byte)((((src[1] & -1))) & 255);
                            dest[9] = 
                (byte)((((src[1] & -1) >> 8)) & 255);
                            dest[10] = 
                (byte)((((src[1] & -1) >> 16)) & 255);
                            dest[11] = 
                (byte)((((src[1] & -1) >> 24)) & 255);
                            dest[12] = 
                (byte)((((src[1] & -1) >> 32)) & 255);
                            dest[13] = 
                (byte)((((src[1] & -1) >> 40)) & 255);
                            dest[14] = 
                (byte)((((src[1] & -1) >> 48)) & 255);
                            dest[15] = 
                (byte)((((src[1] & -1) >> 56)) & 255);
                            dest[16] = 
                (byte)((((src[2] & -1))) & 255);
                            dest[17] = 
                (byte)((((src[2] & -1) >> 8)) & 255);
                            dest[18] = 
                (byte)((((src[2] & -1) >> 16)) & 255);
                            dest[19] = 
                (byte)((((src[2] & -1) >> 24)) & 255);
                            dest[20] = 
                (byte)((((src[2] & -1) >> 32)) & 255);
                            dest[21] = 
                (byte)((((src[2] & -1) >> 40)) & 255);
                            dest[22] = 
                (byte)((((src[2] & -1) >> 48)) & 255);
                            dest[23] = 
                (byte)((((src[2] & -1) >> 56)) & 255);
                            dest[24] = 
                (byte)((((src[3] & -1))) & 255);
                            dest[25] = 
                (byte)((((src[3] & -1) >> 8)) & 255);
                            dest[26] = 
                (byte)((((src[3] & -1) >> 16)) & 255);
                            dest[27] = 
                (byte)((((src[3] & -1) >> 24)) & 255);
                            dest[28] = 
                (byte)((((src[3] & -1) >> 32)) & 255);
                            dest[29] = 
                (byte)((((src[3] & -1) >> 40)) & 255);
                            dest[30] = 
                (byte)((((src[3] & -1) >> 48)) & 255);
                            dest[31] = 
                (byte)((((src[3] & -1) >> 56)) & 255);
                            dest[32] = 
                (byte)((((src[4] & -1))) & 255);
                            dest[33] = 
                (byte)((((src[4] & -1) >> 8)) & 255);
                            dest[34] = 
                (byte)((((src[4] & -1) >> 16)) & 255);
                            dest[35] = 
                (byte)((((src[4] & -1) >> 24)) & 255);
                            dest[36] = 
                (byte)((((src[4] & -1) >> 32)) & 255);
                            dest[37] = 
                (byte)((((src[4] & -1) >> 40)) & 255);
                            dest[38] = 
                (byte)((((src[4] & -1) >> 48)) & 255);
                            dest[39] = 
                (byte)((((src[4] & -1) >> 56)) & 255);
                            dest[40] = 
                (byte)((((src[5] & -1))) & 255);
                            dest[41] = 
                (byte)((((src[5] & -1) >> 8)) & 255);
                            dest[42] = 
                (byte)((((src[5] & -1) >> 16)) & 255);
                            dest[43] = 
                (byte)((((src[5] & -1) >> 24)) & 255);
                            dest[44] = 
                (byte)((((src[5] & -1) >> 32)) & 255);
                            dest[45] = 
                (byte)((((src[5] & -1) >> 40)) & 255);
                            dest[46] = 
                (byte)((((src[5] & -1) >> 48)) & 255);
                            dest[47] = 
                (byte)((((src[5] & -1) >> 56)) & 255);
                            dest[48] = 
                (byte)((((src[6] & -1))) & 255);
                            dest[49] = 
                (byte)((((src[6] & -1) >> 8)) & 255);
                            dest[50] = 
                (byte)((((src[6] & -1) >> 16)) & 255);
                            dest[51] = 
                (byte)((((src[6] & -1) >> 24)) & 255);
                            dest[52] = 
                (byte)((((src[6] & -1) >> 32)) & 255);
                            dest[53] = 
                (byte)((((src[6] & -1) >> 40)) & 255);
                            dest[54] = 
                (byte)((((src[6] & -1) >> 48)) & 255);
                            dest[55] = 
                (byte)((((src[6] & -1) >> 56)) & 255);
                            dest[56] = 
                (byte)((((src[7] & -1))) & 255);
                            dest[57] = 
                (byte)((((src[7] & -1) >> 8)) & 255);
                            dest[58] = 
                (byte)((((src[7] & -1) >> 16)) & 255);
                            dest[59] = 
                (byte)((((src[7] & -1) >> 24)) & 255);
                            dest[60] = 
                (byte)((((src[7] & -1) >> 32)) & 255);
                            dest[61] = 
                (byte)((((src[7] & -1) >> 40)) & 255);
                            dest[62] = 
                (byte)((((src[7] & -1) >> 48)) & 255);
                            dest[63] = 
                (byte)((((src[7] & -1) >> 56)) & 255);
                        }
        private static void Unpack8LongValuesBE64(Span<byte> src, Span<long> dest) {
                    dest[0] = ((((long)src[0]) << 56) & -1) | ((((long)src[1]) << 48) & 72057594037927935) | ((((long)src[2]) << 40) & 281474976710655) | ((((long)src[3]) << 32) & 1099511627775) | ((((long)src[4]) << 24) & 4294967295) | ((((long)src[5]) << 16) & 16777215) | ((((long)src[6]) << 8) & 65535) | ((((long)src[7])) & 255);            dest[1] = ((((long)src[8]) << 56) & -1) | ((((long)src[9]) << 48) & 72057594037927935) | ((((long)src[10]) << 40) & 281474976710655) | ((((long)src[11]) << 32) & 1099511627775) | ((((long)src[12]) << 24) & 4294967295) | ((((long)src[13]) << 16) & 16777215) | ((((long)src[14]) << 8) & 65535) | ((((long)src[15])) & 255);            dest[2] = ((((long)src[16]) << 56) & -1) | ((((long)src[17]) << 48) & 72057594037927935) | ((((long)src[18]) << 40) & 281474976710655) | ((((long)src[19]) << 32) & 1099511627775) | ((((long)src[20]) << 24) & 4294967295) | ((((long)src[21]) << 16) & 16777215) | ((((long)src[22]) << 8) & 65535) | ((((long)src[23])) & 255);            dest[3] = ((((long)src[24]) << 56) & -1) | ((((long)src[25]) << 48) & 72057594037927935) | ((((long)src[26]) << 40) & 281474976710655) | ((((long)src[27]) << 32) & 1099511627775) | ((((long)src[28]) << 24) & 4294967295) | ((((long)src[29]) << 16) & 16777215) | ((((long)src[30]) << 8) & 65535) | ((((long)src[31])) & 255);            dest[4] = ((((long)src[32]) << 56) & -1) | ((((long)src[33]) << 48) & 72057594037927935) | ((((long)src[34]) << 40) & 281474976710655) | ((((long)src[35]) << 32) & 1099511627775) | ((((long)src[36]) << 24) & 4294967295) | ((((long)src[37]) << 16) & 16777215) | ((((long)src[38]) << 8) & 65535) | ((((long)src[39])) & 255);            dest[5] = ((((long)src[40]) << 56) & -1) | ((((long)src[41]) << 48) & 72057594037927935) | ((((long)src[42]) << 40) & 281474976710655) | ((((long)src[43]) << 32) & 1099511627775) | ((((long)src[44]) << 24) & 4294967295) | ((((long)src[45]) << 16) & 16777215) | ((((long)src[46]) << 8) & 65535) | ((((long)src[47])) & 255);            dest[6] = ((((long)src[48]) << 56) & -1) | ((((long)src[49]) << 48) & 72057594037927935) | ((((long)src[50]) << 40) & 281474976710655) | ((((long)src[51]) << 32) & 1099511627775) | ((((long)src[52]) << 24) & 4294967295) | ((((long)src[53]) << 16) & 16777215) | ((((long)src[54]) << 8) & 65535) | ((((long)src[55])) & 255);            dest[7] = ((((long)src[56]) << 56) & -1) | ((((long)src[57]) << 48) & 72057594037927935) | ((((long)src[58]) << 40) & 281474976710655) | ((((long)src[59]) << 32) & 1099511627775) | ((((long)src[60]) << 24) & 4294967295) | ((((long)src[61]) << 16) & 16777215) | ((((long)src[62]) << 8) & 65535) | ((((long)src[63])) & 255);        }

        private static void Pack8LongValuesBE64(Span<long> src, Span<byte> dest) {
                    dest[0] = 
                (byte)((((src[0] & -1) >> 56)) & 255);
                            dest[1] = 
                (byte)((((src[0] & -1) >> 48)) & 255);
                            dest[2] = 
                (byte)((((src[0] & -1) >> 40)) & 255);
                            dest[3] = 
                (byte)((((src[0] & -1) >> 32)) & 255);
                            dest[4] = 
                (byte)((((src[0] & -1) >> 24)) & 255);
                            dest[5] = 
                (byte)((((src[0] & -1) >> 16)) & 255);
                            dest[6] = 
                (byte)((((src[0] & -1) >> 8)) & 255);
                            dest[7] = 
                (byte)((((src[0] & -1))) & 255);
                            dest[8] = 
                (byte)((((src[1] & -1) >> 56)) & 255);
                            dest[9] = 
                (byte)((((src[1] & -1) >> 48)) & 255);
                            dest[10] = 
                (byte)((((src[1] & -1) >> 40)) & 255);
                            dest[11] = 
                (byte)((((src[1] & -1) >> 32)) & 255);
                            dest[12] = 
                (byte)((((src[1] & -1) >> 24)) & 255);
                            dest[13] = 
                (byte)((((src[1] & -1) >> 16)) & 255);
                            dest[14] = 
                (byte)((((src[1] & -1) >> 8)) & 255);
                            dest[15] = 
                (byte)((((src[1] & -1))) & 255);
                            dest[16] = 
                (byte)((((src[2] & -1) >> 56)) & 255);
                            dest[17] = 
                (byte)((((src[2] & -1) >> 48)) & 255);
                            dest[18] = 
                (byte)((((src[2] & -1) >> 40)) & 255);
                            dest[19] = 
                (byte)((((src[2] & -1) >> 32)) & 255);
                            dest[20] = 
                (byte)((((src[2] & -1) >> 24)) & 255);
                            dest[21] = 
                (byte)((((src[2] & -1) >> 16)) & 255);
                            dest[22] = 
                (byte)((((src[2] & -1) >> 8)) & 255);
                            dest[23] = 
                (byte)((((src[2] & -1))) & 255);
                            dest[24] = 
                (byte)((((src[3] & -1) >> 56)) & 255);
                            dest[25] = 
                (byte)((((src[3] & -1) >> 48)) & 255);
                            dest[26] = 
                (byte)((((src[3] & -1) >> 40)) & 255);
                            dest[27] = 
                (byte)((((src[3] & -1) >> 32)) & 255);
                            dest[28] = 
                (byte)((((src[3] & -1) >> 24)) & 255);
                            dest[29] = 
                (byte)((((src[3] & -1) >> 16)) & 255);
                            dest[30] = 
                (byte)((((src[3] & -1) >> 8)) & 255);
                            dest[31] = 
                (byte)((((src[3] & -1))) & 255);
                            dest[32] = 
                (byte)((((src[4] & -1) >> 56)) & 255);
                            dest[33] = 
                (byte)((((src[4] & -1) >> 48)) & 255);
                            dest[34] = 
                (byte)((((src[4] & -1) >> 40)) & 255);
                            dest[35] = 
                (byte)((((src[4] & -1) >> 32)) & 255);
                            dest[36] = 
                (byte)((((src[4] & -1) >> 24)) & 255);
                            dest[37] = 
                (byte)((((src[4] & -1) >> 16)) & 255);
                            dest[38] = 
                (byte)((((src[4] & -1) >> 8)) & 255);
                            dest[39] = 
                (byte)((((src[4] & -1))) & 255);
                            dest[40] = 
                (byte)((((src[5] & -1) >> 56)) & 255);
                            dest[41] = 
                (byte)((((src[5] & -1) >> 48)) & 255);
                            dest[42] = 
                (byte)((((src[5] & -1) >> 40)) & 255);
                            dest[43] = 
                (byte)((((src[5] & -1) >> 32)) & 255);
                            dest[44] = 
                (byte)((((src[5] & -1) >> 24)) & 255);
                            dest[45] = 
                (byte)((((src[5] & -1) >> 16)) & 255);
                            dest[46] = 
                (byte)((((src[5] & -1) >> 8)) & 255);
                            dest[47] = 
                (byte)((((src[5] & -1))) & 255);
                            dest[48] = 
                (byte)((((src[6] & -1) >> 56)) & 255);
                            dest[49] = 
                (byte)((((src[6] & -1) >> 48)) & 255);
                            dest[50] = 
                (byte)((((src[6] & -1) >> 40)) & 255);
                            dest[51] = 
                (byte)((((src[6] & -1) >> 32)) & 255);
                            dest[52] = 
                (byte)((((src[6] & -1) >> 24)) & 255);
                            dest[53] = 
                (byte)((((src[6] & -1) >> 16)) & 255);
                            dest[54] = 
                (byte)((((src[6] & -1) >> 8)) & 255);
                            dest[55] = 
                (byte)((((src[6] & -1))) & 255);
                            dest[56] = 
                (byte)((((src[7] & -1) >> 56)) & 255);
                            dest[57] = 
                (byte)((((src[7] & -1) >> 48)) & 255);
                            dest[58] = 
                (byte)((((src[7] & -1) >> 40)) & 255);
                            dest[59] = 
                (byte)((((src[7] & -1) >> 32)) & 255);
                            dest[60] = 
                (byte)((((src[7] & -1) >> 24)) & 255);
                            dest[61] = 
                (byte)((((src[7] & -1) >> 16)) & 255);
                            dest[62] = 
                (byte)((((src[7] & -1) >> 8)) & 255);
                            dest[63] = 
                (byte)((((src[7] & -1))) & 255);
                        }

        #endregion

    } // class
} // ns