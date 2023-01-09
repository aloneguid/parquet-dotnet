﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.File.Values.Primitives;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class DecimalDataTypeHandler : BasicPrimitiveDataTypeHandler<decimal> {
        public DecimalDataTypeHandler() : base(DataType.Decimal, Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.DECIMAL) {
        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return

               tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.DECIMAL &&

               (
                  tse.Type == Thrift.Type.FIXED_LEN_BYTE_ARRAY ||
                  tse.Type == Thrift.Type.INT32 ||
                  tse.Type == Thrift.Type.INT64
               );
        }

        public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            base.CreateThrift(se, parent, container);

            //modify this element slightly
            Thrift.SchemaElement tse = container.Last();

            if(se is DecimalDataField dse) {
                if(dse.ForceByteArrayEncoding) {
                    tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
                }
                else {
                    if(dse.Precision <= 9)
                        tse.Type = Thrift.Type.INT32;
                    else if(dse.Precision <= 18)
                        tse.Type = Thrift.Type.INT64;
                    else
                        tse.Type = Thrift.Type.FIXED_LEN_BYTE_ARRAY;
                }

                tse.Precision = dse.Precision;
                tse.Scale = dse.Scale;
                tse.Type_length = BigDecimal.GetBufferSize(dse.Precision);
            }
            else {
                //set defaults
                tse.Precision = DecimalFormatDefaults.DefaultPrecision;
                tse.Scale = DecimalFormatDefaults.DefaultScale;
                tse.Type_length = 16;
            }
        }

        public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset) {
            decimal[] ddest = (decimal[])dest;

            switch(tse.Type) {
                case Thrift.Type.INT32:
                    return ReadAsInt32(tse, reader, ddest, offset);
                case Thrift.Type.INT64:
                    return ReadAsInt64(tse, reader, ddest, offset);
                case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
                    return ReadAsFixedLengthByteArray(tse, reader, ddest, offset);
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
            }

        }

        protected override decimal ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    decimal iscaleFactor = (decimal)Math.Pow(10, -tse.Scale);
                    int iv = reader.ReadInt32();
                    decimal idv = iv * iscaleFactor;
                    return idv;
                case Thrift.Type.INT64:
                    decimal lscaleFactor = (decimal)Math.Pow(10, -tse.Scale);
                    long lv = reader.ReadInt64();
                    decimal ldv = lv * lscaleFactor;
                    return ldv;
                case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
                    byte[] itemData = reader.ReadBytes(tse.Type_length);
                    return new BigDecimal(itemData, tse);
                default:
                    throw new InvalidDataException($"data type '{tse.Type}' does not represent a decimal");
            }
        }

        private void ReadAsInt32(Thrift.SchemaElement tse, BinaryReader reader, IList result) {
            decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
            while(reader.BaseStream.Position + 4 <= reader.BaseStream.Length) {
                int iv = reader.ReadInt32();
                decimal dv = iv * scaleFactor;
                result.Add(dv);
            }
        }

        private int ReadAsInt32(Thrift.SchemaElement tse, BinaryReader reader, decimal[] dest, int offset) {
            int start = offset;
            decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
            while(reader.BaseStream.Position + 4 <= reader.BaseStream.Length) {
                int iv = reader.ReadInt32();
                decimal dv = iv * scaleFactor;
                dest[offset++] = dv;
            }
            return offset - start;
        }

        private void ReadAsInt64(Thrift.SchemaElement tse, BinaryReader reader, IList result) {
            decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
            while(reader.BaseStream.Position + 8 <= reader.BaseStream.Length) {
                long lv = reader.ReadInt64();
                decimal dv = lv * scaleFactor;
                result.Add(dv);
            }
        }

        private int ReadAsInt64(Thrift.SchemaElement tse, BinaryReader reader, decimal[] dest, int offset) {
            int start = offset;
            decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
            while(reader.BaseStream.Position + 8 <= reader.BaseStream.Length) {
                long lv = reader.ReadInt64();
                decimal dv = lv * scaleFactor;
                dest[offset++] = dv;
            }
            return offset - start;
        }

        private void ReadAsFixedLengthByteArray(Thrift.SchemaElement tse, BinaryReader reader, IList result) {
            int typeLength = tse.Type_length;

            //can't read if there is no type length set
            if(typeLength == 0)
                return;

            while(reader.BaseStream.Position + typeLength <= reader.BaseStream.Length) {
                byte[] itemData = reader.ReadBytes(typeLength);
                decimal dc = new BigDecimal(itemData, tse);
                result.Add(dc);
            }
        }

        private int ReadAsFixedLengthByteArray(Thrift.SchemaElement tse, BinaryReader reader, decimal[] dest, int offset) {
            int start = offset;
            int typeLength = tse.Type_length;

            //can't read if there is no type length set
            if(typeLength == 0)
                return 0;

            while(reader.BaseStream.Position + typeLength <= reader.BaseStream.Length) {
                byte[] itemData = reader.ReadBytes(typeLength);
                decimal dc = new BigDecimal(itemData, tse);
                dest[offset++] = dc;
            }

            return offset - start;
        }

    }
}