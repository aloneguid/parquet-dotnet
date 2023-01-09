using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class TimeSpanDataTypeHandler : BasicPrimitiveDataTypeHandler<TimeSpan> {
        public TimeSpanDataTypeHandler() : base(DataType.TimeSpan, Thrift.Type.INT64, Thrift.ConvertedType.TIME_MICROS) {

        }

        public override bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return

               (tse.Type == Thrift.Type.INT64 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIME_MICROS) ||

               (tse.Type == Thrift.Type.INT32 && tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.TIME_MILLIS);
        }

        public override void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container) {
            base.CreateThrift(se, parent, container);

            //modify annotations
            Thrift.SchemaElement tse = container.Last();
            if(se is TimeSpanDataField dse) {
                switch(dse.TimeSpanFormat) {
                    case TimeSpanFormat.MicroSeconds:
                        tse.Type = Thrift.Type.INT64;
                        tse.Converted_type = Thrift.ConvertedType.TIME_MICROS;
                        break;
                    case TimeSpanFormat.MilliSeconds:
                        tse.Type = Thrift.Type.INT32;
                        tse.Converted_type = Thrift.ConvertedType.TIME_MILLIS;
                        break;

                        //other cases are just default
                }
            }
            else {
                //default annotation is fine
            }

        }

        public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset) {
            switch(tse.Type) {
                case Thrift.Type.INT32:
                    return ReadAsInt32(reader, (TimeSpan[])dest, offset);
                case Thrift.Type.INT64:
                    return ReadAsInt64(reader, (TimeSpan[])dest, offset);
                default:
                    throw new NotSupportedException();
            }
        }

        protected override TimeSpan ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length) {
            if(tse == null)
                return default;

            switch(tse.Type) {
                case Thrift.Type.INT32:
                    int iv = reader.ReadInt32();
                    return iv.FromUnixDays().TimeOfDay;
                case Thrift.Type.INT64:
                    long lv = reader.ReadInt64();
                    return lv.FromUnixMilliseconds().TimeOfDay;
                default:
                    throw new NotSupportedException();
            }
        }

        private static int ReadAsInt32(BinaryReader reader, TimeSpan[] dest, int offset) {
            int idx = offset;
            while(reader.BaseStream.Position + 4 <= reader.BaseStream.Length) {
                dest[idx++] = ReadAsInt32(reader);
            }

            return idx - offset;
        }

        private static TimeSpan ReadAsInt32(BinaryReader reader) {
            int iv = reader.ReadInt32();
            //TimeSpan e = iv.FromUnixDays();
            TimeSpan e = new TimeSpan(0, 0, 0, 0, iv);
            return e;
        }

        private static void ReadAsInt64(BinaryReader reader, IList result) {
            while(reader.BaseStream.Position + 8 <= reader.BaseStream.Length) {
                result.Add(ReadAsInt64(reader));
            }
        }

        private static TimeSpan ReadAsInt64(BinaryReader reader) {
            long lv = reader.ReadInt64();
            return new TimeSpan(lv * 10);
        }

        private static int ReadAsInt64(BinaryReader reader, TimeSpan[] dest, int offset) {
            int idx = offset;

            while(reader.BaseStream.Position + 8 <= reader.BaseStream.Length) {
                TimeSpan dto = ReadAsInt64(reader);
                dest[idx++] = dto;
            }

            return idx - offset;
        }

        public override object PlainDecode(Thrift.SchemaElement tse, byte[] encoded) {
            if(encoded == null)
                return null;

            using(var ms = new MemoryStream(encoded)) {
                using(var reader = new BinaryReader(ms)) {
                    switch(tse.Type) {
                        case Thrift.Type.INT32:
                            return ReadAsInt32(reader);
                        case Thrift.Type.INT64:
                            return ReadAsInt64(reader);
                        default:
                            throw new NotSupportedException();
                    }
                }
            }
        }
    }
}