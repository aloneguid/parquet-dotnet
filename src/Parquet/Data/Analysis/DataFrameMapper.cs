

using System;
using Microsoft.Data.Analysis;

namespace Parquet.Data.Analysis {
    static class DataFrameMapper {
        public static DataFrameColumn ToDataFrameColumn(DataColumn dc) {
            
            if(dc.Field.ClrType == typeof(System.Boolean)) {
                var dfc = new PrimitiveDataFrameColumn<System.Boolean>(dc.Field.Name);
                foreach(System.Boolean el in (System.Boolean[])dc.Data) {
                    dfc.Append(el);
                }
                return dfc;
            }
            
            if(dc.Field.ClrType == typeof(System.Int32)) {
                var dfc = new PrimitiveDataFrameColumn<System.Int32>(dc.Field.Name);
                foreach(System.Int32 el in (System.Int32[])dc.Data) {
                    dfc.Append(el);
                }
                return dfc;
            }
            

            throw new NotSupportedException(dc.Field.ClrType.Name);
        }
    }
}