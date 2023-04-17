using System;
using Microsoft.Data.Analysis;

namespace Parquet.Data.Analysis {
    static class DataFrameMapper {
        public static DataFrameColumn ToDataFrameColumn(DataColumn dc) {
            
            if(dc.Field.ClrType == typeof(bool)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<bool>(dc.Field.Name, (bool[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<bool>(dc.Field.Name, (bool?[])dc.Data);
                }
            }
            
            if(dc.Field.ClrType == typeof(int)) {
                if(dc.Field.ClrType == dc.Field.ClrNullableIfHasNullsType) {
                    return new PrimitiveDataFrameColumn<int>(dc.Field.Name, (int[])dc.Data);
                } else {
                    return new PrimitiveDataFrameColumn<int>(dc.Field.Name, (int?[])dc.Data);
                }
            }
            
            // special case
            if(dc.Field.ClrType == typeof(string)) {
                return new StringDataFrameColumn(dc.Field.Name, (string[])dc.Data);
            }

            throw new NotSupportedException(dc.Field.ClrType.Name);
        }
    }
}