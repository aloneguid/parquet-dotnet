using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Analysis;
using Parquet.Data;
using Parquet.Data.Analysis;
using Parquet.Schema;

namespace Parquet {
    /// <summary>
    /// Defines extension methods to simplify Parquet usage
    /// </summary>
    public static class AnalysisExtensions {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="inputStream"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<DataFrame> ReadParquetAsDataFrameAsync(
            this Stream inputStream, CancellationToken cancellationToken = default) {
            using ParquetReader reader = await ParquetReader.CreateAsync(inputStream, cancellationToken: cancellationToken);

            var dfcs = new List<DataFrameColumn>();
            //var readableFields = reader.Schema.DataFields.Where(df => df.MaxRepetitionLevel == 0).ToList();
            List<DataField> readableFields = reader.Schema.Fields
                .Select(df => df as DataField)
                .Where(df => df != null)
                .Cast<DataField>()
                .ToList();
            var columns = new List<DataFrameColumn>();

            for(int rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++) {
                using ParquetRowGroupReader rgr = reader.OpenRowGroupReader(rowGroupIndex);

                for(int dataFieldIndex = 0; dataFieldIndex < readableFields.Count; dataFieldIndex++) {
                    DataColumn dc = await rgr.ReadColumnAsync(readableFields[dataFieldIndex], cancellationToken);

                    if(rowGroupIndex == 0) {
                        dfcs.Add(DataFrameMapper.ToDataFrameColumn(dc));
                    } else {
                        DataFrameMapper.AppendValues(dfcs[dataFieldIndex], dc);
                    }
                }
            }

            return new DataFrame(dfcs);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="df"></param>
        /// <param name="outputStream"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task WriteAsync(this DataFrame df, Stream outputStream, CancellationToken cancellationToken = default) {
            // create schema
            var schema = new ParquetSchema(
                df.Columns.Select(col => new DataField(col.Name, col.DataType.GetNullable())));

            using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, outputStream, cancellationToken: cancellationToken);
            using ParquetRowGroupWriter rgw = writer.CreateRowGroup();

            int i = 0;
            foreach(DataFrameColumn? col in df.Columns) {
                if(col == null)
                    throw new InvalidOperationException("unexpected null column");

                Array data = DataFrameMapper.GetTypedDataFast(col);
                var parquetColumn = new DataColumn(schema.DataFields[i], data);

                await rgw.WriteColumnAsync(parquetColumn, cancellationToken);

                i += 1;
            }
        }

        static Type GetNullable(this Type t) {
            TypeInfo ti = t.GetTypeInfo();

            if(ti.IsClass) {
                return t;
            }

            Type nt = typeof(Nullable<>);
            return nt.MakeGenericType(t);
        }
    }
}
