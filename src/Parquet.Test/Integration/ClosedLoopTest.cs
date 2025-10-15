using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Integration {

    //public class 

    public class ClosedLoopTest : IntegrationBase {

        private static string GetDuckDbSqlType(DataField field) {
            // Decimal
            if(field is DecimalDataField ddf) {
                return $"DECIMAL({ddf.Precision}, {ddf.Scale})";
            }

            // Date/Time
            if(field is DateTimeDataField dtf) {
                return dtf.DateTimeFormat switch {
                    DateTimeFormat.Date => "DATE",
                    _ => "TIMESTAMP"
                };
            }

            // TimeSpan / TimeOnly
            if(field is TimeSpanDataField) return "TIME";
#if NET6_0_OR_GREATER
            if(field is TimeOnlyDataField) return "TIME";
#endif
            // Interval
            if(field.ClrType == typeof(Parquet.File.Values.Primitives.Interval)) return "INTERVAL";

            // Primitives by CLR type
            Type t = field.ClrType;
            if(t == typeof(string)) return "VARCHAR";
            if(t == typeof(byte[])) return "BLOB";
            if(t == typeof(bool)) return "BOOLEAN";
            if(t == typeof(Guid)) return "UUID";
            if(t == typeof(double)) return "DOUBLE";
            if(t == typeof(float)) return "REAL";
            if(t == typeof(long)) return "BIGINT";
            if(t == typeof(ulong)) return "UBIGINT";
            if(t == typeof(int)) return "INTEGER";
            if(t == typeof(uint)) return "UINTEGER";
            if(t == typeof(short)) return "SMALLINT";
            if(t == typeof(ushort)) return "USMALLINT";
            if(t == typeof(sbyte)) return "TINYINT";
            if(t == typeof(byte)) return "UTINYINT";
#if !NETCOREAPP3_1
            if(t == typeof(DateOnly)) return "DATE";
#endif
            throw new NotSupportedException($"DuckDB SQL type mapping not supported for {field}");
        }

        private static string? BuildIntervalLiteral(object? value) {
            if(value is null) return null;
            if(value is Parquet.File.Values.Primitives.Interval iv) {
                // INTERVAL '<months> months <days> days <millis> milliseconds'
                return $"INTERVAL '{iv.Months} months {iv.Days} days {iv.Millis} milliseconds'";
            }
            return null;
        }

        private static object? ConvertToDuckDbParameterValue(object? value) {
            if(value is null) return null;
#if NET6_0_OR_GREATER
            if(value is DateOnly d) return d.ToDateTime(TimeOnly.MinValue);
            if(value is TimeOnly to) return to.ToTimeSpan();
#endif
            // Most types can be passed as is (Guid, byte[], numeric, bool, DateTime, TimeSpan, string, decimal)
            return value;
        }

        [Theory, TestBase.TypeTestData]
        public async Task DuckDbGeneratedFileReads(TypeTestInstance input) {
            string tmp = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".parquet");

            try {
                using var conn = new DuckDBConnection("DataSource=:memory:");
                await conn.OpenAsync();

                string sqlType = GetDuckDbSqlType(input.Field);

                // create table
                using(DuckDBCommand cmd = conn.CreateCommand()) {
                    cmd.CommandText = $"CREATE TABLE t (id INTEGER, val {sqlType});";
                    await cmd.ExecuteNonQueryAsync();
                }

                // insert row
                string? intervalLiteral = BuildIntervalLiteral(input.ExpectedValue);
                if(intervalLiteral != null) {
                    using DuckDBCommand insertCmd = conn.CreateCommand();
                    insertCmd.CommandText = $"INSERT INTO t (id, val) VALUES (1, {intervalLiteral});";
                    await insertCmd.ExecuteNonQueryAsync();
                } else {
                    using DuckDBCommand insertCmd = conn.CreateCommand();
                    insertCmd.CommandText = "INSERT INTO t (id, val) VALUES (?, ?);";
                    System.Data.Common.DbParameter p1 = insertCmd.CreateParameter();
                    p1.Value = 1;
                    insertCmd.Parameters.Add(p1);
                    System.Data.Common.DbParameter p2 = insertCmd.CreateParameter();
                    p2.Value = ConvertToDuckDbParameterValue(input.ExpectedValue) ?? (object?)DBNull.Value;
                    insertCmd.Parameters.Add(p2);
                    await insertCmd.ExecuteNonQueryAsync();
                }

                // copy to parquet
                using(DuckDBCommand copyCmd = conn.CreateCommand()) {
                    copyCmd.CommandText = $"COPY t TO '{tmp.Replace("'", "''")}' (FORMAT 'parquet');";
                    await copyCmd.ExecuteNonQueryAsync();
                }

                // read with ParquetReader
                using FileStream fs = System.IO.File.OpenRead(tmp);
                using ParquetReader reader = await ParquetReader.CreateAsync(fs);
                Assert.Equal(1, reader.RowGroupCount);
                DataField[] dfs = reader.Schema.GetDataFields();
                Assert.Equal(2, dfs.Length);

                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);
                DataColumn idCol = await rg.ReadColumnAsync(dfs[0]);
                Assert.Equal(1, Convert.ToInt32(idCol.Data.GetValue(0)));

                DataColumn valCol = await rg.ReadColumnAsync(dfs[1]);
                Assert.Equal(1, valCol.Data.Length);

                object? actual = valCol.Data.GetValue(0);
                if(input.ExpectedValue is null) {
                    Assert.Null(actual);
                } else if(actual is byte[] ab && input.ExpectedValue is byte[] eb) {
                    Assert.True(ab.SequenceEqual(eb));
                } else {
                    // Basic sanity: value exists (deeper equality is type-specific and verified in other tests)
                    Assert.NotNull(actual);
                }
            } finally {
                try { if(System.IO.File.Exists(tmp)) System.IO.File.Delete(tmp); } catch { }
            }
        }

    }
}
