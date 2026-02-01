using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Parquet.Data;
using Parquet.File.Values.Primitives;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Integration {


    /// <summary>
    /// Initial prototype for DuckDB integration. Needs a lot of work.
    /// </summary>
    public class DuckDbIntegrationTest : IntegrationBase {

        private static readonly IReadOnlyDictionary<Type, string> _duckDbTypeMap = new Dictionary<Type, string> {
            [typeof(string)] = "VARCHAR",
            [typeof(byte[])] = "BLOB",
            [typeof(bool)] = "BOOLEAN",
            [typeof(Guid)] = "UUID",
            [typeof(double)] = "DOUBLE",
            [typeof(float)] = "REAL",
            [typeof(long)] = "BIGINT",
            [typeof(ulong)] = "UBIGINT",
            [typeof(int)] = "INTEGER",
            [typeof(uint)] = "UINTEGER",
            [typeof(short)] = "SMALLINT",
            [typeof(ushort)] = "USMALLINT",
            [typeof(sbyte)] = "TINYINT",
            [typeof(byte)] = "UTINYINT",
            [typeof(DateTime)] = "TIMESTAMP",
            [typeof(decimal)] = "DECIMAL(29, 12)",
            // Time-like CLR types
            [typeof(TimeSpan)] = "TIME",
            [typeof(Interval)] = "INTERVAL",
#if !NETCOREAPP3_1
            [typeof(DateOnly)] = "DATE",
#endif
#if NET6_0_OR_GREATER
            [typeof(TimeOnly)] = "TIME"
#endif
        };

        // https://duckdb.org/docs/stable/sql/data_types/overview
        private static string GetDuckDbSqlType(DataField field) {
            // Decimal data field with specific precision/scale
            if(field is DecimalDataField ddf) {
                return $"DECIMAL({ddf.Precision}, {ddf.Scale})";
            }

            // Date/Time logical fields with specific format
            if(field is DateTimeDataField dtf) {
                return dtf.DateTimeFormat switch {
                    DateTimeFormat.Date => "DATE",
                    _ => "TIMESTAMP"
                };
            }

            if(field is TimeSpanDataField || field is TimeOnlyDataField) {
                return "TIME";
            }

            // Fallback to CLR type mapping
            if(_duckDbTypeMap.TryGetValue(field.ClrType, out string? mapped)) {
                return mapped;
            }

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
            
            //

            return value;
        }

        [Fact]
        public async Task DuckDbWorks() {
            if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
               RuntimeInformation.OSArchitecture == Architecture.X86) {
                Assert.Skip("DuckDb is not supported on Windows x86");
            }

            await using var conn = new DuckDBConnection("DataSource=:memory:");
            await conn.OpenAsync(TestContext.Current.CancellationToken);
        }
        
        [Theory, TypeTestData(DuckDb = true)]
        public async Task DuckDbGeneratedFileReads(TTI input) {
            if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
               RuntimeInformation.OSArchitecture == Architecture.X86) {
                Assert.Skip("DuckDb is not supported on Windows x86");
            }

            string tmp = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".parquet");

            try {
                using var conn = new DuckDBConnection("DataSource=:memory:");
                await conn.OpenAsync(TestContext.Current.CancellationToken);

                string sqlType = GetDuckDbSqlType(input.Field);

                // create table
                using(DuckDBCommand cmd = conn.CreateCommand()) {
                    cmd.CommandText = $"CREATE TABLE t (id INTEGER, val {sqlType});";
                    await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
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
                    await insertCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
                }

                // copy to parquet
                using(DuckDBCommand copyCmd = conn.CreateCommand()) {
                    copyCmd.CommandText = $"COPY t TO '{tmp.Replace("'", "''")}' (FORMAT 'parquet');";
                    await copyCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
                }

                // read with ParquetReader
                using FileStream fs = System.IO.File.OpenRead(tmp);
                using ParquetReader reader = await ParquetReader.CreateAsync(fs, cancellationToken: TestContext.Current.CancellationToken);
                Assert.Equal(1, reader.RowGroupCount);
                DataField[] dfs = reader.Schema.GetDataFields();
                Assert.Equal(2, dfs.Length);

                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);
                DataColumn idCol = await rg.ReadColumnAsync(dfs[0], TestContext.Current.CancellationToken);
                Assert.Equal(1, Convert.ToInt32(idCol.Data.GetValue(0)));

                DataColumn valCol = await rg.ReadColumnAsync(dfs[1], TestContext.Current.CancellationToken);
                Assert.Single(valCol.Data);

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
