using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Parquet.Schema;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test.Integration;

public class DuckDbEncryptionIntegrationTest {
    private const string KeyName = "footer-key";
    private const string FooterKeyText = "footerKey-16byte";
    private static readonly byte[] FooterKey = Encoding.UTF8.GetBytes(FooterKeyText);

    [Fact]
    public async Task DuckDbReadsParquetDotNetEncryptedOutput() {
        string path = await WriteParquetDotNetFileAsync();

        try {
            using var connection = OpenDuckDb();
            AddFooterKey(connection);
            using DuckDBCommand command = connection.CreateCommand();
            command.CommandText = $$"""
                SELECT id, name
                FROM read_parquet(
                    '{{EscapeSqlLiteral(path)}}',
                    encryption_config = {footer_key: '{{KeyName}}'})
                ORDER BY id
                """;
            using DuckDBDataReader reader = command.ExecuteReader();
            var rows = new List<(int Id, string Name)>();
            while(reader.Read())
                rows.Add((reader.GetInt32(0), reader.GetString(1)));

            Assert.Equal(
                new[] { (1, "alice"), (2, "bob"), (3, "carol"), (4, "dave") },
                rows);
        } finally {
            F.Delete(path);
        }
    }

    private static async Task<string> WriteParquetDotNetFileAsync() {
        string path = CreateTempPath();
        var id = new DataField<int>("id");
        var name = new DataField<string>("name");
        var options = new ParquetOptions {
            CompressionMethod = CompressionMethod.None,
            Encryption = new ParquetEncryptionOptions(new ParquetKey(FooterKey))
        };

        await using FileStream stream = F.Create(path);
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
                        new ParquetSchema(id, name), stream, options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(id, new[] { 1, 2, 3, 4 });
            await rowGroup.WriteAsync(
                name,
                new string?[] { "alice", "bob", "carol", "dave" });
        }

        return path;
    }

    private static DuckDBConnection OpenDuckDb() {
        var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        return connection;
    }

    private static void AddFooterKey(DuckDBConnection connection) {
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = $"PRAGMA add_parquet_key('{KeyName}', '{FooterKeyText}')";
        command.ExecuteNonQuery();
    }

    private static string CreateTempPath() =>
        Path.Combine(Path.GetTempPath(), $"parquet-dotnet-duckdb-{Guid.NewGuid():N}.parquet");

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
