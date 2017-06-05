using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Parquet
{
    public class ParquetReader : IDisposable
    {
        private readonly Stream _input;
        private readonly BinaryReader _reader;

        public ParquetReader(Stream input)
        {
            _input = input;
            _reader = new BinaryReader(input);

            ReadMetadata();
        }

        private void ReadMetadata()
        {
            _input.Seek(-8, SeekOrigin.End);
            int footerLength = _reader.ReadInt32();
            char[] magic = _reader.ReadChars(4);

            _input.Seek(-8 - footerLength, SeekOrigin.End);
            FileMetaData meta = _input.ThriftRead<FileMetaData>();

            /*foreach (PQ.SchemaElement se in meta.Schema)
            {
                _columnNameToSchema[se.Name] = se;
            }

            Version = new Version(meta.Version, 0, 0, 0);
            CreatedBy = meta.Created_by;
            _rowGroups.AddRange(ParquetRowGroup.FromParquet(meta.Row_groups));*/
        }

        public void Dispose()
        {
        }
    }
}
