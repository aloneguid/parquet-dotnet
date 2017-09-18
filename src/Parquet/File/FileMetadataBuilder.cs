using NetBox.Model;
using Parquet.Data;
using Parquet.File.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using TSchemaElement = Parquet.Thrift.SchemaElement;

namespace Parquet.File
{
   /// <summary>
   /// Responsible for building Thrift file metadata when writing files
   /// </summary>
   class FileMetadataBuilder
   {
      private Thrift.FileMetaData _meta;
      private readonly WriterOptions _writerOptions;

      static FileMetadataBuilder()
      {
         //get file version
         Version fileVersion = typeof(FileMetadataBuilder).FileVersion();
         CreatedBy = $"parquet-dotnet version {fileVersion} (build {fileVersion.ToString().GetHash(HashType.Sha1)})";
      }

      public static string CreatedBy { internal get; set; }

      public FileMetadataBuilder(WriterOptions writerOptions)
      {
         _meta = new Thrift.FileMetaData();
         this._writerOptions = writerOptions;

         _meta.Created_by = CreatedBy;
         _meta.Version = 1;
         _meta.Row_groups = new List<Thrift.RowGroup>();
      }

      public Thrift.FileMetaData ThriftMeta => _meta;

      public void AddSchema(DataSet ds)
      {
         ds.Metadata.CreatedBy = CreatedBy;
         _meta.Schema = new List<TSchemaElement> { new TSchemaElement("schema") { Num_children = ds.Schema.Elements.Count } };

         foreach(SchemaElement se in ds.Schema.Elements)
         {
            AddSchema(_meta.Schema, se);
         }
         
         _meta.Num_rows = ds.Count;
      }

      private static void AddSchema(List<TSchemaElement> container, SchemaElement se)
      {
         if (se.IsRepeated)
         {
            var root = new TSchemaElement(se.Name)
            {
               Converted_type = Thrift.ConvertedType.LIST,
               Repetition_type = Thrift.FieldRepetitionType.OPTIONAL,
               Num_children = 1
            };

            var list = new TSchemaElement("list")
            {
               Repetition_type = Thrift.FieldRepetitionType.REPEATED,
               Num_children = 1
            };

            TSchemaElement element = se.Thrift;
            element.Name = "element";
            element.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;

            container.Add(root);
            container.Add(list);
            container.Add(element);
         }
         else
         {
            container.Add(se.Thrift);
         }
      }

      public static string BuildRepeatablePath(SchemaElement se)
      {
         return $"{se.Name}{Schema.PathSeparator}list{Schema.PathSeparator}element";
      }

      public void SetMeta(Thrift.FileMetaData meta)
      {
         _meta = meta;
      }

      public Thrift.RowGroup AddRowGroup()
      {
         var rg = new Thrift.RowGroup();
         _meta.Row_groups.Add(rg);
         return rg;
      }

      public Thrift.ColumnChunk AddColumnChunk(CompressionMethod compression, Stream output, SchemaElement schema, int valuesCount)
      {
         Thrift.CompressionCodec codec = DataFactory.GetThriftCompression(compression);

         var chunk = new Thrift.ColumnChunk();
         long startPos = output.Position;
         chunk.File_offset = startPos;
         chunk.Meta_data = new Thrift.ColumnMetaData();
         chunk.Meta_data.Num_values = valuesCount;
         chunk.Meta_data.Type = schema.Thrift.Type;
         chunk.Meta_data.Codec = codec;
         chunk.Meta_data.Data_page_offset = startPos;
         chunk.Meta_data.Encodings = new List<Thrift.Encoding>
         {
            Thrift.Encoding.RLE,
            Thrift.Encoding.BIT_PACKED,
            Thrift.Encoding.PLAIN
         };
         chunk.Meta_data.Path_in_schema = new List<string>(schema.Path.Split(Schema.PathSeparatorChar));

         return chunk;
      }

      public Thrift.PageHeader CreateDataPage(int valueCount)
      {
         var ph = new Thrift.PageHeader(Thrift.PageType.DATA_PAGE, 0, 0);
         ph.Data_page_header = new Thrift.DataPageHeader
         {
            Encoding = Thrift.Encoding.PLAIN,
            Definition_level_encoding = Thrift.Encoding.RLE,
            Repetition_level_encoding = Thrift.Encoding.BIT_PACKED,
            Num_values = valueCount
         };

         return ph;
      }

      public Thrift.PageHeader CreateDictionaryPage(int valueCount)
      {
         var ph = new Thrift.PageHeader(Thrift.PageType.DICTIONARY_PAGE, 0, 0);
         ph.Dictionary_page_header = new Thrift.DictionaryPageHeader
         {
            Encoding = Thrift.Encoding.PLAIN,
            Is_sorted = false,
            Num_values = valueCount
         };
         return ph;
      }
   }
}
