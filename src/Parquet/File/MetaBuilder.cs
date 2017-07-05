using Parquet.Data;
using Parquet.Thrift;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DSchema = Parquet.Data.Schema;
using TSchemaElement = Parquet.Thrift.SchemaElement;

namespace Parquet.File
{
   class MetaBuilder
   {
      private readonly FileMetaData _meta;

      public MetaBuilder()
      {
         _meta = new FileMetaData();

         _meta.Created_by = "parquet-dotnet";
         _meta.Version = 1;
         _meta.Row_groups = new List<RowGroup>();
      }

      public FileMetaData ThriftMeta => _meta;

      public void AddSchema(DataSet ds)
      {
         _meta.Schema = new List<TSchemaElement> { new TSchemaElement("schema") { Num_children = ds.Schema.Elements.Count } };
         _meta.Schema.AddRange(ds.Schema.Elements.Select(c => c.Thrift));
         _meta.Num_rows = ds.Count;
      }

      public RowGroup AddRowGroup()
      {
         var rg = new RowGroup();
         _meta.Row_groups.Add(rg);
         return rg;
      }
   }
}
