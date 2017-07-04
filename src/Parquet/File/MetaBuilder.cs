using Parquet.Thrift;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

      public void AddSchema(ParquetDataSet dataSet)
      {
         long totalCount = dataSet.Count;

         _meta.Schema = new List<SchemaElement> { new SchemaElement("schema") { Num_children = dataSet.Columns.Count } };
         _meta.Schema.AddRange(dataSet.Columns.Select(c => c.Schema));
         _meta.Num_rows = totalCount;
      }

      public RowGroup AddRowGroup()
      {
         var rg = new RowGroup();
         _meta.Row_groups.Add(rg);
         return rg;
      }
   }
}
