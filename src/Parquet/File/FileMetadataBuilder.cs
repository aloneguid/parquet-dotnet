/*using Parquet.Data;
using Parquet.File.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Linq;
using System.Collections;

namespace Parquet.File
{
   /// <summary>
   /// Responsible for building Thrift file metadata when writing files
   /// todo: to move under ThriftFooter
   /// </summary>
   class FileMetadataBuilder
   {
      private Thrift.FileMetaData _meta;

      static FileMetadataBuilder()
      {
         //get file version
         Version fileVersion = FileVersion(typeof(FileMetadataBuilder));
         CreatedBy = $"Parquet.Net version {fileVersion}";
      }

      private static Version FileVersion(Type t)
      {
         CustomAttributeData fva = GetAssembly(t).CustomAttributes.First(a => a.AttributeType == typeof(AssemblyFileVersionAttribute));
         CustomAttributeTypedArgument varg = fva.ConstructorArguments[0];
         string fileVersion = (string)varg.Value;
         return new Version(fileVersion);
      }

      private static Assembly GetAssembly(Type t)
      {
         return t.GetTypeInfo().Assembly;
      }

      public static string CreatedBy { internal get; set; }

      public FileMetadataBuilder()
      {
         _meta = new Thrift.FileMetaData();

         _meta.Created_by = CreatedBy;
         _meta.Version = 1;
         _meta.Row_groups = new List<Thrift.RowGroup>();
      }

      public Thrift.FileMetaData ThriftMeta => _meta;

      public void AddSchema(DataSet ds)
      {
         ds.Metadata.CreatedBy = CreatedBy;

         _meta.Schema = new List<Thrift.SchemaElement> { new Thrift.SchemaElement("schema") { Num_children = ds.Schema.Elements.Count } };
         _meta.Key_value_metadata = ds.Metadata.Custom.Select(kv => new Thrift.KeyValue(kv.Key) { Value = kv.Value }).ToList();

         foreach(SchemaElement se in ds.Schema.Elements)
         {
            AddSchema(_meta.Schema, se);
         }
         
         _meta.Num_rows = ds.Count;
         
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

      public Thrift.SchemaElement CreateSimpleSchemaElement(string name, bool nullable)
      {
         var th = new Thrift.SchemaElement(name);
         th.Repetition_type = nullable ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED;
         return th;
      }


      private static bool TryGetNonNullableType(Type t, out Type nonNullable)
      {
         TypeInfo ti = t.GetTypeInfo();

         if (!ti.IsClass && ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(Nullable<>))
         {
            nonNullable = ti.GenericTypeArguments[0];
            return true;
         }

         nonNullable = t;
         return false;
      }

      private void CreateDictionary(Thrift.SchemaElement schema, Type keyType, Type valueType)
      {
         schema.Converted_type = Thrift.ConvertedType.MAP;

      }

      public List<Thrift.SchemaElement> BuildSchemaExperimental(Schema schema)
      {
         var container = new List<Thrift.SchemaElement>();

         var root = new Thrift.SchemaElement("schema") { Num_children = schema.Elements.Count };
         container.Add(root);

         BuildSchemaExperimental(container, schema.Elements);

         return container;
      }

      private void BuildSchemaExperimental(List<Thrift.SchemaElement> container, IList<SchemaElement> elements)
      {
         foreach(SchemaElement element in elements)
         {
            IDataTypeHandler handler = DataTypeFactory.Match(element.DataType);

            handler.CreateThrift(element, container);
         }
      }
   }
}*/