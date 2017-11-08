using Parquet.Data;
using Parquet.File.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using TSchemaElement = Parquet.Thrift.SchemaElement;
using System.Linq;
using System.Collections;

namespace Parquet.File
{
   /// <summary>
   /// Responsible for building Thrift file metadata when writing files
   /// </summary>
   class FileMetadataBuilder
   {
      private Thrift.FileMetaData _meta;

      static FileMetadataBuilder()
      {
         //get file version
         Version fileVersion = FileVersion(typeof(FileMetadataBuilder));
         CreatedBy = $"parquet-dotnet version {fileVersion}";
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
            if (se.IsMap)
            {
               AddMapSchema(container, se);
            }
            else
            {
               AddListSchema(container, se);
            }
         }
         else if(se.IsNestedStructure)
         {
            var structure = new TSchemaElement(se.Name)
            {
               Repetition_type = Thrift.FieldRepetitionType.OPTIONAL,
               Num_children = se.Children.Count
            };
            container.Add(structure);

            foreach(SchemaElement child in se.Children)
            {
               AddSchema(container, child);
            }
         }
         else
         {
            container.Add(se.Thrift);
         }
      }

      private static void AddMapSchema(List<TSchemaElement> container, SchemaElement se)
      {
         var root = new TSchemaElement(se.Name)
         {
            Converted_type = Thrift.ConvertedType.MAP,
            Num_children = 1
         };
         container.Add(root);

         var kv = new TSchemaElement("key_value")
         {
            Num_children = 2,
            Repetition_type = Thrift.FieldRepetitionType.REPEATED
         };
         container.Add(kv);

         foreach(SchemaElement kvse in se.Extra)
         {
            AddSchema(container, kvse);
         }
      }

      private static void AddListSchema(List<TSchemaElement> container, SchemaElement se)
      {
         var root = new TSchemaElement(se.Name)
         {
            Converted_type = Thrift.ConvertedType.LIST,
            Repetition_type = Thrift.FieldRepetitionType.OPTIONAL,
            Num_children = 1
         };
         container.Add(root);

         var list = new TSchemaElement("list")
         {
            Repetition_type = Thrift.FieldRepetitionType.REPEATED,
            Num_children = 1
         };
         container.Add(list);

         if (se.IsNestedStructure)
         {
            var element = new TSchemaElement("element");
            element.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
            element.Num_children = se.Children.Count;
            container.Add(element);

            foreach (SchemaElement child in se.Children)
            {
               AddSchema(container, child);
            }
         }
         else
         {
            TSchemaElement element = se.Thrift;
            element.Name = "element";
            element.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
            container.Add(element);
         }
      }

      public static string BuildListPath(string name)
      {
         return $"{name}{Schema.PathSeparator}list{Schema.PathSeparator}element";
      }

      public static string BuildDictionaryPath(string name)
      {
         return $"{name}{Schema.PathSeparator}key_value";
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

      public TSchemaElement CreateSimpleSchemaElement(string name, bool nullable)
      {
         var th = new TSchemaElement(name);
         th.Repetition_type = nullable ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED;
         return th;
      }

      public TSchemaElement CreateSchemaElement(string name, Type systemType,
         out Type elementType,
         out string pathName,
         out Type[] extras)
      {
         var th = new TSchemaElement(name);
         extras = null;

         if(TypeFactory.TryExtractDictionaryType(systemType, out Type keyType, out Type valueType))
         {
            elementType = typeof(IDictionary);
            pathName = BuildDictionaryPath(name);
            CreateDictionary(th, keyType, valueType);
            th.Repetition_type = Thrift.FieldRepetitionType.REPEATED;

            extras = new[] { keyType, valueType };
         }
         else if (TypeFactory.TryExtractEnumerableType(systemType, out Type baseType))
         {
            elementType = baseType;
            pathName = BuildListPath(name);
            th.Repetition_type = Thrift.FieldRepetitionType.REPEATED;
         }
         else if(typeof(Row) == systemType)
         {
            elementType = typeof(Row);
            pathName = null;
            th.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
         }
         else
         {
            if(TryGetNonNullableType(systemType, out Type nonNullableSystemType))
            {
               elementType = nonNullableSystemType;
               th.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
            }
            else if(systemType.GetTypeInfo().IsClass)
            {
               //reference types are always nullable
               elementType = systemType;
               th.Repetition_type = Thrift.FieldRepetitionType.OPTIONAL;
            }
            else
            {
               elementType = systemType;
               th.Repetition_type = Thrift.FieldRepetitionType.REQUIRED;
            }

            pathName = null;
         }

         //adjust schema

         if (typeof(Row) != elementType && typeof(IDictionary) != elementType)
         {
            TypePrimitive tp = TypePrimitive.Find(elementType);

            th.Type = tp.ThriftType;

            if (tp.ThriftAnnotation != null)
            {
               th.Converted_type = tp.ThriftAnnotation.Value;
            }

            //todo: not the best place for it, but it's a special case at the moment
            if (systemType == typeof(decimal))
            {
               th.Precision = 38;
               th.Scale = 18;
               th.Type_length = 16;   //set to default type length to be used when no elements are added
            }
         }

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

      private void CreateDictionary(TSchemaElement schema, Type keyType, Type valueType)
      {
         schema.Converted_type = Thrift.ConvertedType.MAP;

      }
   }
}
