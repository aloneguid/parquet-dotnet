using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using System.Linq;
using Parquet.DataTypes;

namespace Parquet.File
{
   /// <summary>
   /// Responsible for parsing file metadata from Thrift structures
   /// </summary>
   class FileMetadataParser
   {
      private readonly Thrift.FileMetaData _fileMeta;

      public FileMetadataParser(Thrift.FileMetaData fileMeta)
      {
         _fileMeta = fileMeta ?? throw new ArgumentNullException(nameof(fileMeta));
      }

      public Schema ParseSchema(ParquetOptions formatOptions)
      {
         void Build(SchemaElement node, ref int i, int count, bool isRoot)
         {
            while (node.Children.Count < count)
            {
               Thrift.SchemaElement tse = _fileMeta.Schema[i];
               SchemaElement mse;

               if (tse.Converted_type == Thrift.ConvertedType.LIST)
               {
                  mse = BuildListSchema(ref tse, ref i, isRoot, node, formatOptions);
               }
               else if(tse.Converted_type == Thrift.ConvertedType.MAP || tse.Converted_type == Thrift.ConvertedType.MAP_KEY_VALUE)
               {
                  mse = BuildMapSchema(ref tse, ref i, isRoot, node, formatOptions);
               }
               else
               {
                  Type containerType = tse.Num_children > 0
                     ? typeof(Row)
                     : null;

                  SchemaElement parent = isRoot ? null : node;
                  mse = BuildSchemaElement(tse, parent, formatOptions, containerType);

                  AddFlags(mse, tse);
               }

               int mrl = mse.MaxRepetitionLevel;
               int mdl = mse.MaxDefinitionLevel;
               node.Children.Add(mse);
               mse.MaxRepetitionLevel = mrl;
               mse.MaxDefinitionLevel = mdl;

               i += 1;

               if (tse.Num_children > 0)
               {
                  Build(mse, ref i, tse.Num_children, false);
               }
            }
         }

         //extract schema tree
         var root = new SchemaElement<int>("root") { Path = string.Empty };
         root.AutoUpdateLevels = false;
         int start = 1;
         Build(root, ref start, _fileMeta.Schema[0].Num_children, true);

         foreach(SchemaElement se in root.Children)
         {
            se.Detach();
         }
         root.AutoUpdateLevels = true;

         return new Schema(root.Children);
      }

      public void AddMeta(DataSet ds)
      {
         ds.Metadata.Custom.Clear();

         if (_fileMeta.Key_value_metadata != null && _fileMeta.Key_value_metadata.Count > 0)
         {
            foreach(Thrift.KeyValue tkv in _fileMeta.Key_value_metadata)
            {
               ds.Metadata.Custom[tkv.Key] = tkv.Value;
            }
         }
      }

      private SchemaElement BuildListSchema(ref Thrift.SchemaElement tse, ref int i, bool isRoot, SchemaElement node, ParquetOptions formatOptions)
      {
         Thrift.SchemaElement tseTop = tse;
         Thrift.SchemaElement tseList = _fileMeta.Schema[++i];
         Thrift.SchemaElement tseElement = _fileMeta.Schema[++i];

         SchemaElement mse = BuildSchemaElement(tseElement,
            isRoot ? null : node,
            formatOptions,
            tseElement.Num_children == 0
               ? typeof(IEnumerable)   //augmented to generic IEnumerable in constructor
               : typeof(IEnumerable<Row>),
            tseTop.Name);
         mse.Path = string.Join(Schema.PathSeparator, tseTop.Name, tseList.Name, tseElement.Name);
         mse.IsRepeated = true;
         if (!isRoot) mse.Path = node.Path + Schema.PathSeparator + mse.Path;

         AddFlags(mse, tseTop, tseList, tseElement);

         tse = tseElement;

         return mse;
      }

      private SchemaElement BuildMapSchema(ref Thrift.SchemaElement tse, ref int i, bool isRoot, SchemaElement node, ParquetOptions formatOptions)
      {
         //tse is followed by map container (REPEATED) and another two elements - key and value

         Thrift.SchemaElement tseContainer = _fileMeta.Schema[++i];
         Thrift.SchemaElement tseKey = _fileMeta.Schema[++i];
         Thrift.SchemaElement tseValue = _fileMeta.Schema[++i];

         Type keyType = TypePrimitive.GetSystemTypeBySchema(tseKey, formatOptions);
         Type valueType = TypePrimitive.GetSystemTypeBySchema(tseValue, formatOptions);
         Type gt = typeof(Dictionary<,>);
         Type masterType = gt.MakeGenericType(keyType, valueType);

         //master schema
         var se = new SchemaElement(tseContainer, tse.Name, masterType, masterType,
            string.Join(Schema.PathSeparator, tse.Name, tseContainer.Name));
         if (!isRoot) se.Path = node.Parent + Schema.PathSeparator + se.Path;
         se.Parent = node;
         se.IsMap = true;
         AddFlags(se, tse, tseContainer);

         //extra schamas
         var kse = new SchemaElement(tseKey, null, keyType, keyType, null) { Parent = se };
         var vse = new SchemaElement(tseValue, null, valueType, valueType, null) { Parent = se };
         se.Extra.Add(kse);
         se.Extra.Add(vse);
         AddFlags(kse, tseKey);
         AddFlags(vse, tseValue);

         tse = tseValue;
         return se;
      }

      private SchemaElement BuildSchemaElement(Thrift.SchemaElement tse, SchemaElement parent, ParquetOptions formatOptions, Type elementType, string name = null)
      {
         Type columnType;

         if (elementType != null)
         {
            columnType = elementType;

            if (elementType == typeof(IEnumerable))
            {
               Type itemType = TypePrimitive.GetSystemTypeBySchema(tse, formatOptions);
               Type ienumType = typeof(IEnumerable<>);
               Type ienumGenericType = ienumType.MakeGenericType(itemType);
               elementType = itemType;
               columnType = ienumGenericType;
            }
         }
         else
         {
            elementType = TypePrimitive.GetSystemTypeBySchema(tse, formatOptions);
            columnType = elementType;
         }

         var se = new SchemaElement(tse, name, elementType, columnType, null);
         se.Parent = parent;
         return se;
      }

      private void AddFlags(SchemaElement node, params Thrift.SchemaElement[] tses)
      {
         if (node.Parent != null)
         {
            node.MaxRepetitionLevel = node.Parent.MaxRepetitionLevel;
            node.MaxDefinitionLevel = node.Parent.MaxDefinitionLevel;
         }

         foreach (Thrift.SchemaElement tse in tses)
         {
            if (tse.__isset.repetition_type && tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
            {
               node.MaxRepetitionLevel += 1;
            }

            //definition level increases for anything that _required_ (not just OPTIONAL)
            if(tse.Repetition_type != Thrift.FieldRepetitionType.REQUIRED)
            {
               node.MaxDefinitionLevel += 1;
            }
         }
      }

      #region [ Experimental ]

      public SchemaElement ParseSchemaExperimental(ParquetOptions formatOptions)
      {
         int si = 0;
         Thrift.SchemaElement tse = _fileMeta.Schema[si++];
         var root = new SchemaElement(tse.Name, DataType.Unspecified, null);

         ParseSchemaExperimenal(root, tse.Num_children, ref si, formatOptions);

         return root;
      }

      private void ParseSchemaExperimenal(SchemaElement parent, int childCount, ref int si, ParquetOptions formatOptions)
      {
         for(int i = 0; i < childCount && si < _fileMeta.Schema.Count; i++)
         {
            Thrift.SchemaElement tse = _fileMeta.Schema[si];
            IDataTypeHandler dth = DataTypeFactory.Match(tse, formatOptions);

            if (dth == null)
            {
               if (tse.Num_children > 0)
               {
                  //it's an element
                  ParseSchemaExperimenal(parent, _fileMeta.Schema[si++].Num_children, ref si, formatOptions);
                  continue;
               }
               else
               {
                  ThrowNoHandler(tse);
               }
            }

            SchemaElement newRoot = dth.Create(parent, _fileMeta.Schema, ref si);

            if(newRoot != null)
            {
               ParseSchemaExperimenal(newRoot, _fileMeta.Schema[si - 1].Num_children, ref si, formatOptions);
            }
         }
      }

      private void ThrowNoHandler(Thrift.SchemaElement tse)
      {
         string ct = tse.__isset.converted_type
            ? $" ({tse.Converted_type})"
            : null;

         string t = tse.__isset.type
            ? $"'{tse.Type}'"
            : "<unspecified>";

         throw new NotSupportedException($"cannot find data type handler for schema element '{tse.Name}' (type: {t}{ct})");
      }

      #endregion
   }
}
