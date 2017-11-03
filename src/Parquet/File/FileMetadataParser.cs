using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using System.Linq;

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
                  mse = new SchemaElement(tse, parent, formatOptions, containerType);

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

      private SchemaElement BuildListSchema(ref Thrift.SchemaElement tse, ref int i, bool isRoot, SchemaElement node, ParquetOptions formatOptions)
      {
         Thrift.SchemaElement tseTop = tse;
         Thrift.SchemaElement tseList = _fileMeta.Schema[++i];
         Thrift.SchemaElement tseElement = _fileMeta.Schema[++i];

         SchemaElement mse = new SchemaElement(tseElement,
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

         var mse = new SchemaElement<IDictionary>(tse.Name);   //throws NotSupportedException

         //todo: repetition and definition levels for mse

         ++i;  //skip container

         Thrift.SchemaElement tseKey = _fileMeta.Schema[++i];
         Thrift.SchemaElement tseValue = _fileMeta.Schema[++i];

         //todo: combine key and value meta into mse (as children?)

         throw new NotImplementedException("map is not implemented yet.");
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
   }
}
