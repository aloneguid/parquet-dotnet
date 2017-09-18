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
                  Thrift.SchemaElement tseTop = tse;
                  Thrift.SchemaElement tseList = _fileMeta.Schema[++i];
                  Thrift.SchemaElement tseElement = _fileMeta.Schema[++i];

                  mse = new SchemaElement(tseElement,
                     isRoot ? null : node,
                     formatOptions,
                     tseElement.Num_children == 0
                     ? typeof(IEnumerable)   //augmented to generic IEnumerable in constructor
                     : typeof(IEnumerable<Row>),
                     tseTop.Name);
                  mse.Path = string.Join(Schema.PathSeparator, tseTop.Name, tseList.Name, tseElement.Name);
                  mse.IsRepeated = true;
                  if (!isRoot) mse.Path = node.Path + Schema.PathSeparator + mse.Path;

                  mse.MaxDefinitionLevel = CountRepetitions(Thrift.FieldRepetitionType.OPTIONAL, mse, tseList);
                  mse.MaxRepetitionLevel = CountRepetitions(Thrift.FieldRepetitionType.REPEATED, mse, tseList, tseTop);

                  tse = tseElement;
               }
               else
               {

                  Type containerType = tse.Num_children > 0
                     ? typeof(Row)
                     : null;

                  SchemaElement parent = isRoot ? null : node;
                  mse = new SchemaElement(tse, parent, formatOptions, containerType);
                  mse.MaxDefinitionLevel = CountRepetitions(Thrift.FieldRepetitionType.OPTIONAL, mse);
                  mse.MaxRepetitionLevel = CountRepetitions(Thrift.FieldRepetitionType.REPEATED, mse);
               }

               node.Children.Add(mse);

               i += 1;

               if (tse.Num_children > 0)
               {
                  Build(mse, ref i, tse.Num_children, false);
               }
            }
         }

         //extract schema tree
         var root = new SchemaElement<int>("root");
         int start = 1;
         Build(root, ref start, _fileMeta.Schema[0].Num_children, true);

         return new Schema(root.Children);
      }

      private int CountRepetitions(Thrift.FieldRepetitionType repetitionType, SchemaElement schema, params Thrift.SchemaElement[] extra)
      {
         var elements = new List<Thrift.SchemaElement>();
         while(schema != null)
         {
            elements.Add(schema.Thrift);
            schema = schema.Parent;
         }

         if (extra != null) elements.AddRange(extra);

         return elements.Count(e => e.__isset.repetition_type && e.Repetition_type == repetitionType);
      }
   }
}
