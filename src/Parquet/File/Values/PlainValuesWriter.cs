using Parquet.Thrift;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using TType = Parquet.Thrift.Type;

namespace Parquet.File.Values
{
   class PlainValuesWriter : IValuesWriter
   {
      public void Write(BinaryWriter writer, SchemaElement schema, IList data)
      {
         switch(schema.Type)
         {
            case TType.INT32:
               WriteInt32(writer, schema, data);
               break;

            default:
               throw new NotImplementedException($"type {schema.Type} not implemented");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteInt32(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var dataTyped = (List<int>)data;
         foreach(int el in dataTyped)
         {
            writer.Write(el);
         }
      }
   }
}
