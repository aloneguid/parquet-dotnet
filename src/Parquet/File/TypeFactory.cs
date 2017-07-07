using System;
using System.Collections.Generic;
using System.Collections;
using System.Reflection;
using Parquet.Data;
using System.Linq;

namespace Parquet.File
{
   static class TypeFactory
   {
      struct TypeTag
      {
         public Thrift.Type PType;

         public Thrift.ConvertedType? ConvertedType;

         public TypeTag(Thrift.Type ptype, Thrift.ConvertedType? convertedType)
         {
            PType = ptype;
            ConvertedType = convertedType;
         }
      }

      private static readonly Dictionary<Type, TypeTag> TypeToTag = new Dictionary<Type, TypeTag>
      {
         { typeof(int), new TypeTag(Thrift.Type.INT32, null) },
         { typeof(bool), new TypeTag(Thrift.Type.BOOLEAN, null) },
         { typeof(string), new TypeTag(Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8) },
         { typeof(DateTimeOffset), new TypeTag(Thrift.Type.INT64, Thrift.ConvertedType.TIMESTAMP_MILLIS) }
      };

      public static void AdjustSchema(Thrift.SchemaElement schema, Type systemType)
      {
         if (!TypeToTag.TryGetValue(systemType, out TypeTag tag))
         {
            string supportedTypes = string.Join(", ", TypeToTag.Keys.Select(t => t.ToString()));

            throw new NotSupportedException($"system type {systemType} is not supported, we currently support only these types: '{supportedTypes}'");
         }

         schema.Type = tag.PType;

         if (tag.ConvertedType != null)
         {
            schema.Converted_type = tag.ConvertedType.Value;
         }
      }

      public static IList Create(Type systemType, bool nullable = false)
      {
         //make the type nullable if it's not a class
         if(nullable)
         {
            if(!systemType.GetTypeInfo().IsClass)
            {
               systemType = typeof(Nullable<>).MakeGenericType(systemType);
            }
         }

         //create generic list instance
         Type listType = typeof(List<>);
         Type listGType = listType.MakeGenericType(systemType);
         return (IList)Activator.CreateInstance(listGType);
      }

      public static IList Create(SchemaElement schema, bool nullable = false)
      {
         Type t = ToSystemType(schema.Thrift);
         return Create(t, nullable);
      }

      //todo: this can be rewritten by looking up in TypeToTag
      public static Type ToSystemType(Thrift.SchemaElement schema)
      {
         switch (schema.Type)
         {
            case Thrift.Type.BOOLEAN:
               return typeof(bool);
            case Thrift.Type.INT32:
               if (schema.Converted_type == Thrift.ConvertedType.DATE)
               {
                  return typeof(DateTimeOffset);
               }
               else
               {
                  return typeof(int);
               }
            case Thrift.Type.FLOAT:
               return typeof(float);
            case Thrift.Type.INT64:
               return typeof(long);
            case Thrift.Type.DOUBLE:
               return typeof(double);
            case Thrift.Type.INT96:
               return typeof(DateTimeOffset);
            case Thrift.Type.BYTE_ARRAY:
               if (schema.Converted_type == Thrift.ConvertedType.UTF8)
               {
                  return typeof(string);
               }
               else
               {
                  return typeof(byte[]);
               }
            case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
               if (schema.Converted_type == Thrift.ConvertedType.DECIMAL)
               {
                  return typeof(decimal);
               }
               else
               {
                  return typeof(byte[]);
               }
            default:
               throw new NotImplementedException($"type {schema.Type} not implemented");
         }

      }
   }
}
