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
      public struct TypeTag
      {
         public Thrift.Type PType;

         public Thrift.ConvertedType? ConvertedType;

         public Type ConcreteType;

         public TypeTag(Type concreteType, Thrift.Type ptype, Thrift.ConvertedType? convertedType)
         {
            PType = ptype;
            ConvertedType = convertedType;
            ConcreteType = concreteType;
         }
      }

      private static readonly List<TypeTag> TypeToTag = new List<TypeTag>
      {
         new TypeTag(typeof(int), Thrift.Type.INT32, null),
         new TypeTag(typeof(bool), Thrift.Type.BOOLEAN, null),
         new TypeTag(typeof(string), Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8),
         new TypeTag(typeof(float), Thrift.Type.FLOAT, null),
         new TypeTag(typeof(double), Thrift.Type.DOUBLE, null),
         // is the coerced type TIMESTAMP_MILLS but holds backward-compatilibility with Impala and HIVE
         new TypeTag(typeof(DateTimeOffset), Thrift.Type.INT96, null),
         new TypeTag(typeof(DateTimeOffset), Thrift.Type.INT64, Thrift.ConvertedType.TIMESTAMP_MILLIS)
      };

      private static readonly TypeTag DefaultTypeTag = new TypeTag(typeof(int), Thrift.Type.INT32, null);

      public static void AdjustSchema(Thrift.SchemaElement schema, Type systemType)
      {
         bool flag = false;
         TypeTag tag = DefaultTypeTag;
         foreach (var type in TypeToTag)
         {
            if (type.ConcreteType == systemType &&
                ((type.ConvertedType != null && type.ConvertedType == schema.Converted_type)
                 || type.ConvertedType == null))
            {
               tag = type;
               flag = true;
               break;
            }
         }
      
         if (!flag)
         {
            string supportedTypes = string.Join(", ", TypeToTag.Select(t => t.ConcreteType.ToString()));

            throw new NotSupportedException($"system type {systemType} is not supported, list of supported types: '{supportedTypes}'");
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
               return schema.Converted_type == Thrift.ConvertedType.DATE ? typeof(DateTimeOffset) : typeof(int);
            case Thrift.Type.FLOAT:
               return typeof(float);
            case Thrift.Type.INT64:
               return schema.Converted_type == Thrift.ConvertedType.TIMESTAMP_MILLIS ? typeof(DateTimeOffset) : typeof(long);
            case Thrift.Type.DOUBLE:
               return typeof(double);
            case Thrift.Type.INT96:
               return typeof(DateTimeOffset);
            //return schema.Converted_type == Thrift.ConvertedType.INTERVAL ? typeof(DateTimeOffset) : typeof(byte[]);
            case Thrift.Type.BYTE_ARRAY:
               return schema.Converted_type == Thrift.ConvertedType.UTF8 ? typeof(string) : typeof(byte[]);
            case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
               return schema.Converted_type == Thrift.ConvertedType.DECIMAL ? typeof(decimal) : typeof(byte[]);
            default:
               throw new NotImplementedException($"type {schema.Type} not implemented");
         }

      }
   }
}
