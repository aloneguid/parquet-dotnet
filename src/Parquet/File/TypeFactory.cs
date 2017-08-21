using System;
using System.Collections.Generic;
using System.Collections;
using System.Reflection;
using Parquet.Data;
using System.Linq;
using Parquet.File.Values;

namespace Parquet.File
{
   static class TypeFactory
   {
      public struct TypeTag
      {
         public Thrift.Type PType;

         public Thrift.ConvertedType? ConvertedType;

         public Type ConcreteType;

         public int BitWidth;

         public TypeTag(Type concreteType, Thrift.Type ptype, Thrift.ConvertedType? convertedType, int bitWidth)
         {
            PType = ptype;
            ConvertedType = convertedType;
            ConcreteType = concreteType;
            BitWidth = bitWidth;
         }
      }

      private static readonly List<TypeTag> AllTags = new List<TypeTag>
      {
         new TypeTag(typeof(int), Thrift.Type.INT32, null, 32),
         new TypeTag(typeof(bool), Thrift.Type.BOOLEAN, null, 1),
         new TypeTag(typeof(string), Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8, 0),
         new TypeTag(typeof(float), Thrift.Type.FLOAT, null, 0),
         new TypeTag(typeof(double), Thrift.Type.DOUBLE, null, 0),
         // is the coerced type TIMESTAMP_MILLS but holds backward-compatilibility with Impala and HIVE
         new TypeTag(typeof(DateTimeOffset), Thrift.Type.INT96, null, 0),
         new TypeTag(typeof(DateTimeOffset), Thrift.Type.INT64, Thrift.ConvertedType.TIMESTAMP_MILLIS, 0),
         new TypeTag(typeof(Interval), Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.INTERVAL, 0)
      };

      private static readonly Dictionary<Type, TypeTag> SystemTypeToTag = new Dictionary<Type, TypeTag>();

      static TypeFactory()
      {
         foreach(TypeTag tt in AllTags)
         {
            if (!SystemTypeToTag.ContainsKey(tt.ConcreteType)) SystemTypeToTag[tt.ConcreteType] = tt; 
         }
      }

      private static readonly TypeTag DefaultTypeTag = new TypeTag(typeof(int), Thrift.Type.INT32, null, 32);

      public static int GetBitWidth(Type t)
      {
         if (!SystemTypeToTag.TryGetValue(t, out TypeTag tt)) return 0;

         return tt.BitWidth;
      }

      public static void AdjustSchema(Thrift.SchemaElement schema, Type systemType)
      {
         if(IsPrimitiveNullable(systemType))
         {
            throw new ArgumentException($"Type '{systemType}' in column '{schema.Name}' is a nullable type. Please pass either a class or a primitive type.", nameof(systemType));
         }

         bool flag = false;
         TypeTag tag = DefaultTypeTag;
         foreach (TypeTag type in AllTags)
         {
            if (type.ConcreteType == systemType)
            {
               tag = type;
               flag = true;
               break;
            }
         }
      
         if (!flag)
         {
            string supportedTypes = string.Join(", ", AllTags.Select(t => t.ConcreteType.ToString()).Distinct());

            throw new NotSupportedException($"system type {systemType} is not supported, list of supported types: '{supportedTypes}'");
         }

         schema.Type = tag.PType;

         if (tag.ConvertedType != null)
         {
            schema.Converted_type = tag.ConvertedType.Value;
         }
      }

      private static bool IsPrimitiveNullable(Type t)
      {
         TypeInfo ti = t.GetTypeInfo();

         if (ti.IsClass) return false;

         return ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(Nullable<>);
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

      public static IList Create(SchemaElement schema, ParquetOptions options, bool nullable = false)
      {
         Type t = ToSystemType(schema, options);
         return Create(t, nullable);
      }

      //todo: this can be rewritten by looking up in TypeToTag
      public static Type ToSystemType(SchemaElement schema, ParquetOptions options)
      {
         switch (schema.Thrift.Type)
         {
            case Thrift.Type.BOOLEAN:
               return typeof(bool);
            case Thrift.Type.INT32:
               return schema.IsAnnotatedWith(Thrift.ConvertedType.DATE) ? typeof(DateTimeOffset) : typeof(int);
            case Thrift.Type.FLOAT:
               return typeof(float);
            case Thrift.Type.INT64:
               return schema.IsAnnotatedWith(Thrift.ConvertedType.TIMESTAMP_MILLIS) ? typeof(DateTimeOffset) : typeof(long);
            case Thrift.Type.DOUBLE:
               return typeof(double);
            case Thrift.Type.INT96:
               // Need to look at this as default type is used here which is skewing this test - UTF8 + INT96 is an impossible siutation 
               return (schema.IsAnnotatedWith(Thrift.ConvertedType.TIMESTAMP_MILLIS) || schema.IsAnnotatedWith(Thrift.ConvertedType.UTF8) || options.TreatBigIntegersAsDates)
                  ? typeof(DateTimeOffset) 
                  : typeof(byte[]);
            case Thrift.Type.BYTE_ARRAY:
               return (schema.IsAnnotatedWith(Thrift.ConvertedType.UTF8) || schema.IsAnnotatedWith(Thrift.ConvertedType.JSON) || options.TreatByteArrayAsString)
                  ? typeof(string)
                  : typeof(byte[]);
            case Thrift.Type.FIXED_LEN_BYTE_ARRAY:
               // currently supports either fixed len Decimal types or 12-byte intervals
               return schema.IsAnnotatedWith(Thrift.ConvertedType.DECIMAL) ? typeof(decimal) : typeof(Interval);
            default:
               throw new NotImplementedException($"type {schema.Thrift.Type} not implemented");
         }

      }
   }
}
