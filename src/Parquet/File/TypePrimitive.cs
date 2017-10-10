using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Data;
using Parquet.File.Values;
using Parquet.File.Values.Primitives;

namespace Parquet.File
{
   class TypePrimitive<TSystemType> : TypePrimitive
   {
      public TypePrimitive(
         Thrift.Type thriftType,
         Thrift.ConvertedType? thriftAnnotation = null,
         int? bitWidth = null) : 
         base(typeof(TSystemType), thriftType, thriftAnnotation, bitWidth)
      {
      }
   }

   class TypePrimitive
   {
      public Type SystemType { get; private set; }

      public Thrift.Type ThriftType { get; private set; }

      public Thrift.ConvertedType? ThriftAnnotation { get; private set; }

      public int? BitWidth { get; private set; }

      public TypePrimitive(Type systemType,
         Thrift.Type thriftType,
         Thrift.ConvertedType? thriftAnnotation = null,
         int? bitWidth = null)
      {
         SystemType = systemType ?? throw new ArgumentNullException(nameof(systemType));
         ThriftType = thriftType;
         ThriftAnnotation = thriftAnnotation;
         BitWidth = bitWidth;
      }


      #region [ Global Utility methods ]

      private static readonly List<TypePrimitive> allTypePrimitives = new List<TypePrimitive>
      {
         new TypePrimitive<byte[]>(Thrift.Type.BYTE_ARRAY),
         new TypePrimitive<int>(Thrift.Type.INT32, null, 32),
         new TypePrimitive<bool>(Thrift.Type.BOOLEAN, null, 1),
         new TypePrimitive<string>(Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8),
         new TypePrimitive<float>(Thrift.Type.FLOAT),
         new TypePrimitive<decimal>(Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.DECIMAL),
         new TypePrimitive<decimal>(Thrift.Type.INT32, Thrift.ConvertedType.DECIMAL),
         new TypePrimitive<decimal>(Thrift.Type.INT64, Thrift.ConvertedType.DECIMAL),
         new TypePrimitive<long>(Thrift.Type.INT64),
         new TypePrimitive<double>(Thrift.Type.DOUBLE),
         new TypePrimitive<DateTimeOffset>(Thrift.Type.INT96),
         new TypePrimitive<DateTimeOffset>(Thrift.Type.INT64, Thrift.ConvertedType.TIMESTAMP_MILLIS),
         new TypePrimitive<DateTimeOffset>(Thrift.Type.INT32, Thrift.ConvertedType.DATE),
         new TypePrimitive<Interval>(Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.INTERVAL),
         new TypePrimitive<DateTime>(Thrift.Type.INT96),
         new TypePrimitive<byte>(Thrift.Type.INT32, Thrift.ConvertedType.UINT_8, 8),
         new TypePrimitive<sbyte>(Thrift.Type.INT32, Thrift.ConvertedType.INT_8, 8),
         new TypePrimitive<short>(Thrift.Type.INT32, Thrift.ConvertedType.INT_16, 16),
         new TypePrimitive<ushort>(Thrift.Type.INT32, Thrift.ConvertedType.UINT_16, 16)
      };

      private static readonly Dictionary<Type, TypePrimitive> systemTypeToPrimitive =
         new Dictionary<Type, TypePrimitive>();

      private static readonly Dictionary<KeyValuePair<Thrift.Type, Thrift.ConvertedType?>, TypePrimitive>
         thriftTypeAndAnnotationToPrimitive =
            new Dictionary<KeyValuePair<Thrift.Type, Thrift.ConvertedType?>, TypePrimitive>();

      static TypePrimitive()
      {
         foreach (TypePrimitive tt in allTypePrimitives)
         {
            if (!systemTypeToPrimitive.ContainsKey(tt.SystemType))
            {
               systemTypeToPrimitive[tt.SystemType] = tt;
            }

            var ttan = new KeyValuePair<Thrift.Type, Thrift.ConvertedType?>(tt.ThriftType, tt.ThriftAnnotation);
            if (!thriftTypeAndAnnotationToPrimitive.ContainsKey(ttan))
            {
               thriftTypeAndAnnotationToPrimitive[ttan] = tt;
            }
         }
      }

      internal static int GetBitWidth(Type t)
      {
         if (!systemTypeToPrimitive.TryGetValue(t, out TypePrimitive tt)) return 0;

         return tt.BitWidth ?? 0;
      }

      internal static TypePrimitive Find(Type systemType)
      {
         if (!systemTypeToPrimitive.TryGetValue(systemType, out TypePrimitive tp))
         {
            string supportedTypes = string.Join(", ", allTypePrimitives.Select(t => t.SystemType.ToString()).Distinct());

            throw new NotSupportedException($"system type {systemType} is not supported, list of supported types: '{supportedTypes}'");
         }

         return tp;
      }

      internal static Type GetSystemTypeBySchema(SchemaElement schema, ParquetOptions options)
      {
         //edge cases

         switch (schema.Thrift.Type)
         {
            case Thrift.Type.INT96:
               if (options.TreatBigIntegersAsDates) return typeof(DateTimeOffset);
               break;
            case Thrift.Type.BYTE_ARRAY:
               if (options.TreatByteArrayAsString) return typeof(string);
               break;
         }

         //end of edge cases

         var kvp = new KeyValuePair<Thrift.Type, Thrift.ConvertedType?>(schema.Thrift.Type,
            schema.Thrift.__isset.converted_type
               ? new Thrift.ConvertedType?(schema.Thrift.Converted_type)
               : null);

         if (!thriftTypeAndAnnotationToPrimitive.TryGetValue(kvp, out TypePrimitive tp))
         {
            throw new NotSupportedException(
               $"cannot find primitive by type '{schema.Thrift.Type}' and annotation '{schema.Thrift.Converted_type}'");
         }

         return tp.SystemType;
      }

      #endregion
   }
}
