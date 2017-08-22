using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.File.Values;

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
         new TypePrimitive<int>(Thrift.Type.INT32, null, 32),
         new TypePrimitive<bool>(Thrift.Type.BOOLEAN, null, 1),
         new TypePrimitive<string>(Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.UTF8),
         new TypePrimitive<float>(Thrift.Type.FLOAT),
         //new TypePrimitive<decimal>(Thrift.Type.BYTE_ARRAY, Thrift.ConvertedType.DECIMAL),   //complex
         new TypePrimitive<long>(Thrift.Type.INT64),
         new TypePrimitive<double>(Thrift.Type.DOUBLE),
         new TypePrimitive<DateTimeOffset>(Thrift.Type.INT96),
         new TypePrimitive<DateTimeOffset>(Thrift.Type.INT64, Thrift.ConvertedType.TIMESTAMP_MILLIS),
         new TypePrimitive<Interval>(Thrift.Type.FIXED_LEN_BYTE_ARRAY, Thrift.ConvertedType.INTERVAL),
         new TypePrimitive<DateTime>(Thrift.Type.INT96)
      };

      private static readonly Dictionary<Type, TypePrimitive> systemTypeToPrimitive =
         new Dictionary<Type, TypePrimitive>();

      static TypePrimitive()
      {
         foreach (TypePrimitive tt in allTypePrimitives)
         {
            if (!systemTypeToPrimitive.ContainsKey(tt.SystemType))
            {
               systemTypeToPrimitive[tt.SystemType] = tt;
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

      #endregion
   }
}
