using System;
using System.Collections.Generic;
using System.Text;
using Type = System.Type;
using TType = Parquet.Thrift.Type;
using System.Collections;
using Parquet.Thrift;

namespace Parquet.File
{
   static class ListFactory
   {
      struct TypeTag
      {
         public TType PType;

         public Func<IList> Create;

         public ConvertedType? ConvertedType;

         public TypeTag(TType ptype, Func<IList> create, ConvertedType? convertedType)
         {
            PType = ptype;
            Create = create;
            ConvertedType = convertedType;
         }
      }

      private static readonly Dictionary<Type, TypeTag> TypeToTag = new Dictionary<Type, TypeTag>
      {
         { typeof(int), new TypeTag(TType.INT32, () => new List<int>(), null) },
         { typeof(bool), new TypeTag(TType.BOOLEAN, () => new List<bool>(), null) },
         { typeof(string), new TypeTag(TType.BYTE_ARRAY, () => new List<string>(), ConvertedType.UTF8) }
      };

      public static IList Create(Type systemType, SchemaElement schema)
      {
         if (!TypeToTag.TryGetValue(systemType, out TypeTag tag))
            throw new NotSupportedException($"system type {systemType} is not supported");

         schema.Type = tag.PType;
         if(tag.ConvertedType != null)
            schema.Converted_type = tag.ConvertedType.Value;
         return tag.Create();
      }
   }
}
