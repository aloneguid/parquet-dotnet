using System;
using System.Collections.Generic;
using System.Text;
using Parquet.File;

namespace Parquet.Data
{
   /// <summary>
   /// Element of dataset's schema. Provides a helper way to construct a schema element with .NET generics.
   /// <typeparamref name="T">Type of element in the column</typeparamref>
   /// </summary>
   public class SchemaElement<T> : SchemaElement
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      public SchemaElement(string name) : base(name, Discover().dataType, Discover().hasNulls, Discover().isArray)
      {
      }

      private struct CInfo
      {
         public DataType dataType;
         public Type baseType;
         public bool isArray;
         public bool hasNulls;
      }

      private static CInfo Discover()
      {
         Type t = typeof(T);
         Type baseType = t;
         bool isArray = false;
         bool hasNulls = false;

         //throw a useful hint
         if(t.TryExtractDictionaryType(out Type dKey, out Type dValue))
         {
            throw new ArgumentException($"cannot declare a dictionary this way, please use {nameof(DictionarySchemaElement)}.");
         }

         if (t.TryExtractEnumerableType(out Type enumItemType))
         {
            baseType = enumItemType;
            isArray = true;
         }

         if (baseType.IsNullable())
         {
            baseType = baseType.GetNonNullable();
            hasNulls = true;
         }

         IDataTypeHandler handler = DataTypeFactory.Match(baseType);
         if (handler == null) DataTypeFactory.ThrowClrTypeNotSupported(baseType);

         return new CInfo
         {
            dataType = handler.DataType,
            baseType = baseType,
            isArray = isArray,
            hasNulls = hasNulls
         };
      }

   }

}
