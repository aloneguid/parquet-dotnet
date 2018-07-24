using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.File;

namespace Parquet.Data
{
   abstract class BasicPrimitiveDataTypeHandler<TSystemType> : BasicDataTypeHandler<TSystemType>
      where TSystemType : struct
   {
      public BasicPrimitiveDataTypeHandler(DataType dataType, Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null)
         : base(dataType, thriftType, convertedType)
      {
      }

      public override Array GetArray(int minCount, bool rent, bool isNullable)
      {
         if(rent)
         {
            return isNullable
               ? ArrayPool<TSystemType?>.Shared.Rent(minCount)
               : ArrayPool<TSystemType?>.Shared.Rent(minCount);
         }

         if (isNullable)
         {
            return new TSystemType?[minCount];
         }
         return new TSystemType[minCount];
      }

      public override void ReturnArray(Array array, bool isNullable)
      {
         if(isNullable)
         {
            ArrayPool<TSystemType?>.Shared.Return((TSystemType?[])array);
         }
         else
         {
            ArrayPool<TSystemType>.Shared.Return((TSystemType[])array);
         }
      }

      public override Array UnpackDefinitions(Array untypedSource, int[] definitionLevels, int maxDefinitionLevel)
      {
         return UnpackDefinitions((TSystemType[])untypedSource, definitionLevels, maxDefinitionLevel);
      }

      public override TypedArrayWrapper CreateTypedArrayWrapper(Array array, bool isNullable)
      {
         if (isNullable)
         {
            return TypedArrayWrapper.Create<TSystemType?>(array);
         }
         return TypedArrayWrapper.Create<TSystemType>(array);
      }

      private TSystemType?[] UnpackDefinitions(TSystemType[] src, int[] definitionLevels, int maxDefinitionLevel)
      {
         TSystemType?[] result = (TSystemType?[])GetArray(definitionLevels.Length, false, true);

         int isrc = 0;
         for (int i = 0; i < definitionLevels.Length; i++)
         {
            int level = definitionLevels[i];

            if (level == maxDefinitionLevel)
            {
               result[i] = src[isrc++];
            }
         }

         return result;

      }
   }
}
