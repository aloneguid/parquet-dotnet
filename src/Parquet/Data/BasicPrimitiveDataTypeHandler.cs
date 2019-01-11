using System;
using System.Buffers;
using System.Linq;

namespace Parquet.Data
{
   abstract class BasicPrimitiveDataTypeHandler<TSystemType> : BasicDataTypeHandler<TSystemType>
      where TSystemType : struct
   {
      private static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

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

      public override Array PackDefinitions(Array data, int maxDefinitionLevel, out int[] definitions, out int definitionsLength, out int nullCount)
      {
         return PackDefinitions((TSystemType?[])data, maxDefinitionLevel, out definitions, out definitionsLength, out nullCount);
      }

      public override Array UnpackDefinitions(Array untypedSource, int[] definitionLevels, int maxDefinitionLevel, out bool[] hasValueFlags)
      {
         return UnpackDefinitions((TSystemType[])untypedSource, definitionLevels, maxDefinitionLevel, out hasValueFlags);
      }

      private TSystemType?[] UnpackDefinitions(TSystemType[] src, int[] definitionLevels, int maxDefinitionLevel, out bool[] hasValueFlags)
      {
         TSystemType?[] result = (TSystemType?[])GetArray(definitionLevels.Length, false, true);
         hasValueFlags = new bool[definitionLevels.Length];

         int isrc = 0;
         for (int i = 0; i < definitionLevels.Length; i++)
         {
            int level = definitionLevels[i];

            if (level == maxDefinitionLevel)
            {
               result[i] = src[isrc++];
               hasValueFlags[i] = true;
            }
            else if(level == 0)
            {
               hasValueFlags[i] = true;
            }

         }

         return result;

      }

      private TSystemType[] PackDefinitions(TSystemType?[] data, int maxDefinitionLevel, out int[] definitionLevels, out int definitionsLength, out int nullCount)
      {
         definitionLevels = IntPool.Rent(data.Length);
         definitionsLength = data.Length;

         nullCount = data.Count(i => !i.HasValue);
         TSystemType[] result = new TSystemType[data.Length - nullCount];
         int ir = 0;

         for(int i = 0; i < data.Length; i++)
         {
            TSystemType? value = data[i];

            if(value == null)
            {
               definitionLevels[i] = 0;
            }
            else
            {
               definitionLevels[i] = maxDefinitionLevel;
               result[ir++] = value.Value;
            }
         }

         return result;
      }
   }
}
