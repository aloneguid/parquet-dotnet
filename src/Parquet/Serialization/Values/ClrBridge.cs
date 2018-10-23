using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Parquet.Data;

namespace Parquet.Serialization.Values
{
   class ClrBridge
   {
      private readonly Type _classType;
      private static readonly Dictionary<TypeCachingKey, MSILGenerator.PopulateListDelegate> _collectorKeyToTag = new Dictionary<TypeCachingKey, MSILGenerator.PopulateListDelegate>();
      private static readonly Dictionary<TypeCachingKey, MSILGenerator.AssignArrayDelegate> _assignerKeyToTag = new Dictionary<TypeCachingKey, MSILGenerator.AssignArrayDelegate>();

      public ClrBridge(Type classType)
      {
         _classType = classType;
      }

      public DataColumn BuildColumn(DataField field, Array classInstances, int classInstancesCount)
      {
         var key = new TypeCachingKey(_classType, field);

         if(!_collectorKeyToTag.TryGetValue(key, out MSILGenerator.PopulateListDelegate populateList))
         {
            _collectorKeyToTag[key] = populateList = new MSILGenerator().GenerateCollector(_classType, field);
         }

         IList resultList = field.ClrNullableIfHasNullsType.CreateGenericList();
         object result = populateList(classInstances, resultList, null);

         MethodInfo toArrayMethod = typeof(List<>).MakeGenericType(field.ClrNullableIfHasNullsType).GetTypeInfo().GetDeclaredMethod("ToArray");
         object array = toArrayMethod.Invoke(resultList, null);

         return new DataColumn(field, (Array)array);
      }

      public void AssignColumn(DataColumn dataColumn, Array classInstances, int classInstancesCount)
      {
         var key = new TypeCachingKey(_classType, dataColumn.Field);

         if(!_assignerKeyToTag.TryGetValue(key, out MSILGenerator.AssignArrayDelegate assignColumn))
         {
            _assignerKeyToTag[key] = assignColumn = new MSILGenerator().GenerateAssigner(_classType, dataColumn.Field);
         }

         assignColumn(dataColumn.Data, classInstances, classInstancesCount);
      }
   }
}
