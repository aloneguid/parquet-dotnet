﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Serialization.Values {
    class ClrBridge {
        private readonly Type _classType;
        private static readonly ConcurrentDictionary<TypeCachingKey, MSILGenerator.PopulateListDelegate> _collectorKeyToTag = new ConcurrentDictionary<TypeCachingKey, MSILGenerator.PopulateListDelegate>();

        public ClrBridge(Type classType) {
            _classType = classType;
        }

        public DataColumn BuildColumn(DataField field, Array classInstances, int classInstancesCount) {
            var key = new TypeCachingKey(_classType, field);

            MSILGenerator.PopulateListDelegate populateList = _collectorKeyToTag.GetOrAdd(key, (_) => new MSILGenerator().GenerateCollector(_classType, field));

            IList resultList = field.ClrNullableIfHasNullsType.CreateGenericList();
            Type prop = PropertyHelpers.GetDeclaredPropertyFromClassType(_classType, field).PropertyType;
            bool underlyingTypeIsEnumerable = prop.TryExtractEnumerableType(out _);
            List<int> repLevelsList = field.IsArray || underlyingTypeIsEnumerable ? new List<int>() : null;
            object result = populateList(classInstances, resultList, repLevelsList, field.MaxRepetitionLevel);

            MethodInfo toArrayMethod = typeof(List<>).MakeGenericType(field.ClrNullableIfHasNullsType).GetTypeInfo().GetDeclaredMethod("ToArray");
            object array = toArrayMethod.Invoke(resultList, null);

            return new DataColumn(field, (Array)array, repLevelsList?.ToArray());
        }

        public void AssignColumn(DataColumn dataColumn, Array classInstances) {
            var key = new TypeCachingKey(_classType, dataColumn.Field);
            MSILGenerator.AssignArrayDelegate assignColumn = new MSILGenerator().GenerateAssigner(dataColumn, _classType);
            assignColumn(dataColumn, classInstances);
        }
    }
}