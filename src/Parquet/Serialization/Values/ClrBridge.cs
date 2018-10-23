using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using Parquet.Data;
using static System.Reflection.Emit.OpCodes;

namespace Parquet.Serialization.Values
{
   class ClrBridge
   {
      public delegate void AssignArrayDelegate(object columnArray, object classInstances, int length);

      private readonly Type _classType;
      private static readonly Dictionary<TypeCachingKey, MSILGenerator.PopulateListDelegate> _collectorKeyToTag = new Dictionary<TypeCachingKey, MSILGenerator.PopulateListDelegate>();
      private static readonly Dictionary<TypeCachingKey, AssignArrayDelegate> _assignerKeyToTag = new Dictionary<TypeCachingKey, AssignArrayDelegate>();

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
         AssignArrayDelegate assign = GetAssignerTag(new TypeCachingKey(_classType, dataColumn.Field));

         assign(dataColumn.Data, classInstances, classInstancesCount);
      }

      private AssignArrayDelegate GetAssignerTag(TypeCachingKey key)
      {
         if(!_assignerKeyToTag.TryGetValue(key, out AssignArrayDelegate value))
         {
            _assignerKeyToTag[key] = value = CreateAssignerDelegate(key.ClassType, key.Field);
         }

         return value;
      }

      private AssignArrayDelegate CreateAssignerDelegate(Type classType, DataField field)
      {
         TypeInfo ti = classType.GetTypeInfo();
         PropertyInfo pi = ti.GetDeclaredProperty(field.ClrPropName ?? field.Name);
         MethodInfo setValueMethod = pi.SetMethod;

         // array, classes, length
         Type[] methodArgs = { typeof(object), typeof(object), typeof(int) };
         var runMethod = new DynamicMethod(
            $"Set{classType.Name}{field.Name}",
            typeof(void),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         LocalBuilder length = il.DeclareLocal(typeof(int));
         LocalBuilder dataItem = il.DeclareLocal(field.ClrNullableIfHasNullsType);
         LocalBuilder instanceItem = il.DeclareLocal(classType);

         il.Emit(Ldarg_2);
         il.Emit(Stloc, length.LocalIndex);

         using (il.ForLoop(length, out LocalBuilder i))
         {
            //load data element
            il.Emit(Ldarg_0);  //data array
            il.Emit(Ldloc, i.LocalIndex);  //index
            if (field.HasNulls && !field.ClrNullableIfHasNullsType.IsSystemNullable())
            {
               il.Emit(Ldelem_Ref);
            }
            else
            {
               il.Emit(Ldelem, field.ClrNullableIfHasNullsType);
            }
            il.Emit(Stloc, dataItem.LocalIndex);  //data value

            //load class element
            il.Emit(Ldarg_1);  //classes array
            il.Emit(Ldloc, i.LocalIndex);  //index
            il.Emit(Ldelem_Ref);
            il.Emit(Stloc, instanceItem.LocalIndex);  //class instance

            //assign data element to class property
            il.Emit(Ldloc, instanceItem.LocalIndex);  //element to make call on
            il.Emit(Ldloc, dataItem.LocalIndex);  //data element
            il.Emit(Callvirt, setValueMethod);
         }

         il.Emit(Ret);
         // -- end of IL Code ---

         return (AssignArrayDelegate)runMethod.CreateDelegate(typeof(AssignArrayDelegate));
      }

   }
}
