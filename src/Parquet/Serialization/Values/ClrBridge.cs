using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using Parquet.Data;
using static System.Reflection.Emit.OpCodes;

namespace Parquet.Serialization.Values
{
   class ClrBridge
   {
      private readonly Type _classType;
      private static readonly Dictionary<TypeCachingKey, CollectorTag> _collectorKeyToTag = new Dictionary<TypeCachingKey, CollectorTag>();
      private static readonly Dictionary<TypeCachingKey, AssignerTag> _assignerKeyToTag = new Dictionary<TypeCachingKey, AssignerTag>();

      public ClrBridge(Type classType)
      {
         _classType = classType;
      }

      public DataColumn BuildColumn(DataField field, Array classInstances, int classInstancesCount)
      {
         Array data = Array.CreateInstance(field.ClrNullableIfHasNullsType, classInstancesCount);
         CollectorTag tag = GetCollectorTag(new TypeCachingKey(_classType, field));
         tag.Collect(classInstances, data, classInstancesCount);

         return new DataColumn(field, data);
      }

      public void AssignColumn(DataColumn dataColumn, Array classInstances, int classInstancesCount)
      {
         AssignerTag tag = GetAssignerTag(new TypeCachingKey(_classType, dataColumn.Field));
         tag.Assign(dataColumn.Data, classInstances, classInstancesCount);
      }

      private CollectorTag GetCollectorTag(TypeCachingKey key)
      {
         if(!_collectorKeyToTag.TryGetValue(key, out CollectorTag value))
         {
            value = new CollectorTag
            {
               Collect = CreateCollectorDelegate(key.ClassType, key.Field)
            };

            _collectorKeyToTag[key] = value;
         }

         return value;
      }

      private AssignerTag GetAssignerTag(TypeCachingKey key)
      {
         if(!_assignerKeyToTag.TryGetValue(key, out AssignerTag value))
         {
            value = new AssignerTag
            {
               Assign = CreateAssignerDelegate(key.ClassType, key.Field)
            };

            _assignerKeyToTag[key] = value;
         }

         return value;
      }

      private AssignerTag.AssignArrayDelegate CreateAssignerDelegate(Type classType, DataField field)
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

         return (AssignerTag.AssignArrayDelegate)runMethod.CreateDelegate(typeof(AssignerTag.AssignArrayDelegate));
      }

      private CollectorTag.CollectArrayDelegate CreateCollectorDelegate(Type classType, DataField field)
      {
         TypeInfo ti = classType.GetTypeInfo();
         PropertyInfo pi = ti.GetDeclaredProperty(field.ClrPropName ?? field.Name);
         MethodInfo getValueMethod = pi.GetMethod;

         Type[] methodArgs = { typeof(object), typeof(object), typeof(int) };
         var runMethod = new DynamicMethod(
            $"Get{classType.Name}{field.Name}",
            typeof(void),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         // -- IL Code ---
         //il.DeclareLocal(typeof(int)); //loop counter
         LocalBuilder length = il.DeclareLocal(typeof(int));
         LocalBuilder instanceItem = il.DeclareLocal(classType);
         LocalBuilder dataItem = il.DeclareLocal(field.ClrNullableIfHasNullsType);

         il.Emit(Ldarg_2);
         il.Emit(Stloc, length.LocalIndex);

         using (il.ForLoop(length, out LocalBuilder i))
         {
            //get object instance from array
            il.Emit(Ldarg_0);  //load array
            il.Emit(Ldloc, i.LocalIndex);  //load index
            il.Emit(Ldelem_Ref);
            il.Emit(Stloc, instanceItem.LocalIndex);  //save element

            //get the property value (first parameter - object instance)
            il.Emit(Ldloc, instanceItem.LocalIndex);  //load element
            il.Emit(Callvirt, getValueMethod);
            il.Emit(Stloc, dataItem.LocalIndex);  //save property value

            //assign property value to array element
            il.Emit(Ldarg_1);  //load destination array
            il.Emit(Ldloc, i.LocalIndex);  //load index
            il.Emit(Ldloc, dataItem.LocalIndex);  //load element
            if (field.HasNulls && !field.ClrNullableIfHasNullsType.IsSystemNullable())
            {
               il.Emit(Stelem_Ref);
            }
            else
            {
               il.Emit(Stelem, field.ClrNullableIfHasNullsType);
            }

         }

         il.Emit(Ret);
         // -- end of IL Code ---

         return (CollectorTag.CollectArrayDelegate)runMethod.CreateDelegate(typeof(CollectorTag.CollectArrayDelegate));
      }
   }
}
