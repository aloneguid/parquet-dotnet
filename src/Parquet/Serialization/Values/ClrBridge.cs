using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Parquet.Data;

namespace Parquet.Serialization.Values
{
   class ClrBridge
   {
      private readonly Type _classType;
      private readonly Dictionary<TypeCachingKey, CollectorTag> _collectorKeyToTag = new Dictionary<TypeCachingKey, CollectorTag>();
      private readonly Dictionary<TypeCachingKey, AssignerTag> _assignerKeyToTag = new Dictionary<TypeCachingKey, AssignerTag>();

      public ClrBridge(Type classType)
      {
         _classType = classType;
      }

      public DataColumn BuildColumn(DataField field, Array classInstances, int classInstancesCount)
      {
         Array data = Array.CreateInstance(field.ClrNullableIfHasNullsType, classInstancesCount);
         CollectorTag tag = GetCollectorTag(new TypeCachingKey(_classType, field));
         int collected = tag.Collect(classInstances, data, classInstancesCount);

         return new DataColumn(field, data);
      }

      public void AssignColumn(DataColumn dataColumn, Array classInstances, int classInstancesCount)
      {
         AssignerTag tag = GetAssignerTag(new TypeCachingKey(_classType, dataColumn.Field));
         int assigned = tag.Assign(dataColumn.Data, classInstances, classInstancesCount);
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
            typeof(int),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();
         // -- IL Code ---
         il.DeclareLocal(typeof(int));                          //loop counter
         il.DeclareLocal(classType);                            //current element instance
         il.DeclareLocal(field.ClrNullableIfHasNullsType);      //!!! specific to property type

         using (il.Loop(OpCodes.Ldloc_0, OpCodes.Stloc_0, OpCodes.Ldarg_2))
         {
            //load data element
            il.Emit(OpCodes.Ldarg_0);  //data array
            il.Emit(OpCodes.Ldloc_0);  //index
            if (field.HasNulls && !field.ClrNullableIfHasNullsType.IsSystemNullable())
            {
               il.Emit(OpCodes.Ldelem_Ref);
            }
            else
            {
               il.Emit(OpCodes.Ldelem, field.ClrNullableIfHasNullsType);
            }
            il.Emit(OpCodes.Stloc_2);  //data value

            //load class element
            il.Emit(OpCodes.Ldarg_1);  //classes array
            il.Emit(OpCodes.Ldloc_0);  //index
            il.Emit(OpCodes.Ldelem_Ref);
            il.Emit(OpCodes.Stloc_1);  //class instance

            //assign data element to class property
            il.Emit(OpCodes.Ldloc_1);  //element to make call on
            il.Emit(OpCodes.Ldloc_2);  //data element
            il.Emit(OpCodes.Callvirt, setValueMethod);
         }

         //return loop counter
         il.Emit(OpCodes.Ldloc_0);
         il.Emit(OpCodes.Ret);
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
            typeof(int),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         // -- IL Code ---
         il.DeclareLocal(typeof(int));                          //loop counter
         il.DeclareLocal(classType);                            //current element instance
         il.DeclareLocal(field.ClrNullableIfHasNullsType);      //!!! specific to property type

         using (il.Loop(OpCodes.Ldloc_0, OpCodes.Stloc_0, OpCodes.Ldarg_2))
         {
            //get object instance from array
            il.Emit(OpCodes.Ldarg_0);  //load array
            il.Emit(OpCodes.Ldloc_0);  //load index
            il.Emit(OpCodes.Ldelem_Ref);
            il.Emit(OpCodes.Stloc_1);  //save element

            //get the property value (first parameter - object instance)
            il.Emit(OpCodes.Ldloc_1);  //load element
            il.Emit(OpCodes.Callvirt, getValueMethod);
            il.Emit(OpCodes.Stloc_2);  //save property value

            //assign property value to array element
            il.Emit(OpCodes.Ldarg_1);  //load destination array
            il.Emit(OpCodes.Ldloc_0);  //load index
            il.Emit(OpCodes.Ldloc_2);  //load element
            if (field.HasNulls && !field.ClrNullableIfHasNullsType.IsSystemNullable())
            {
               il.Emit(OpCodes.Stelem_Ref);
            }
            else
            {
               il.Emit(OpCodes.Stelem, field.ClrNullableIfHasNullsType);
            }
         }

         //return loop counter
         il.Emit(OpCodes.Ldloc_0);
         il.Emit(OpCodes.Ret);
         // -- end of IL Code ---

         return (CollectorTag.CollectArrayDelegate)runMethod.CreateDelegate(typeof(CollectorTag.CollectArrayDelegate));
      }
   }
}
