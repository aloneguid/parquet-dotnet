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
      private readonly Dictionary<CollectorKey, CollectorTag> _collectorKeyToTag = new Dictionary<CollectorKey, CollectorTag>();

      public ClrBridge(Type classType)
      {
         _classType = classType;
      }

      public DataColumn BuildColumn(DataField field, IEnumerable classInstances, int classInstancesCount)
      {
         Array data = Array.CreateInstance(field.ClrNullableIfHasNullsType, classInstancesCount);
         CollectorTag tag = GetCollectorTag(new CollectorKey(_classType, field));
         int collected = tag.Collect(classInstances, data, classInstancesCount);

         return new DataColumn(field, data);
      }


      private void AddValue(ref PropertyInfo pi, ref int idx, object classInstance, DataField field, Array dest)
      {
         if (pi == null)
         {
            TypeInfo ti = _classType.GetTypeInfo();
            pi = ti.GetDeclaredProperty(field.Name);
         }

         object value = pi.GetValue(classInstance);
         dest.SetValue(value, idx++);
      }

      public IReadOnlyCollection<T> CreateClassInstances<T>(IReadOnlyCollection<DataColumn> columns) where T : new()
      {
         var result = new List<T>();

         throw new NotImplementedException();
      }

      private CollectorTag GetCollectorTag(CollectorKey key)
      {
         if(!_collectorKeyToTag.TryGetValue(key, out CollectorTag value))
         {
            value = new CollectorTag
            {
               Collect = CreateArrayDelegate(key.ClassType, key.Field)
            };

            _collectorKeyToTag[key] = value;
         }

         return value;
      }

      private CollectorTag.CollectArrayDelegate CreateArrayDelegate(Type classType, DataField field)
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
         Label lBody = il.DefineLabel();        //loop body start
         Label lExit = il.DefineLabel();        //final and return

         il.EmitWriteLine("start");

         //exit if loop counter is greater
         il.MarkLabel(lBody);
         il.Emit(OpCodes.Ldloc_0);  //counter
         il.Emit(OpCodes.Ldarg_2);  //max
         il.Emit(OpCodes.Clt);  //1 - less, 0 - equal or more (exit)
         il.Emit(OpCodes.Brfalse_S, lExit); //jump on 1

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

         //increment loop counter
         il.Emit(OpCodes.Ldc_I4_1);
         il.Emit(OpCodes.Ldloc_0);
         il.Emit(OpCodes.Add);
         il.Emit(OpCodes.Stloc_0);

         //loop again
         il.Emit(OpCodes.Br, lBody);

         il.MarkLabel(lExit);
         //return loop counter
         il.Emit(OpCodes.Ldloc_0);
         il.Emit(OpCodes.Ret);
         // -- end of IL Code ---

         return (CollectorTag.CollectArrayDelegate)runMethod.CreateDelegate(typeof(CollectorTag.CollectArrayDelegate));
      }
   }
}
