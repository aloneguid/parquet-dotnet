using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Parquet.Data;
using static System.Reflection.Emit.OpCodes;

namespace Parquet.Serialization.Values
{
   class MSILGenerator
   {
      public delegate object PopulateListDelegate(object instances,
         object resultItemsList,
         object resultRepetitionsList);

      public MSILGenerator()
      {
      }

      public PopulateListDelegate GenerateCollector(Type classType, DataField field)
      {
         Type[] methodArgs = { typeof(object), typeof(object), typeof(object) };

         TypeInfo ti = classType.GetTypeInfo();
         PropertyInfo pi = ti.GetDeclaredProperty(field.ClrPropName ?? field.Name);
         MethodInfo getValueMethod = pi.GetMethod;

         MethodInfo addToListMethod = typeof(List<>).MakeGenericType(field.ClrNullableIfHasNullsType).GetTypeInfo().GetDeclaredMethod("Add");

         var runMethod = new DynamicMethod(
            $"Get{classType.Name}{field.Path}",
            typeof(object),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         GenerateCollector(il, classType,
            field,
            getValueMethod,
            addToListMethod);

         return (PopulateListDelegate)runMethod.CreateDelegate(typeof(PopulateListDelegate));
      }

      private void GenerateCollector(ILGenerator il, Type classType,
         DataField f,
         MethodInfo getValueMethod,
         MethodInfo addToListMethod)
      {
         //arg 0 - collection of classes, clr objects
         //arg 1 - data items (typed list)
         //arg 2 - repetitions (optional)

         //make collection a local variable
         LocalBuilder collection = il.DeclareLocal(classType);
         il.Emit(Ldarg_0);
         il.Emit(Stloc, collection.LocalIndex);

         //hold item
         LocalBuilder item = il.DeclareLocal(f.ClrNullableIfHasNullsType);

         using (il.ForEachLoop(classType, collection, out LocalBuilder currentElement))
         {
            //get current value
            il.Emit(Ldloc, currentElement.LocalIndex);
            il.Emit(Callvirt, getValueMethod);
            il.Emit(Stloc, item.LocalIndex);

            //store in destination list
            il.Emit(Ldarg_1);
            il.Emit(Ldloc, item.LocalIndex);
            il.Emit(Callvirt, addToListMethod);
         }

         il.Emit(Ldc_I4_0);
         il.Emit(Ret);
      }
   }
}
