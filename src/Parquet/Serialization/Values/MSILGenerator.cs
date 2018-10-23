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
         object resultRepetitionsList,
         int maxRepetitionLevel);

      public delegate void AssignArrayDelegate(object columnArray,
         object classInstances,
         int length);

      public MSILGenerator()
      {
      }

      public PopulateListDelegate GenerateCollector(Type classType, DataField field)
      {
         Type[] methodArgs = { typeof(object), typeof(object), typeof(object), typeof(int) };

         TypeInfo ti = classType.GetTypeInfo();
         PropertyInfo pi = ti.GetDeclaredProperty(field.ClrPropName ?? field.Name);
         MethodInfo getValueMethod = pi.GetMethod;

         MethodInfo addToListMethod = typeof(List<>).MakeGenericType(field.ClrNullableIfHasNullsType).GetTypeInfo().GetDeclaredMethod("Add");
         MethodInfo addRepLevelMethod = typeof(List<int>).GetTypeInfo().GetDeclaredMethod("Add");

         var runMethod = new DynamicMethod(
            $"Get{classType.Name}{field.Path}",
            typeof(object),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         GenerateCollector(il, classType,
            field,
            getValueMethod,
            addToListMethod,
            addRepLevelMethod);

         return (PopulateListDelegate)runMethod.CreateDelegate(typeof(PopulateListDelegate));
      }

      private void GenerateCollector(ILGenerator il, Type classType,
         DataField f,
         MethodInfo getValueMethod,
         MethodInfo addToListMethod,
         MethodInfo addRepLevelMethod)
      {
         //arg 0 - collection of classes, clr objects
         //arg 1 - data items (typed list)
         //arg 2 - repetitions (optional)
         //arg 3 - max repetition level

         //make collection a local variable
         LocalBuilder collection = il.DeclareLocal(classType);
         il.Emit(Ldarg_0);
         il.Emit(Stloc, collection.LocalIndex);

         //hold item
         LocalBuilder item = il.DeclareLocal(f.ClrNullableIfHasNullsType);

         //current repetition level
         LocalBuilder rl = null;
         if(f.IsArray)
         {
            rl = il.DeclareLocal(typeof(int));
         }

         using (il.ForEachLoop(classType, collection, out LocalBuilder currentElement))
         {
            if (f.IsArray)
            {
               //reset repetition level to 0
               il.Emit(Ldc_I4_0);
               il.StLoc(rl);

               //currentElement is a nested array in this case
               LocalBuilder array = il.DeclareLocal(typeof(object));

               //get array and put into arrayElement
               il.Emit(Ldloc, currentElement.LocalIndex);
               il.Emit(Callvirt, getValueMethod);
               il.Emit(Stloc, array.LocalIndex);

               //enumerate this array
               using (il.ForEachLoop(f.ClrNullableIfHasNullsType, array, out LocalBuilder arrayElement))
               {
                  //store in destination list
                  il.CallVirt(addToListMethod, Ldarg_1, arrayElement);

                  //add repetition level
                  il.CallVirt(addRepLevelMethod, Ldarg_2, rl);

                  //set repetition level to max
                  il.Emit(Ldarg_3);
                  il.StLoc(rl);
               }

            }
            else
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
         }

         il.Emit(Ldc_I4_0);
         il.Emit(Ret);
      }

      public AssignArrayDelegate GenerateAssigner(Type classType, DataField field)
      {
         Type[] methodArgs = { typeof(object), typeof(object), typeof(int) };
         var runMethod = new DynamicMethod(
            $"Set{classType.Name}{field.Name}",
            typeof(void),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         GenerateAssigner(il, classType, field);

         return (AssignArrayDelegate)runMethod.CreateDelegate(typeof(AssignArrayDelegate));
      }

      private void GenerateAssigner(ILGenerator il, Type classType, DataField field)
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
      }
   }
}
