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

      public delegate void AssignArrayDelegate(
         Array values,
         int[] repetitionLevels,
         Array classInstances);

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
         Type[] methodArgs = { typeof(Array), typeof(int[]), typeof(Array) };
         var runMethod = new DynamicMethod(
            $"Set{classType.Name}{field.Name}",
            typeof(void),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         TypeInfo ti = classType.GetTypeInfo();
         PropertyInfo pi = ti.GetDeclaredProperty(field.ClrPropName ?? field.Name);
         MethodInfo setValueMethod = pi.SetMethod;

         GenerateAssigner(il, classType, field,
            setValueMethod);

         return (AssignArrayDelegate)runMethod.CreateDelegate(typeof(AssignArrayDelegate));
      }

      private void GenerateAssigner(ILGenerator il, Type classType, DataField field,
         MethodInfo setValueMethod)
      {
         //arg 0 - datacolumn array (Array)
         //arg 1 - repetition levels array (int[])
         //arg 2 - class intances array (Array)

         //get length of values
         LocalBuilder dataLength = il.DeclareLocal(typeof(int));
         il.GetArrayLength(Ldarg_0, dataLength);

         //get class array length
         LocalBuilder classLength = il.DeclareLocal(typeof(int));
         il.GetArrayLength(Ldarg_2, classLength);

         LocalBuilder dataItem = il.DeclareLocal(field.ClrNullableIfHasNullsType);
         bool dataIsRef = field.HasNulls && !field.ClrNullableIfHasNullsType.IsSystemNullable();

         LocalBuilder classInstance = il.DeclareLocal(classType);

         using (il.ForLoop(dataLength, out LocalBuilder i))
         {
            //get data value
            il.GetArrayElement(Ldarg_0, i, dataIsRef, field.ClrNullableIfHasNullsType, dataItem);

            //get class instance
            il.GetArrayElement(Ldarg_2, i, true, classType, classInstance);

            //assign data item to class property
            il.CallVirt(setValueMethod, classInstance, dataItem);
         }

         il.Emit(Ret);
      }
   }
}
