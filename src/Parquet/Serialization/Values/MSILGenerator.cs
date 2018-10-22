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
      public delegate void PopulateListDelegate(object instances,
         object resultItemsList,
         object resultRepetitionsList);

      public MSILGenerator()
      {
      }

      public PopulateListDelegate GenerateCollector(Type classType, DataField dataField)
      {
         Type[] methodArgs = { typeof(object), typeof(object), typeof(object) };

         var runMethod = new DynamicMethod(
            $"Get{classType.Name}{dataField.Path}",
            typeof(void),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         GenerateCollector(il, classType, dataField.IsArray);

         return (PopulateListDelegate)runMethod.CreateDelegate(typeof(PopulateListDelegate));
      }

      private void GenerateCollector(ILGenerator il, Type elementType, bool hasRepetitions)
      {
         //arg 0 - collection
         //arg 1 - data items (typed list)
         //arg 2 - repetitions (optional)

         //make collection a local variable
         LocalBuilder collection = il.DeclareLocal(elementType);
         il.Emit(Ldarg_0);
         il.Emit(Stloc, collection.LocalIndex);

         using (il.ForEachLoop(elementType, collection, out LocalBuilder currentElement))
         {

         }

         il.Emit(Ret);
      }
   }
}
