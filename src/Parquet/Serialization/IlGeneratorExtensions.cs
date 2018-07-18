using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Text;

namespace Parquet.Serialization
{
   /// <summary>
   /// Extension methods to simplify IL Generation
   /// </summary>
   static class IlGeneratorExtensions
   {
      class FinaliseCode : IDisposable
      {
         private readonly ILGenerator _il;
         private readonly Action<ILGenerator> _action;

         public FinaliseCode(ILGenerator il, Action<ILGenerator> action)
         {
            _il = il;
            _action = action;
         }

         public void Dispose()
         {
            if (_action == null) return;

            _action(_il);
         }
      }

      /// <summary>
      /// Generates loop instruction
      /// </summary>
      /// <param name="il"></param>
      /// <param name="loadCounterInstruction"></param>
      /// <param name="storeCounterInstruction"></param>
      /// <param name="loadMaxInstruction"></param>
      /// <returns></returns>
      public static IDisposable Loop(this ILGenerator il,
         OpCode loadCounterInstruction,
         OpCode storeCounterInstruction,
         OpCode loadMaxInstruction)
      {
         Label lBody = il.DefineLabel();        //loop body start
         Label lExit = il.DefineLabel();        //final and return

         //exit if loop counter is greater
         il.MarkLabel(lBody);
         il.Emit(loadCounterInstruction);  //counter
         il.Emit(loadMaxInstruction);      //max
         il.Emit(OpCodes.Clt);  //1 - less, 0 - equal or more (exit)
         il.Emit(OpCodes.Brfalse_S, lExit); //jump on 1

         return new FinaliseCode(il, eil =>
         {
            eil.Emit(OpCodes.Ldc_I4_1);
            eil.Emit(loadCounterInstruction);
            eil.Emit(OpCodes.Add);
            eil.Emit(storeCounterInstruction);

            //loop again
            eil.Emit(OpCodes.Br, lBody);

            eil.MarkLabel(lExit);
         });
      }
   }
}
