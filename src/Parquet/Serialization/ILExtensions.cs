using System;
using System.Collections;
using System.Reflection;
using System.Reflection.Emit;
using static System.Reflection.Emit.OpCodes;

namespace Parquet.Serialization
{
   /// <summary>
   /// Extension methods to simplify MSIL generation
   /// </summary>
   static class ILExtensions
   {
      //brtrue_s vs brtrue - "short" forms use 1-byte offset used to generate smaller binaries, however they won't reach the destination
      //if you're jumping too far, so beware!!!!

      private static readonly MethodInfo getEnumeratorMethod;
      private static readonly MethodInfo moveNextMethod;
      private static readonly MethodInfo getCurrentMethod;

      static ILExtensions()
      {
         TypeInfo iEnumerable = typeof(IEnumerable).GetTypeInfo();
         TypeInfo iEnumerator = typeof(IEnumerator).GetTypeInfo();

         getEnumeratorMethod = iEnumerable.GetDeclaredMethod(nameof(IEnumerable.GetEnumerator));
         moveNextMethod = iEnumerator.GetDeclaredMethod(nameof(IEnumerator.MoveNext));
         getCurrentMethod = iEnumerator.GetDeclaredProperty(nameof(IEnumerator.Current)).GetMethod;
      }

      public static IDisposable ForEachLoop(this ILGenerator il, Type elementType, LocalBuilder collection, out LocalBuilder currentElement)
      {

         Label lMoveNext = il.DefineLabel();
         Label lWork = il.DefineLabel();
         currentElement = il.DeclareLocal(elementType);

         //get collection enumerator
         LocalBuilder enumerator = il.DeclareLocal(typeof(IEnumerator));
#if DEBUG
         il.EmitWriteLine("foreach-begin");
#endif
         il.Emit(Ldloc, collection.LocalIndex);
         il.Emit(Callvirt, getEnumeratorMethod);
         il.Emit(Stloc, enumerator.LocalIndex);

         //immediately move to "move next" to start enumeration
         il.Emit(Br, lMoveNext);

         //iteration work block
         il.MarkLabel(lWork);
#if DEBUG
         il.EmitWriteLine("  foreach-loop");
#endif

         //get current element
         il.Emit(Ldloc, enumerator.LocalIndex);
         il.Emit(Callvirt, getCurrentMethod);
         il.Emit(Unbox_Any, elementType);
         il.Emit(Stloc, currentElement.LocalIndex);

         return il.After(() =>
         {
            //"move next" block
            il.MarkLabel(lMoveNext);
            il.Emit(Ldloc, enumerator.LocalIndex);  //load enumerator as an argument
            il.Emit(Callvirt, moveNextMethod);
            il.Emit(Brtrue, lWork);   //if result is true, go to lWork

            //otherwise, dispose enumerator and exit
            //il.Emit(Ldloc, enumerator.LocalIndex);
            //il.Emit(Callvirt, disposeMethod);
#if DEBUG
            il.EmitWriteLine("foreach-end");
#endif
         });
      }

      public static IDisposable ForLoop(this ILGenerator il, LocalBuilder max, out LocalBuilder loopCounter)
      {
         loopCounter = il.DeclareLocal(typeof(int)); //loop counter
#if DEBUG
         il.EmitWriteLine("for-begin");
#endif
         il.Emit(Ldc_I4_0);
         il.Emit(Stloc, loopCounter.LocalIndex);

         Label lBody = il.DefineLabel();     //loop body start
         Label lExit = il.DefineLabel();     //final and return

         il.MarkLabel(lBody); //loop body starts here

         //load counter and max and compare them
         il.Emit(Ldloc, loopCounter.LocalIndex);
         il.Emit(Ldloc, max.LocalIndex);
         //il.Emit(Ldc_I4_3);
         il.Emit(Clt);   //1 - less, 0 - equal or more (exit)
         il.Emit(Brfalse, lExit);  //jump on 1

#if DEBUG
         il.EmitWriteLine("  for-loop");
#endif

         // loop body executes here

         LocalBuilder loopCounterAfter = loopCounter;
         return il.After(() =>
         {
            //increment loop counter
            il.Emit(Ldc_I4_1);
            il.Emit(Ldloc, loopCounterAfter.LocalIndex);
            il.Emit(Add);
            il.Emit(Stloc, loopCounterAfter.LocalIndex);

            //loop again
            il.Emit(Br, lBody);

            il.MarkLabel(lExit);
#if DEBUG
            il.EmitWriteLine("for-end");
#endif
         });
      }

      private static IDisposable After(this ILGenerator thisIl, Action ilAction)
      {
         return new CodeAfter(thisIl, ilAction);
      }

      class CodeAfter : IDisposable
      {
         private readonly ILGenerator _il;
         private readonly Action _action;

         public CodeAfter(ILGenerator il, Action action)
         {
            _il = il;
            _action = action;

         }

         public void Dispose()
         {
            if (_action == null)
               return;

            _action();
         }
      }
   }
}