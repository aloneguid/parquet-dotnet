using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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

      public static IDisposable ForEachLoopFromEnumerator(this ILGenerator il, Type elementType, LocalBuilder enumerator, out LocalBuilder currentElement)
      {
         Label lMoveNext = il.DefineLabel();
         Label lWork = il.DefineLabel();
         currentElement = il.DeclareLocal(elementType);

#if DEBUG
         il.EmitWriteLine("foreach-begin");
#endif

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
            il.Increment(loopCounterAfter);

            //loop again
            il.Emit(Br, lBody);

            il.MarkLabel(lExit);
#if DEBUG
            il.EmitWriteLine("for-end");
#endif
         });
      }

      public static void CreateGenericList(this ILGenerator il, Type elementType)
      {
         List<ConstructorInfo> constructors = typeof(List<>).MakeGenericType(elementType).GetTypeInfo().DeclaredConstructors.ToList();

         il.Emit(Newobj, constructors[0]);
      }

      public static void LdLoc(this ILGenerator il, LocalBuilder local)
      {
         il.Emit(Ldloc, local.LocalIndex);
      }

      public static void StLoc(this ILGenerator il, LocalBuilder local)
      {
         il.Emit(Stloc, local.LocalIndex);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="il"></param>
      /// <param name="method"></param>
      /// <param name="parameters">List of parameters, OpCode and LocalBuilder are supported</param>
      public static void CallVirt(this ILGenerator il, MethodInfo method, params object[] parameters)
      {
         if(parameters != null)
         {
            foreach(object param in parameters)
            {
               switch (param)
               {
                  case OpCode oc:
                     il.Emit(oc);
                     break;
                  case LocalBuilder lb:
                     il.Emit(Ldloc, lb.LocalIndex);
                     break;
                  default:
                     throw new NotSupportedException("don't know how to treat parameter " + param?.GetType());
               }
            }
         }

         il.Emit(Callvirt, method);
      }

      public static void GetArrayLength(this ILGenerator il, OpCode loadArrayCode, LocalBuilder lengthReceiver)
      {
         il.Emit(loadArrayCode);
         il.Emit(Ldlen);
         il.Emit(Conv_I4);
         il.StLoc(lengthReceiver);
      }

      public static void GetArrayLength(this ILGenerator il, LocalBuilder array, LocalBuilder lengthReceiver)
      {
         il.Emit(Ldloc, array.LocalIndex);
         il.Emit(Ldlen);
         il.Emit(Conv_I4);
         il.StLoc(lengthReceiver);
      }

      public static void GetArrayElement(this ILGenerator il, OpCode loadArrayCode, LocalBuilder index, bool loadByRef, Type elementType, LocalBuilder elementReceiver)
      {
         //load data element
         il.Emit(loadArrayCode);  //data array
         il.Emit(Ldloc, index.LocalIndex);  //index
         if (loadByRef)
         {
            il.Emit(Ldelem_Ref);
         }
         else
         {
            il.Emit(Ldelem, elementType);
         }
         il.Emit(Stloc, elementReceiver.LocalIndex);  //data value
      }

      public static void GetArrayElement(this ILGenerator il, LocalBuilder array, LocalBuilder index, bool loadByRef, Type elementType, LocalBuilder elementReceiver)
      {
         //load data element
         il.Emit(Ldloc, array.LocalIndex);  //data array
         il.Emit(Ldloc, index.LocalIndex);  //index
         if (loadByRef)
         {
            il.Emit(Ldelem_Ref);
         }
         else
         {
            il.Emit(Ldelem, elementType);
         }
         il.Emit(Stloc, elementReceiver.LocalIndex);  //data value
      }

      public static void Increment(this ILGenerator il, LocalBuilder intItem)
      {
         //increment loop counter
         il.Emit(Ldc_I4_1);
         il.Emit(Ldloc, intItem.LocalIndex);
         il.Emit(Add);
         il.Emit(Stloc, intItem.LocalIndex);
      }

      private static IDisposable After(this ILGenerator thisIl, Action ilAction)
      {
         return new CodeAfter(thisIl, ilAction);
      }

      public static bool Matches(this ParameterInfo[] parameters, Type[] types)
      {
         if (parameters.Length != types.Length)
         {
            return false;
         }
         for (int i = 0; i < parameters.Length; i++)
         {
            if (parameters[i].ParameterType != types[i])
            {
               return false;
            }
         }
         return true;
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