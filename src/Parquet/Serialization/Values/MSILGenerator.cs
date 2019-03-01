using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using static System.Reflection.Emit.OpCodes;

namespace Parquet.Serialization.Values
{
   class MSILGenerator
   {
      private static readonly TypeConversion[] conversions = new TypeConversion[]
      {
         DateTimeToDateTimeOffsetConversion.Instance,
         DateTimeOffsetToDateTimeConversion.Instance,
         NullableDateTimeToDateTimeOffsetConversion.Instance,
         NullableDateTimeOffsetToDateTimeConversion.Instance,
      };

      public delegate object PopulateListDelegate(object instances,
         object resultItemsList,
         object resultRepetitionsList,
         int maxRepetitionLevel);

      public delegate void AssignArrayDelegate(
         DataColumn column,
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

         TypeConversion conversion = GetConversion(pi.PropertyType, field.ClrNullableIfHasNullsType);

         ILGenerator il = runMethod.GetILGenerator();

         GenerateCollector(il, classType,
            field,
            getValueMethod,
            addToListMethod,
            addRepLevelMethod,
            conversion);

         return (PopulateListDelegate)runMethod.CreateDelegate(typeof(PopulateListDelegate));
      }

      private void GenerateCollector(ILGenerator il, Type classType,
         DataField f,
         MethodInfo getValueMethod,
         MethodInfo addToListMethod,
         MethodInfo addRepLevelMethod,
         TypeConversion conversion)
      {
         //arg 0 - collection of classes, clr objects
         //arg 1 - data items (typed list)
         //arg 2 - repetitions (optional)
         //arg 3 - max repetition level

         //make collection a local variable
         LocalBuilder collection = il.DeclareLocal(classType);
         il.Emit(Ldarg_0);
         il.Emit(Stloc, collection.LocalIndex);

         using (il.ForEachLoop(classType, collection, out LocalBuilder currentElement))
         {
            if (f.IsArray)
            {
               //reset repetition level to 0
               LocalBuilder rl = il.DeclareLocal(typeof(int));
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
               //hold item
               LocalBuilder item = il.DeclareLocal(f.ClrNullableIfHasNullsType);

               //get current value, converting if necessary
               il.Emit(Ldloc, currentElement.LocalIndex);
               il.Emit(Callvirt, getValueMethod);
               if (conversion != null)
               {
                  conversion.Emit(il);
               }
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

      public AssignArrayDelegate GenerateAssigner(DataColumn dataColumn, Type classType)
      {
         DataField field = dataColumn.Field;

         Type[] methodArgs = { typeof(DataColumn), typeof(Array) };
         var runMethod = new DynamicMethod(
            $"Set{classType.Name}{field.Name}",
            typeof(void),
            methodArgs,
            GetType().GetTypeInfo().Module);

         ILGenerator il = runMethod.GetILGenerator();

         //set class property method
         TypeInfo ti = classType.GetTypeInfo();
         PropertyInfo pi = ti.GetDeclaredProperty(field.ClrPropName ?? field.Name);
         MethodInfo setValueMethod = pi.SetMethod;

         TypeInfo dcti = dataColumn.GetType().GetTypeInfo();
         MethodInfo getDataMethod = dcti.GetDeclaredProperty(nameof(DataColumn.Data)).GetMethod;
         MethodInfo getRepsMethod = dcti.GetDeclaredProperty(nameof(DataColumn.RepetitionLevels)).GetMethod;

         TypeConversion conversion = GetConversion(dataColumn.Field.ClrNullableIfHasNullsType, pi.PropertyType);

         GenerateAssigner(il, classType, field,
            setValueMethod,
            getDataMethod,
            getRepsMethod,
            conversion);

         return (AssignArrayDelegate)runMethod.CreateDelegate(typeof(AssignArrayDelegate));
      }

      private void GenerateAssigner(ILGenerator il, Type classType, DataField field,
         MethodInfo setValueMethod,
         MethodInfo getDataMethod,
         MethodInfo getRepsMethod,
         TypeConversion conversion)
      {
         //arg 0 - DataColumn
         //arg 1 - class intances array (Array)

         if (field.IsArray)
         {
            LocalBuilder repItem = il.DeclareLocal(typeof(int));
            LocalBuilder dce = il.DeclareLocal(typeof(DataColumnEnumerator));

            //we will use DataColumnEnumerator for complex types

            //create an instance of it
            il.Emit(Ldarg_0); //constructor argument
            il.Emit(Newobj, typeof(DataColumnEnumerator).GetTypeInfo().DeclaredConstructors.First());
            il.StLoc(dce);

            LocalBuilder ci = il.DeclareLocal(typeof(int)); //class index
            LocalBuilder classInstance = il.DeclareLocal(classType); //class instance

            using (il.ForEachLoopFromEnumerator(typeof(object), dce, out LocalBuilder element))
            {
               //element should be an array for simple repeatables

               //get class instance by index
               il.GetArrayElement(Ldarg_1, ci, true, typeof(Array), classInstance);

               //assign data item to class property
               il.CallVirt(setValueMethod, classInstance, element);

               il.Increment(ci);
            }
         }
         else
         {
            //get values
            LocalBuilder data = il.DeclareLocal(typeof(Array));
            il.CallVirt(getDataMethod, Ldarg_0);
            il.StLoc(data);

            //get length of values
            LocalBuilder dataLength = il.DeclareLocal(typeof(int));
            il.GetArrayLength(data, dataLength);

            LocalBuilder dataItem = il.DeclareLocal(field.ClrNullableIfHasNullsType);  //current value
            bool dataIsRef = field.HasNulls && !field.ClrNullableIfHasNullsType.IsSystemNullable();

            LocalBuilder classInstance = il.DeclareLocal(classType);

            using (il.ForLoop(dataLength, out LocalBuilder iData))
            {
               //get data value
               il.GetArrayElement(data, iData, dataIsRef, field.ClrNullableIfHasNullsType, dataItem);

               //get class instance
               il.GetArrayElement(Ldarg_1, iData, true, classType, classInstance);

               il.LdLoc(classInstance);

               //convert if necessary
               if (conversion != null)
               {
                  il.LdLoc(dataItem);
                  conversion.Emit(il);
               }
               else
               {
                  il.Emit(Ldloc, dataItem.LocalIndex);
               }

               //assign data item to class property
               il.Emit(Callvirt, setValueMethod);
            }
         }

         il.Emit(Ret);
      }

      private TypeConversion GetConversion(Type fromType, Type toType)
      {
         if (fromType == toType) { return null; }
         return conversions.FirstOrDefault(c => c.FromType == fromType && c.ToType == toType);
      }

      private abstract class TypeConversion
      {
         public abstract Type FromType { get; }
         public abstract Type ToType { get; }
         public abstract void Emit(ILGenerator il);
      }

      private abstract class TypeConversion<TFrom, TTo> : TypeConversion
      {
         public override Type FromType { get { return typeof(TFrom); } }
         public override Type ToType { get { return typeof(TTo); } }
      }

      sealed private class DateTimeToDateTimeOffsetConversion : TypeConversion<DateTime, DateTimeOffset>
      {
         public static readonly TypeConversion<DateTime, DateTimeOffset> Instance = new DateTimeToDateTimeOffsetConversion();

         private static readonly ConstructorInfo method = typeof(DateTimeOffset).GetTypeInfo().DeclaredConstructors
            .First(c => c.GetParameters().Matches(new[] { typeof(DateTime) }));

         public override void Emit(ILGenerator il)
         {
            il.Emit(OpCodes.Newobj, method);
         }
      }

      sealed class DateTimeOffsetToDateTimeConversion : TypeConversion<DateTimeOffset, DateTime>
      {
         public static readonly TypeConversion<DateTimeOffset, DateTime> Instance = new DateTimeOffsetToDateTimeConversion();

         private static readonly MethodInfo method = typeof(ConversionHelpers).GetTypeInfo().GetDeclaredMethod("DateTimeFromDateTimeOffset");

         public override void Emit(ILGenerator il)
         {
            il.Emit(OpCodes.Call, method);
         }
      }

      sealed private class NullableDateTimeToDateTimeOffsetConversion : TypeConversion<DateTime?, DateTimeOffset?>
      {
         public static readonly TypeConversion<DateTime?, DateTimeOffset?> Instance = new NullableDateTimeToDateTimeOffsetConversion();

         private static readonly MethodInfo method = typeof(ConversionHelpers).GetTypeInfo().GetDeclaredMethod("NullableDateTimeOffsetFromDateTime");

         public override void Emit(ILGenerator il)
         {
            il.Emit(OpCodes.Call, method);
         }
      }

      sealed class NullableDateTimeOffsetToDateTimeConversion : TypeConversion<DateTimeOffset?, DateTime?>
      {
         public static readonly TypeConversion<DateTimeOffset?, DateTime?> Instance = new NullableDateTimeOffsetToDateTimeConversion();

         private static readonly MethodInfo method = typeof(ConversionHelpers).GetTypeInfo().GetDeclaredMethod("NullableDateTimeFromDateTimeOffset");

         public override void Emit(ILGenerator il)
         {
            il.Emit(OpCodes.Call, method);
         }
      }
   }

   /// <summary>
   /// This class is public to simplify use from Reflection
   /// </summary>
   public static class ConversionHelpers
   {
      /// <summary>
      /// Convert DateTimeOffset to DateTime
      /// </summary>
      /// <param name="value">DateTimeOffset</param>
      /// <returns>DateTime</returns>
      public static DateTime DateTimeFromDateTimeOffset(DateTimeOffset value)
      {
         return value.DateTime;
      }

      /// <summary>
      /// Convert DateTimeOffset? to DateTime?
      /// </summary>
      /// <param name="value">DateTimeOffset?</param>
      /// <returns>DateTime?</returns>
      public static DateTime? NullableDateTimeFromDateTimeOffset(DateTimeOffset? value)
      {
         return value?.DateTime;
      }

      /// <summary>
      /// Convert DateTime? to DateTimeOffset?
      /// </summary>
      /// <param name="value">DateTime?</param>
      /// <returns>DateTimeOffset?</returns>
      public static DateTimeOffset? NullableDateTimeOffsetFromDateTime(DateTime? value)
      {
         if (value == null) { return null; }
         return new DateTimeOffset(value.Value);
      }
   }
}
