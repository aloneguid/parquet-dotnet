/*using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using System.Reflection;
using System.Reflection.Emit;
using static System.Reflection.Emit.OpCodes;

namespace Parquet.Serialization.Values
{
   /// <summary>
   /// Generates IL for extracting data from columns.
   /// </summary>
   class ColumnValuesILExtractor : IColumnValuesExtractor
   {
      private delegate int ExtractColumns(object classInstance, List<DataField> columns);

      private static readonly Dictionary<KeyValuePair<Type, Schema>, ExtractColumns> TagToDelegate =
         new Dictionary<KeyValuePair<Type, Schema>, ExtractColumns>();

      public void ExtractToList(Type classType, IEnumerable classInstances, Schema flatSchema, List<DataField> columns)
      {
         ExtractColumns extract = GetExtractionDelegate(classType, flatSchema, columns);

         foreach(object classIntance in classInstances)
         {
            extract(classIntance, columns);
         }
      }

      private ExtractColumns GetExtractionDelegate(Type classType, Schema flatSchema, List<DataField> columns)
      {
         var key = new KeyValuePair<Type, Schema>(classType, flatSchema);

         if (TagToDelegate.TryGetValue(key, out ExtractColumns extractColumns))
         {
            return extractColumns;
         }

         extractColumns = BuildExtractMethod(classType, flatSchema, columns);
         TagToDelegate[key] = extractColumns;
         return extractColumns;
      }

      private ExtractColumns BuildExtractMethod(Type classType, Schema flatSchema, List<DataField> columns)
      {
         Type[] methodArgs = { typeof(object), typeof(List<IList>) };

         var squareIt = new DynamicMethod(
            $"ExtractColumns-{Guid.NewGuid()}",
            typeof(int),
            methodArgs,
            typeof(ColumnValuesILExtractor).GetTypeInfo().Module);

         ILGenerator il = squareIt.GetILGenerator();

         //generate local variable per schema field
         int i = 0;
         foreach(Field field in flatSchema.Fields)
         {
            GenerateLocal(il, field, i++);
         }

         il.DeclareLocal(typeof(int));	//declare local variable (index: 0)
         il.DeclareLocal(typeof(string));

         i = 0;
         foreach(Field field in flatSchema.Fields)
         {
            GenerateValueGetter(classType, il, field, i++);
         }

         i = 0;
         MethodInfo getListItemMethod = typeof(List<IList>)
            .GetTypeInfo()
            .GetDeclaredProperty("Item")
            .GetMethod;
         foreach(Field field in flatSchema.Fields)
         {
            GenerateListAdd(il, field, columns[i], getListItemMethod, i++);
         }

         //return fake value for now
         //todo: return number of fields added
         il.Emit(Ldc_I4_0);
         il.Emit(Ret);

         return (ExtractColumns)squareIt.CreateDelegate(typeof(ExtractColumns));
      }

      private void GenerateLocal(ILGenerator il, Field field, int i)
      {
         DataField dataField = (DataField)field;
         il.DeclareLocal(dataField.ClrType);
      }

      private void GenerateValueGetter(Type classType, ILGenerator il, Field f, int i)
      {
         //get the method which gets property value from class instance
         MethodInfo getValueMethod = classType.GetTypeInfo().GetDeclaredProperty(f.ClrPropName ?? f.Name).GetMethod;

         //get property value first and store in local variable
         il.Emit(Ldarg_0);
         il.Emit(Callvirt, getValueMethod);
         il.Emit(Stloc, i);
      }

      private void GenerateListAdd(ILGenerator il, Field field, IList valuesList, MethodInfo getListItemMethod, int i)
      {
         //get the generic method which adds values to the generic list
         MethodInfo addMethod = valuesList
            .GetType()
            .GetTypeInfo()
            .GetDeclaredMethod("Add");

         il.Emit(Ldarg_1);    //loads List<IList>
         il.Emit(Ldc_I4, i);  //list index
         il.Emit(Callvirt, getListItemMethod);

         il.Emit(Ldloc, i);   //load value
         il.Emit(Callvirt, addMethod);
      }
   }
}
*/