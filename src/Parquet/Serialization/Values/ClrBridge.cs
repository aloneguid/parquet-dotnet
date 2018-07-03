using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Parquet.Data;

namespace Parquet.Serialization.Values
{
   //todo: incorporate IL emit on top of this progressively
   class ClrBridge
   {
      private readonly Type _classType;

      public ClrBridge(Type classType)
      {
         _classType = classType;
      }

      public DataColumn BuildColumn(DataField field, IEnumerable classInstances, int classInstancesCount)
      {
         PropertyInfo pi = null;
         int idx = 0;
         Array data = Array.CreateInstance(field.ClrNullableIfHasNullsType, classInstancesCount);

         foreach (object instance in classInstances)
         {
            AddValue(ref pi, ref idx, instance, field, data);
         }

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
   }
}
