using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   class TreeList
   {
      private readonly TreeList _parent;
      private List<TreeList> _children;
      private List<object> _values;

      public TreeList(TreeList parent)
      {
         _parent = parent;
      }

      public bool HasValues => _values != null && _values.Count > 0;

      public TreeList FirstChild => (_children == null || _children.Count == 0) ? null : _children[0];

      public TreeList Submerge(int depth)
      {
         if (depth == 0)
            return this;

         if(depth > 0)
         {
            TreeList result = this;
            while (depth-- > 0)
            {
               var next = new TreeList(result);
               if (result._children == null)
                  result._children = new List<TreeList>();
               if(result._values != null)
               {
                  foreach(object v in result._values)
                  {
                     next.Add(v);
                  }
                  result._values = null;
               }
               result._children.Add(next);
               result = next;

            }
            return result;
         }

         TreeList r2 = this;
         while(depth++ < 0)
         {
            r2 = r2._parent;
         }

         return r2;
      }

      public void Add(object value)
      {
         if(_values == null)
         {
            _values = new List<object>();
         }
         _values.Add(value);
      }

      public object Compact(Type clrType)
      {
         if(_values != null)
         {
            return ValuesAs(clrType);
         }

         return new List<object>(_children.Select(c => c.Compact(clrType)));
      }

      public object ValuesAs(Type clrType)
      {
         Array cellArray = Array.CreateInstance(clrType, _values.Count);
         for (int i = 0; i < _values.Count; i++)
         {
            cellArray.SetValue(_values[i], i);
         }
         return cellArray;
      }

      public object FinalValue(Type clrType)
      {
         return FirstChild == null
            ? ((_values != null && _values[0] != null) ? ValuesAs(clrType) : null)
            : FirstChild?.Compact(clrType);
      }
   }
}
