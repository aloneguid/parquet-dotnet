using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data
{
   /// <summary>
   /// Represents a list of items. The list can contain either a normal data field or a complex structure.
   /// If you need to get a list of primitive data fields it's more efficient to use arrays.
   /// </summary>
   public class ListField : Field
   {
      /// <summary>
      /// Item contained within this list
      /// </summary>
      public Field Item { get; internal set; }

      public ListField(string name, Field item) : this(name)
      {
         Item = item ?? throw new ArgumentNullException(nameof(item));
      }

      private ListField(string name) : base(name, SchemaType.List)
      {
      }

      public override string ToString()
      {
         return $"list of ({Item})";
      }

      internal static ListField CreateWithNoItem(string name)
      {
         return new ListField(name);
      }

      internal override void Assign(Field field)
      {
         if(Item != null)
         {
            throw new InvalidOperationException($"item was already assigned to this list ({Name}), somethin is terribly wrong because a list can only have one item.");
         }

         Item = field ?? throw new ArgumentNullException(nameof(field));
      }
   }
}
