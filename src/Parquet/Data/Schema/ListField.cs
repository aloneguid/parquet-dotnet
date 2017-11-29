using System;

namespace Parquet.Data
{
   /// <summary>
   /// Represents a list of items. The list can contain either a normal data field or a complex structure.
   /// If you need to get a list of primitive data fields it's more efficient to use arrays.
   /// </summary>
   public class ListField : Field, IEquatable<ListField>
   {
      internal const string ContainerName = "list";

      /// <summary>
      /// Item contained within this list
      /// </summary>
      public Field Item { get; internal set; }

      public ListField(string name, Field item) : this(name)
      {
         Item = item ?? throw new ArgumentNullException(nameof(item));
         PathPrefix = null;
      }

      private ListField(string name) : base(name, SchemaType.List)
      {
      }

      internal override string PathPrefix
      {
         set
         {
            Path = value.AddPath(Name, ContainerName);
            Item.PathPrefix = Path;
         }
      }

      public override string ToString()
      {
         return $"{Name}: ({Item})";
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

      public bool Equals(ListField other)
      {
         if (ReferenceEquals(null, other)) return false;
         if (ReferenceEquals(this, other)) return true;

         return Name.Equals(other.Name) && Item.Equals(other.Item);
      }

      public override bool Equals(object obj)
      {
         if (ReferenceEquals(null, obj)) return false;
         if (ReferenceEquals(this, obj)) return true;
         if (obj.GetType() != GetType()) return false;

         return Equals((ListField)obj);
      }

      public override int GetHashCode()
      {
         return Name.GetHashCode() * Item.GetHashCode();
      }
   }
}
