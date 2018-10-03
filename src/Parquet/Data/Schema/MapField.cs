using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet.Data
{
   /// <summary>
   /// Implements a dictionary field
   /// </summary>
   public class MapField : Field
   {
      internal const string ContainerName = "key_value";

      /// <summary>
      /// Data field used as a key
      /// </summary>
      public Field Key { get; private set; }

      /// <summary>
      /// Data field used as a value
      /// </summary>
      public Field Value { get; private set; }

      /// <summary>
      /// Declares a map field
      /// </summary>
      public MapField(string name, DataField keyField, DataField valueField)
         : base(name, SchemaType.Map)
      {
         Key = keyField;
         Value = valueField;

         Path = name.AddPath(ContainerName);
         Key.PathPrefix = Path;
         Value.PathPrefix = Path;
      }

      internal MapField(string name)
         : base(name, SchemaType.Map)
      {
      }

      internal override void Assign(Field se)
      {
         if(Key == null)
         {
            Key = se;
         }
         else if(Value == null)
         {
            Value = se;
         }
         else
         {
            throw new InvalidOperationException($"'{Name}' already has key and value assigned");
         }
      }

      internal override string PathPrefix
      {
         set
         {
            Path = value.AddPath(Name, ContainerName);
            Key.PathPrefix = Path;
            Value.PathPrefix = Path;
         }
      }

      internal override void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel)
      {
         int rl = parentRepetitionLevel;
         int dl = parentDefinitionLevel;

         //"container" is optional and adds on 1 DL
         dl += 1;

         //"key_value" is repeated therefore it adds on 1 RL + 1 DL
         rl += 1;
         dl += 1;

         //push to children
         Key.PropagateLevels(rl, dl);
         Value.PropagateLevels(rl, dl);
      }

      /// <summary>
      /// Creates an empty dictionary to keep values for this map field. Only works when both key and value are <see cref="DataField"/>
      /// </summary>
      /// <returns></returns>
      internal IDictionary CreateSimpleDictionary()
      {
         Type genericType = typeof(Dictionary<,>);
         Type concreteType = genericType.MakeGenericType(
            ((DataField)Key).ClrNullableIfHasNullsType,
            ((DataField)Value).ClrNullableIfHasNullsType);

         return (IDictionary)Activator.CreateInstance(concreteType);
      }

      /// <summary>
      /// <see cref="Equals(object)"/>
      /// </summary>
      public override bool Equals(object obj)
      {
         if (ReferenceEquals(obj, null)) return false;
         if (ReferenceEquals(obj, this)) return true;
         if (obj.GetType() != typeof(MapField)) return false;

         MapField other = (MapField)obj;

         return Name.Equals(other.Name) && Key.Equals(other.Key) && Value.Equals(other.Value);
      }

      /// <summary>
      /// <see cref="GetHashCode"/>
      /// </summary>
      public override int GetHashCode()
      {
         return Name.GetHashCode() * Key.GetHashCode() * Value.GetHashCode();
      }
   }
}
