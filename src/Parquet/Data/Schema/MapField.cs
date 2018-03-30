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
      internal const string _containerName = "key_value";

      public DataField Key { get; private set; }

      public DataField Value { get; private set; }

      /// <summary>
      /// 
      /// </summary>
      public DataType KeyType => Key.DataType;

      /// <summary>
      /// 
      /// </summary>
      public DataType ValueType => Value.DataType;

      /// <summary>
      /// Declares a map field
      /// </summary>
      public MapField(string name, DataField keyField, DataField valueField)
         : base(name, SchemaType.Map)
      {
         Key = keyField;
         Value = valueField;

         Path = name.AddPath(_containerName);
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
            Key = (DataField)se;
         }
         else if(Value == null)
         {
            Value = (DataField)se;
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
            Path = value.AddPath(Name, _containerName);
            Key.PathPrefix = Path;
            Value.PathPrefix = Path;
         }
      }

      internal IDictionary CreateCellValue(IDictionary<string, IList> pathToValues, int index)
      {
         IList keys = (IList)(pathToValues[Key.Path][index]);
         IList values = (IList)(pathToValues[Value.Path][index]);

         Type gt = typeof(Dictionary<,>);
         Type masterType = gt.MakeGenericType(Key.ClrType, Value.ClrType);
         IDictionary result = (IDictionary)Activator.CreateInstance(masterType);

         for (int i = 0; i < keys.Count; i++)
         {
            result.Add(keys[i], values[i]);
         }

         return result;
      }

      internal void AddElement(IList keys, IList values, IDictionary dictionary)
      {
         IDataTypeHandler keyHandler = DataTypeFactory.Match(Key.DataType);
         IDataTypeHandler valueHandler = DataTypeFactory.Match(Value.DataType);

         IList keysList = keyHandler.CreateEmptyList(Key.HasNulls, false, dictionary.Count);
         IList valuesList = valueHandler.CreateEmptyList(Value.HasNulls, false, dictionary.Count);

         foreach (object v in dictionary.Keys) keysList.Add(v);
         foreach (object v in dictionary.Values) valuesList.Add(v);

         keys.Add(keysList);
         values.Add(valuesList);
      }

      /// <summary>
      /// <see cref="Equals(object)"/>
      /// </summary>
      public override bool Equals(Object obj)
      {
         MapField other = (MapField)obj;
         return Name.Equals(other.Name) && KeyType.Equals(other.KeyType) && ValueType.Equals(other.ValueType);
      }

      /// <summary>
      /// <see cref="GetHashCode"/>
      /// </summary>
      public override int GetHashCode()
      {
         return Name.GetHashCode() * KeyType.GetHashCode() * ValueType.GetHashCode();
      }
   }
}
