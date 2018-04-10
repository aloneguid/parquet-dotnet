using System;
using System.Collections.Generic;
using System.Text;
using FB = org.apache.arrow.flatbuf;

namespace SharpArrow.Data
{
   public class Field
   {
      internal Field(FB.Field fbField)
      {
         Name = fbField.Name;
         IsNullable = fbField.Nullable;
         Type = (ArrowType)(byte)fbField.TypeType;
         FB.KeyValue kv = fbField.CustomMetadata(0).GetValueOrDefault();
      }

      public string Name { get; private set; }

      public bool IsNullable { get; private set; }

      public ArrowType Type { get; private set; }

      public override string ToString()
      {
         return Name;
      }
   }
}
