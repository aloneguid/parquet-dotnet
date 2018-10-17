using System;
using System.Collections.Generic;
using System.Text;
using FB = org.apache.arrow.flatbuf;

namespace SharpArrow.Data
{
   public class Schema
   {
      private readonly List<Field> _fields = new List<Field>();

      internal Schema(FB.Schema fbSchema)
      {
         ReadFields(fbSchema);
      }

      private void ReadFields(FB.Schema fbSchema)
      {
         for (int i = 0; i < fbSchema.FieldsLength; i++)
         {
            FB.Field field = fbSchema.Fields(i).GetValueOrDefault();

            for (int mi = 0; mi < field.CustomMetadataLength; mi++)
            {
               FB.KeyValue mkv = field.CustomMetadata(mi).GetValueOrDefault();
            }

            _fields.Add(new Field(field));
         }
      }

      public Field[] Fields => _fields.ToArray();
   }
}