using System;

namespace Parquet.Data
{

   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public abstract class Field
   {
      /// <summary>
      /// Type of schema in this field
      /// </summary>
      public SchemaType SchemaType { get; }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; private set; }

      /// <summary>
      /// Only used internally!
      /// </summary>
      internal string Path { get; set; }

      public Field(string name, SchemaType schemaType)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));
         SchemaType = schemaType;
      }



      #region [ Internal Helpers ]

      internal virtual void Assign(Field se)
      {

      }

      #endregion
   }
}