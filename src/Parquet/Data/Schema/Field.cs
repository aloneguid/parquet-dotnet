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
      /// Path
      /// </summary>
      public string Path { get; set; }

      internal virtual string PathPrefix { set { } }

      /// <summary>
      /// Constructs a field with only requiremd parameters
      /// </summary>
      /// <param name="name">Field name, required</param>
      /// <param name="schemaType">Type of schema of this field</param>
      protected Field(string name, SchemaType schemaType)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));

         if(Name.Contains(Schema.PathSeparator))
         {
            throw new ArgumentException($"'{Schema.PathSeparator}' is not allowed in field name as it's used internally as path separator");
         }

         SchemaType = schemaType;
         Path = name;
      }

      #region [ Internal Helpers ]

      internal virtual void Assign(Field field)
      {
         //only used by some schema fields internally to help construct a field hierarchy
      }

      #endregion
   }
}