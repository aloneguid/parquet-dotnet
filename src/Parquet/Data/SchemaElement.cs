using Parquet.File;
using System;
using PSE = Parquet.Thrift.SchemaElement;

namespace Parquet.Data
{
   /// <summary>
   /// Element of dataset's schema
   /// <typeparamref name="T">Type of element in the column</typeparamref>
   /// </summary>
   public class SchemaElement<T> : SchemaElement
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      public SchemaElement(string name) : base(name, typeof(T))
      {

      }
   }


   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public class SchemaElement
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="elementType">Type of the element in this column</param>
      public SchemaElement(string name, Type elementType)
      {
         if (string.IsNullOrEmpty(name))
            throw new ArgumentException("cannot be null or empty", nameof(name));

         Name = name;
         ElementType = elementType;
         Thrift = new PSE(name)
         {
            //this must be changed later or if column has nulls (on write)
            Repetition_type = Parquet.Thrift.FieldRepetitionType.REQUIRED
         };
         TypeFactory.AdjustSchema(Thrift, elementType);
      }

      internal SchemaElement(PSE thriftSchema)
      {
         Name = thriftSchema.Name;
         Thrift = thriftSchema;
         ElementType = TypeFactory.ToSystemType(thriftSchema);
      }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; }

      /// <summary>
      /// Element type
      /// </summary>
      public Type ElementType { get; }

      /// <summary>
      /// Returns true if element can have null values
      /// </summary>
      public bool IsNullable
      {
         get { return Thrift.Repetition_type != Parquet.Thrift.FieldRepetitionType.REQUIRED; }
         set { Thrift.Repetition_type = value ? Parquet.Thrift.FieldRepetitionType.OPTIONAL : Parquet.Thrift.FieldRepetitionType.REQUIRED; }
      }

      internal PSE Thrift { get; set; }

      internal bool IsAnnotatedWith(Thrift.ConvertedType ct)
      {
         //checking __isset is important, this is a way of Thrift to tell whether the variable is set at all
         return Thrift.__isset.converted_type && Thrift.Converted_type == ct;
      }

      /// <summary>
      /// Pretty prints
      /// </summary>
      public override string ToString()
      {
         return $"{Name} ({ElementType}), nullable: {IsNullable}";
      }
   }
}