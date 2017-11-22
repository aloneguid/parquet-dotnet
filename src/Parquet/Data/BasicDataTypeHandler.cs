using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using Parquet.File;

namespace Parquet.Data
{

   abstract class BasicDataTypeHandler<TSystemType> : IDataTypeHandler
   {
      private readonly Thrift.Type _thriftType;
      private readonly Thrift.ConvertedType? _convertedType;
      private readonly int? _bitWidth;

      public BasicDataTypeHandler(DataType dataType, Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null)
      {
         DataType = dataType;
         _thriftType = thriftType;
         _convertedType = convertedType;
      }

      public DataType DataType { get; private set; }

      public SchemaType SchemaType => SchemaType.PrimitiveType;

      public Type ClrType => typeof(TSystemType);

      public virtual bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            (tse.__isset.type && _thriftType == tse.Type) &&
            (_convertedType == null || (tse.__isset.converted_type && tse.Converted_type == _convertedType.Value));
      }

      public virtual Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount)
      {
         Thrift.SchemaElement tse = schema[index++];

         bool hasNulls = (tse.Repetition_type != Thrift.FieldRepetitionType.REQUIRED);
         bool isArray = (tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED);

         Field simple = CreateSimple(tse, hasNulls, isArray);
         ownedChildCount = 0;
         return simple;
      }

      protected virtual DataField CreateSimple(Thrift.SchemaElement tse, bool hasNulls, bool isArray)
      {
         return new DataField(tse.Name, DataType, hasNulls, isArray);
      }

      public abstract IList CreateEmptyList(bool isNullable, bool isArray, int capacity);

      public virtual IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         int totalLength = (int)reader.BaseStream.Length;

         //create list with effective capacity
         //int capacity = (int)((reader.BaseStream.Position - totalLength) / _typeWidth);
         int capacity = 0;
         IList result = CreateEmptyList(tse.IsNullable(), false, capacity);

         Stream s = reader.BaseStream;
         try
         {
            while (s.Position < totalLength)
            {
               TSystemType element = ReadOne(reader);
               result.Add(element);
            }
         }
         catch(EndOfStreamException)
         {
            //that's fine to hit the end of stream as many types are longer than one byte
            throw;
         }

         return result;
      }

      public virtual void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         foreach(TSystemType one in values)
         {
            WriteOne(writer, one);
         }
      }

      public virtual void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         DataField sef = (DataField)se;
         var tse = new Thrift.SchemaElement(se.Name);
         tse.Type = _thriftType;
         if (_convertedType != null) tse.Converted_type = _convertedType.Value;
         tse.Repetition_type = sef.IsArray
            ? Thrift.FieldRepetitionType.REPEATED
            : (sef.HasNulls ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED);
         container.Add(tse);
         parent.Num_children += 1;
      }

      #region [ Reader / Writer Helpers ]

      protected virtual TSystemType ReadOne(BinaryReader reader)
      {
         throw new NotSupportedException();
      }

      protected virtual void WriteOne(BinaryWriter writer, TSystemType value)
      {
         throw new NotSupportedException();
      }

      #endregion

   }
}
