using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.DataTypes;

namespace Parquet.File
{
   class PrimitiveReader<TElement>
      where TElement : struct
   {
      private readonly Thrift.SchemaElement _schemaElement;
      private readonly ParquetOptions _parquetOptions;
      private readonly IDataTypeHandler _dataTypeHandler;
      private readonly BinaryReader _binaryReader;
      private readonly int _typeWidth;
      private readonly Func<BinaryReader, TElement> _readOneFunc;

      public PrimitiveReader(
         Thrift.SchemaElement schemaElement,
         ParquetOptions parquetOptions,
         IDataTypeHandler dataTypeHandler,
         BinaryReader binaryReader,
         int typeWidth,
         Func<BinaryReader, TElement> readOneFunc)
      {
         _schemaElement = schemaElement;
         _parquetOptions = parquetOptions;
         _dataTypeHandler = dataTypeHandler;
         _binaryReader = binaryReader;
         _typeWidth = typeWidth;
         _readOneFunc = readOneFunc;
      }

      public IList ReadAll()
      {
         int totalLength = (int)_binaryReader.BaseStream.Length;

         //create list with effective capacity
         int capacity = (int)((_binaryReader.BaseStream.Position - totalLength) / _typeWidth);
         IList result = _dataTypeHandler.CreateEmptyList(_schemaElement, _parquetOptions, capacity);

         if(_schemaElement.IsNullable())
         {
            ReadNullable((List<TElement?>)result, totalLength);
         }
         else
         {
            ReadNonNullable((List<TElement>)result, totalLength);
         }

         return result;
      }

      private void ReadNullable(List<TElement?> result, int totalStreamLength)
      {
         while(_binaryReader.BaseStream.Position + _typeWidth <= totalStreamLength)
         {
            TElement element = _readOneFunc(_binaryReader);
            result.Add(element);
         }
      }

      private void ReadNonNullable(List<TElement> result, int totalStreamLength)
      {
         while (_binaryReader.BaseStream.Position + _typeWidth <= totalStreamLength)
         {
            TElement element = _readOneFunc(_binaryReader);
            result.Add(element);
         }
      }

   }
}
