using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;

namespace Parquet
{
    class ParquetEncoding
    {
       public T ReadPlain<T>(byte[] parquetBlock, ParquetTypes.Type type, int count, int width = 0)
       {
         // This is a poor implementation we have the width so we should walk the array - boxing/unboxing and casting ugly, slow and bloaty!!
          switch (type)
          {
            case ParquetTypes.Type.Int32:
               return (T) (object) BitConverter.ToInt32(parquetBlock, 0);
            case ParquetTypes.Type.Int64:
                return (T)(object) BitConverter.ToInt64(parquetBlock, 0);
            case ParquetTypes.Type.Int96:
<<<<<<< HEAD
               return (T)(object) new BigInteger(parquetBlock);
=======
               return (T)(object) Decimal.Parse(System.Text.Encoding.UTF8.GetString(parquetBlock));
>>>>>>> a44d614d3090aac60225a52446d138e4948d7b45
             case ParquetTypes.Type.Float:
                return (T)(object) BitConverter.ToSingle(parquetBlock, 0);
             case ParquetTypes.Type.Double:
                return (T) (object) BitConverter.ToDouble(parquetBlock, 0);
             case ParquetTypes.Type.Boolean:
                return (T) (object) new BitArray(parquetBlock).ConvertToBoolArray(count);
            default:
               return default(T);
         }
       }

      
   }
}
