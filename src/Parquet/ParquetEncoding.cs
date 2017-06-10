using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet
{
    public class ParquetEncoding
    {
       public T ReadPlain<T>(byte[] parquetBlock, ParquetTypes.Type type, int count, int width = 0)
       {
         // This is a poor implementation we have the width so we should walk the array - boxing/unboxing and casting ugly, slow and bloaty!!
          switch (type)
          {
            case ParquetTypes.Type.Int32:
               return (T) (object) Int32.Parse(System.Text.Encoding.UTF8.GetString(parquetBlock));
            case ParquetTypes.Type.Int64:
                return (T)(object) Int64.Parse(System.Text.Encoding.UTF8.GetString(parquetBlock));
            case ParquetTypes.Type.Int96:
               return (T)(object) Decimal.Parse(System.Text.Encoding.UTF8.GetString(parquetBlock));
             case ParquetTypes.Type.Float:
                return (T)(object) Single.Parse(System.Text.Encoding.UTF8.GetString(parquetBlock));
             case ParquetTypes.Type.Double:
                return (T)(object) Double.Parse(System.Text.Encoding.UTF8.GetString(parquetBlock));
             case ParquetTypes.Type.Boolean:
                return (T) (object) new BitArray(parquetBlock).ConvertToBoolArray(count);
            default:
               return default(T);
         }
       }

      
   }
}
