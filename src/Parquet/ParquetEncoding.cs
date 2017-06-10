using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet
{
    public class ParquetEncoding
    {
       public T ReadPlain<T>(byte[] parquetBlock, ParquetTypes.Type type, int count, int width = 0)
       {
          switch (type)
          {
            case ParquetTypes.Type.Int32:
               return (T)(object)Convert.ToInt32(parquetBlock[0]);
            case ParquetTypes.Type.Int64:
                return (T)(object)Convert.ToInt64(parquetBlock[0]);
            case ParquetTypes.Type.Int96:
               return (T)(object)Convert.ToInt64(parquetBlock[0]);
            default:
               return default(T);
         }
       }
    }
}
