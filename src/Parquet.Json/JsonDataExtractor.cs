using Newtonsoft.Json.Linq;
using Parquet.Data;

namespace Parquet.Json
{
   class JsonDataExtractor
   {
      private readonly Schema _schema;

      public JsonDataExtractor(Schema schema)
      {
         _schema = schema;
      }

      public void AddRow(JObject jo)
      {
      }

      private string GetJsonPath(Field field)
      {
         return null;
      }
   }
}
