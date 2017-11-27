using System.Linq;

namespace Parquet.Data.Predicates
{
   static class PredicateFactory
   {
      public static FieldPredicate[] CreateFieldPredicates(ReaderOptions options)
      {
         if (options == null || options.Columns == null) return null;

         return options.Columns
            .Select(name => new MatchColumnNameFieldPredicate(name))
            .ToArray();
      }
   }
}
