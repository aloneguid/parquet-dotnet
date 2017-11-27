namespace Parquet.Data.Predicates
{
   class MatchColumnNameFieldPredicate : FieldPredicate
   {
      private readonly string _prefix;
      private readonly string _columnName;

      public MatchColumnNameFieldPredicate(string columnName)
      {
         _columnName = columnName;
         _prefix = columnName + Schema.PathSeparator;
      }

      public override bool IsMatch(Thrift.ColumnChunk columnChunk, string path)
      {
         return path == _columnName || path.StartsWith(_prefix);
      }

      public override bool IsMatch(Field field)
      {
         return field.Path == _columnName || field.Path.StartsWith(_prefix);
      }
   }
}
