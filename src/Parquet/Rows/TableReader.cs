namespace Parquet.Rows {
    /// <summary>
    /// Navigates the table
    /// </summary>
    internal sealed class TableReader {
        private readonly Table _table;

        /// <summary>
        /// 
        /// </summary>
        public TableReader(Table table) {
            _table = table;
        }
    }
}