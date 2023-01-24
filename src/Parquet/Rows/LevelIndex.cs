namespace Parquet.Rows {
    struct LevelIndex {
        public LevelIndex(int level, int index) {
            Level = level;
            Index = index;
        }

        public int Level { get; set; }
        public int Index { get; set; }

        public override string ToString() {
            return $"Level: {Level}; Index: {Index}";
        }

        public override bool Equals(object? obj) {
            if(obj is LevelIndex li &&
               li.Index == Index &&
               li.Level == Level)
                return true;

            return false;
        }

        public override int GetHashCode() {
            return (Level.GetHashCode() * 17) + Index.GetHashCode();
        }
    }
}