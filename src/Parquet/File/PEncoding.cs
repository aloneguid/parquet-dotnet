namespace Parquet.File
{
   /// <summary>
   /// Handles parquet encoding logic.
   /// All methods are made static for increased performance.
   /// </summary>
   static class PEncoding
   {
      public static int GetWidthFromMaxInt(int value)
      {
         for(int i = 0; i < 64; i++)
         {
            if (value == 0) return i;
            value >>= 1;
         }

         return 1;
      }
   }
}
