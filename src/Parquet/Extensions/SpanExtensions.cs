namespace System
{
   static class SpanExtensions
   {
      public static int ReadInt32(this Span<byte> span)
      {
         if (BitConverter.IsLittleEndian)
            return (int)span[0] | (int)span[1] << 8 | (int)span[2] << 16 | (int)span[3] << 24;
         return (int)span[0] << 24 | (int)span[1] << 16 | (int)span[2] << 8 | (int)span[3];
      }
   }
}
