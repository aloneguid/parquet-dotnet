namespace Parquet.File.Data
{
   interface IDataReader
   {
      void Read(byte[] buffer, int offset, int count);
   }
}