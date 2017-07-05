namespace Parquet.File.Data
{
   interface IDataReader
   {
      byte[] Read(byte[] buffer, int offset, int count);
   }
}