using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.CLI.Models
{
   public class ColumnDetails
   {
      public bool isNullable { get; set; }
      public Data.DataType type { get; set; }

      public string columnName { get; set; }
      public int columnWidth { get; set; }

      public string GetFormattedValue(object rawValue, ViewPort viewPort, bool displayNulls, string verticalSeperator)
      {
         string value = Convert.ToString(rawValue);
         var formatted = new StringBuilder();
         int targetWidth = columnWidth > viewPort.Width ? viewPort.Width - ((verticalSeperator.Length*2)+1) : columnWidth;

         if (displayNulls && rawValue == null)
         {

            for (int k = 0; k < targetWidth - 6; k++)
            {
               formatted.Append(" ");
            }
            formatted.Append("[null]");

            return formatted.ToString();
         }

         int padReq = targetWidth - value.Length;
         if (padReq > 0)
         {
            for (int k = 0; k < padReq; k++)
            {
               formatted.Append(" ");
            }
            formatted.Append(value);
         }
         else if (padReq < 0)
         {
            if (columnWidth > 3)
            {
               formatted.Append(value.Substring(0, targetWidth - 3));
               formatted.Append("...");
            }
            else
            {
               formatted.Append(value.Substring(0, targetWidth));
            }
         }
         else
         {
            formatted.Append(value);
         }
         return formatted.ToString();
      }
   }
}
