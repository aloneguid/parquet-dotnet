using System;
using System.Collections.Generic;
using System.Text;

namespace parq.Display
{
    internal class ViewModel
    {
      public ColumnDetails Column { get; set; }
      public object RawValue { get; set; }
      public string GetFormattedValue()
      {
         var value = Convert.ToString(RawValue);
         var formatted = new StringBuilder();
         var padReq = Column.columnWidth - value.Length;

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
            if (Column.columnWidth > 3)
            {
               formatted.Append(value.Substring(0, Column.columnWidth - 3));
               formatted.Append("...");
            }
            else
            {
               formatted.Append(value.Substring(0, Column.columnWidth));
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
