using parq.Display.Models;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace parq.Display.Views
{
    public class SchemaView
    {
      public void Draw(ViewModel viewModel)
      {
         foreach (SchemaElement column in viewModel.Schema.Elements)
         {
            Console.WriteLine("{0}\t{1}", column.Name, column.ElementType);
         }
         Console.WriteLine("-------------------------------------");
         Console.WriteLine("Total {0} Columns", viewModel.Schema.Elements.Count);
      }
    }
}
