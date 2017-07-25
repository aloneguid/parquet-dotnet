using parq.Display.Models;
using System;

namespace parq.Display.Views
{
    public class RowCountView
    {
      public void Draw(ViewModel viewModel)
      {
         Console.WriteLine("Total RowCount: {0}", viewModel.RowCount);
      }
    }
}
