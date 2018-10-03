using System;
using System.Collections.Generic;
using System.Text;
using Parquet.CLI.Models;

namespace Parquet.CLI.Views
{
   interface IDrawViews
   {
      void Draw(ViewModel viewModel, ViewSettings settings);
   }
}
