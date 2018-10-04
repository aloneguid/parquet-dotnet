using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.CLI.Models
{
   public enum Input
   {
      NoOp,
      Quit,
      NextSheet,
      PrevSheet,
      NextFold,
      PrevFold,
      CellReference
   }
   public class UserInput
   {
      public Input InputType { get; set; }
      public string Reference { get; set; }
   }
}