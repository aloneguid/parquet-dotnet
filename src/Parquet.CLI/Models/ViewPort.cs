using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.CLI.Models
{
   public class ViewPort
   {
      private int _height;
      private int _width;

      public ViewPort() : this(Console.WindowWidth, Console.WindowHeight)
      {

      }
      public ViewPort(int width, int height)
      {
         Width = width;
         Height = height;
      }

      public int Width { get => _width; set => _width = value; }
      public int Height { get => _height; set => _height = value; }
   }
}
