using System;
using System.Collections.Generic;
using System.Text;

namespace parq.Display
{
    internal class ViewPort
    {
      private int _height;
      private int _width;

      public ViewPort() : this(Console.WindowWidth, Console.WindowHeight)
      {

      }
      public ViewPort(int width, int height)
      {
         _width = width;
         _height = height;
      }
   }
}
