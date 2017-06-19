/* MIT License
 *
 * Copyright (c) 2017 Elastacloud Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

using System;
using System.Collections;

namespace Parquet
{
   /// <summary>
   /// Represents a column
   /// </summary>
   public class ParquetColumn : IEquatable<ParquetColumn>
   {
      /// <summary>
      /// Creates a new instance of Parquet column from name and values
      /// </summary>
      /// <param name="name"></param>
      /// <param name="values"></param>
      public ParquetColumn(string name, IList values)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));
         Values = values ?? throw new ArgumentNullException(nameof(values));
      }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; }

      public string ParquetRawType { get; internal set; }

      /// <summary>
      /// List of values
      /// </summary>
      public IList Values { get; }

      public void Add(ParquetColumn col)
      {
         foreach(var value in col.Values)
         {
            Values.Add(value);
         }
      }

      /// <summary>
      /// Returns column name
      /// </summary>
      /// <returns></returns>
      public override string ToString()
      {
         return Name;
      }

      public bool Equals(ParquetColumn other)
      {
         if (ReferenceEquals(other, null)) return false;
         if (ReferenceEquals(other, this)) return true;
         return other.Name == this.Name;
      }

      public override bool Equals(object obj)
      {
         if (ReferenceEquals(obj, null)) return false;
         if (obj.GetType() != typeof(ParquetColumn)) return false;
         return Equals((ParquetColumn)obj);
      }

      public override int GetHashCode()
      {
         return Name.GetHashCode();
      }
   }
}
