using System.Buffers;

namespace Parquet.Data
{
   class WritableArrayView<T> : ArrayView
   {
      private static readonly ArrayPool<T> ArrayPool = ArrayPool<T>.Shared;
      private readonly T[] _typedArray;
      int _count;

      public WritableArrayView(int length) : this(ArrayPool.Rent(length), 0, length)
      {
      }

      WritableArrayView(T[] array, int offset, int count) : base(array, offset, count)
      {
         _typedArray = array;
      }

      public override int Count => _count;

      protected override void ReturnArray()
      {
         ArrayPool.Return(_typedArray);
      }

      public T this[int i]
      {
         set
         {
            {
               _typedArray[i] = value;
               int count = i + 1;
               if (count > _count) _count = count;
            }
         }
      }
   }
}