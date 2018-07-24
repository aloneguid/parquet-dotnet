using System;

namespace Parquet.Data
{
   abstract class TypedArrayWrapper
   {
      public static TypedArrayWrapper Create<TSystemType>(Array array)
      {
         return new GenericTypedArrayWrapper<TSystemType>((TSystemType[]) array);
      }

      public abstract void SetValue(object value, int index);
      public abstract object GetValue(int index);

      class GenericTypedArrayWrapper<TSystemType> : TypedArrayWrapper
      {
         readonly TSystemType[] _typedArray;

         public GenericTypedArrayWrapper(TSystemType[] typedArray)
         {
            _typedArray = typedArray;
         }

         public override void SetValue(object value, int index)
         {
            _typedArray[index] = (TSystemType)value;
         }

         public override object GetValue(int index)
         {
            return _typedArray[index];
         }
      }
   }
}