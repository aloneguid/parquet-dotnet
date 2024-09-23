using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace Parquet.Serialization {
       internal static class PostConstructor<T> {
        public static readonly Action<T> Execute = GeneratePostConstructor();

        public static readonly bool Required = PostConstructor.GetRequired(typeof(T), new HashSet<Type>());

        public static Action<T> GeneratePostConstructor() {
            var dynamicMethod = new DynamicMethod("DynamicPostConstructor", 
                typeof(void), [typeof(T)], typeof(PostConstructor<>).Module);

            ILGenerator il = dynamicMethod.GetILGenerator();

            if (PostConstructor.GetRequired(typeof(T), new HashSet<Type>())) {
                il.Emit(OpCodes.Ldarg_0);
                PostConstructor.PostConstruct(il, typeof(T), new HashSet<Type>());
            }

            il.Emit(OpCodes.Ret);

            return (Action<T>)dynamicMethod.CreateDelegate(typeof(Action<T>));
        }
    }

    internal static class PostConstructor {
        
        public static (ConstructorInfo Constructor, PropertyInfo[] Properties) GetRecordConstructor(Type recordType) {
            // If it has a default construction, no need to post-construct
            if (recordType.GetConstructor(Type.EmptyTypes) != null) {
                return default;
            }

            var best = recordType.GetConstructors().Select(c => new {
                    Constructor = c,
                    MatchedProperties = c.GetParameters().Select(p => new {
                        Parameter = p,
                        Property = recordType.GetProperty(p.Name ?? string.Empty)
                    })
                    .Where(p => p.Property != null &&  // must exist and be exact same type
                                p.Property.PropertyType == p.Parameter.ParameterType)
                    .Select(p => p.Property!)
                    .ToArray(),
                })
                .Where(c => c.MatchedProperties.Length > 0) // must be at least one such property
                .OrderByDescending(c => c.MatchedProperties.Length)
                .FirstOrDefault();

            return best != null ? (best.Constructor, best.MatchedProperties) : default;
        }

        public static bool GetRequired(Type type, ISet<Type> stack) {
            if (!stack.Add(type)) {
                return false;
            }

            try {
                Type? enumerableT = type.GetInterfaces()
                    .FirstOrDefault(i => i.IsGenericType && 
                        i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

                if (enumerableT != null) {
                    Type elementType = enumerableT.GetGenericArguments()[0];
                    return GetRequired(elementType, stack);
                }

                if (GetRecordConstructor(type).Constructor != null) {
                    return true;
                }

                return type.GetProperties().Any(p => GetRequired(p.PropertyType, stack));
            }
            finally {
                stack.Remove(type);
            }
        }

        // Top of the stack is an object that may need its constructor executed
        // or may reference such objects in its properties).
        public static void PostConstruct(ILGenerator il, Type targetType, ISet<Type> stack)
        {   
            if (!stack.Add(targetType)) {
                il.Emit(OpCodes.Pop);
                return;
            }

            try {
                // Shortcut to avoid pointless deep scanning of common types
                if (targetType.IsPrimitive || 
                    targetType == typeof(string) || 
                    targetType == typeof(object)) {
                    il.Emit(OpCodes.Pop);
                    return;
                }

                // Capture target object in a local variable
                LocalBuilder target = il.DeclareLocal(targetType);
                il.Emit(OpCodes.Stloc, target);

                // if (target != null) ...
                Label skip = il.DefineLabel();
                il.Emit(OpCodes.Ldloc, target);
                il.Emit(OpCodes.Brfalse, skip);

                Type? enumerableT = targetType.GetInterfaces()
                    .FirstOrDefault(i => i.IsGenericType && 
                        i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

                if (enumerableT != null) {
                    il.Emit(OpCodes.Ldloc, target);
                    Type elementType = enumerableT.GetGenericArguments()[0];
                    PostConstructEnumerable(il, elementType, stack);

                    // end of if (target != null)
                    il.MarkLabel(skip);
                    return;
                }

#pragma warning disable IDE0008 // Use explicit type
                var (constructor, properties) = GetRecordConstructor(targetType);
#pragma warning restore IDE0008 // Use explicit type

                if (constructor != null) {
                    // constructor call requires target object to be pushed before args
                    il.Emit(OpCodes.Ldloc, target);

                    // PostConstruct properties and load as constructor args
                    foreach (PropertyInfo property in properties) {
                        il.Emit(OpCodes.Ldloc, target);
                        il.Emit(OpCodes.Callvirt, property.GetGetMethod()!);

                        // Top of stack is now constructor arg - we dup it before recursing
                        il.Emit(OpCodes.Dup);
                        PostConstruct(il, property.PropertyType, stack);
                    }

                    il.Emit(OpCodes.Call, constructor);
                }
                else {
                    // Just PostConstruct properties
                    foreach (PropertyInfo property in targetType.GetProperties()) {
                        il.Emit(OpCodes.Ldloc, target);
                        il.Emit(OpCodes.Callvirt, property.GetGetMethod()!);
                        
                        PostConstruct(il, property.PropertyType, stack);
                    }
                }

                // end of if (target != null)
                il.MarkLabel(skip);
            }
            finally {
                stack.Remove(targetType);
            }
        }

        // The top of the stack is an IEnumerable<elementType>
        private static void PostConstructEnumerable(ILGenerator il, Type elementType, ISet<Type> stack) {
            Type enumerableType = typeof(IEnumerable<>).MakeGenericType(elementType);
            Type enumeratorType = typeof(IEnumerator<>).MakeGenericType(elementType);
            Type enumeratorBaseType = typeof(IEnumerator);

            Label loopStart = il.DefineLabel();
            Label loopEnd = il.DefineLabel();

            // Enumerator            
            il.Emit(OpCodes.Callvirt, enumerableType.GetMethod(nameof(IEnumerable<object>.GetEnumerator))!);
            LocalBuilder enumerator = il.DeclareLocal(enumeratorType);
            il.Emit(OpCodes.Stloc, enumerator);

            // Loop start
            il.MarkLabel(loopStart);

            // Check for end of enumeration
            il.Emit(OpCodes.Ldloc, enumerator);
            il.Emit(OpCodes.Callvirt, enumeratorBaseType.GetMethod(nameof(IEnumerator.MoveNext))!);
            il.Emit(OpCodes.Brfalse, loopEnd);

            // Load current item
            il.Emit(OpCodes.Ldloc, enumerator);
            il.Emit(OpCodes.Callvirt, enumeratorType.GetProperty(nameof(IEnumerator<object>.Current))!.GetGetMethod()!);

            PostConstruct(il, elementType, stack);

            // Loop end
            il.Emit(OpCodes.Br, loopStart);
            il.MarkLabel(loopEnd);

            // Dispose enumerator
            il.Emit(OpCodes.Ldloc, enumerator);
            il.Emit(OpCodes.Callvirt, typeof(IDisposable).GetMethod(nameof(IDisposable.Dispose))!);
        }
    }
}
