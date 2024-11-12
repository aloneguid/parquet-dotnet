using System;
using System.Collections.Generic;
using System.Collections;
using System.Reflection;
using System.Linq;

namespace Parquet {
    static class TypeExtensions {
        /// <summary>
        /// Creates a generic typed list of elements of this type.
        /// </summary>
        public static IList CreateGenericList(this Type t) {
            Type rt = typeof(List<>).MakeGenericType(t);

            return (IList)Activator.CreateInstance(rt)!;
        }

        /// <summary>
        /// Checks if this type implements generic IEnumerable or an array.
        /// </summary>
        /// <param name="t"></param>
        /// <param name="baseType"></param>
        /// <returns></returns>
        public static bool TryExtractIEnumerableType(this Type t, out Type? baseType) {
            if(typeof(byte[]) == t) {
                //it's a special case to avoid confustion between byte arrays and repeatable bytes
                baseType = null;
                return false;
            }

            TypeInfo ti = t.GetTypeInfo();
            Type[] args = ti.GenericTypeArguments;

            if(args.Length > 0) {
                //check derived interfaces
                foreach(Type interfaceType in ti.ImplementedInterfaces) {
                    TypeInfo iti = interfaceType.GetTypeInfo();
                    if(iti.IsGenericType && iti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) {
                        baseType = iti.GenericTypeArguments[0];
                        return true;
                    }
                }

                //check if this is an IEnumerable<>
                if(ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) {
                    baseType = ti.GenericTypeArguments[0];
                    return true;
                }
            }

            if(ti.IsArray) {
                baseType = ti.GetElementType();
                return true;
            }

            baseType = null;
            return false;
        }

        public static Type ExtractElementTypeFromEnumerableType(this Type t) {
            if(t.TryExtractIEnumerableType(out Type? iet))
                return iet!;

            throw new ArgumentException($"type {t} is not single-element generic enumerable", nameof(t));
        }

        public static MethodInfo GetGenericListAddMethod(this Type listType) {
            Type elementType = listType.ExtractElementTypeFromEnumerableType();
            Type genericListType = typeof(List<>).MakeGenericType(elementType);
            MethodInfo? method = genericListType.GetMethod(nameof(IList.Add));
            return method ?? throw new InvalidOperationException("method not present");
        }

        public static bool IsGenericIDictionary(this Type t) {
            return t.IsGenericType &&
                (t.GetGenericTypeDefinition() == typeof(IDictionary<,>) ||
                t.GetInterfaces().Any(x => x.IsGenericType && typeof(IDictionary<,>) == x.GetGenericTypeDefinition()));
        }

        public static bool TryExtractDictionaryType(this Type t, out Type? keyType, out Type? valueType) {
            if(t.IsGenericIDictionary()) {
                TypeInfo ti = t.GetTypeInfo();
                keyType = ti.GenericTypeArguments[0];
                valueType = ti.GenericTypeArguments[1];
                return true;
            }

            keyType = valueType = null;
            return false;
        }

        public static bool IsNullable(this IList list) {
            TypeInfo ti = list.GetType().GetTypeInfo();

            Type t = ti.GenericTypeArguments[0];
            Type? gt = t.GetTypeInfo().IsGenericType ? t.GetTypeInfo().GetGenericTypeDefinition() : null;

            return gt == typeof(Nullable<>) || t.GetTypeInfo().IsClass;
        }

        public static bool IsNullable(this PropertyInfo pi) {
            return pi.GetMethod?.CustomAttributes.Any(x => x.AttributeType.Name == "NullableContextAttribute") == true ||
                pi.DeclaringType?.CustomAttributes.Any(x => x.AttributeType.Name == "NullableContextAttribute" &&
                    (byte)x.ConstructorArguments[0].Value! == 2) == true;
        }

        public static bool IsNullable(this Type t) {
            TypeInfo ti = t.GetTypeInfo();

            return
                ti.IsClass || ti.IsInterface ||
                (ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(Nullable<>));
        }

        public static bool IsSystemNullable(this Type t) {
            TypeInfo ti = t.GetTypeInfo();

            return ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public static Type GetNonNullable(this Type t) {
            TypeInfo ti = t.GetTypeInfo();

            if(ti.IsClass || ti.IsInterface) {
                return t;
            }

            return ti.GenericTypeArguments[0];
        }

        public static Type GetNullable(this Type t) {
            TypeInfo ti = t.GetTypeInfo();

            if(ti.IsClass) {
                return t;
            }

            Type nt = typeof(Nullable<>);
            return nt.MakeGenericType(t);
        }

        public static bool IsSimple(this Type t) {
            if(t == null)
                return true;

            return
                t == typeof(bool) ||
                t == typeof(byte) ||
                t == typeof(sbyte) ||
                t == typeof(char) ||
                t == typeof(decimal) ||
                t == typeof(double) ||
                t == typeof(float) ||
                t == typeof(int) ||
                t == typeof(uint) ||
                t == typeof(long) ||
                t == typeof(ulong) ||
                t == typeof(short) ||
                t == typeof(ushort) ||
                t == typeof(TimeSpan) ||
                t == typeof(DateTime) ||
                t == typeof(Guid) ||
                t == typeof(string);
        }
    }
}