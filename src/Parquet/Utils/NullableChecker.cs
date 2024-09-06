using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Parquet.Utils {
    /// <summary>
    /// This class is used to check if nullable is enabled for a given type - otherwise, it is types will be assumed as nullable
    /// </summary>
    public static class NullableChecker {
        /// <summary>
        /// Checks if a type was compiled using Nullable enabled
        /// See https://github.com/dotnet/roslyn/blob/main/docs/features/nullable-metadata.md for details
        /// </summary>
        /// <param name="type">Type to check</param>
        /// <returns></returns>
        public static bool IsNullableEnabled(Type type) {
            // Check if the NullableContextAttribute is present on the type
            CustomAttributeData? nullableContextAttribute = type.CustomAttributes
                .FirstOrDefault(attr => attr.AttributeType.Name == "NullableContextAttribute" || attr.AttributeType.Name == "NullableAttribute");

            if(nullableContextAttribute != null) {
                return true;
            }

            // Check if any properties have the NullableContextAttribute
            foreach(PropertyInfo? property in type.GetProperties()) {
                nullableContextAttribute = property.CustomAttributes
                    .FirstOrDefault(attr => attr.AttributeType.Name == "NullableAttribute");

                if(nullableContextAttribute != null) {

                    return true;
                }
            }

            return false;
        }
    }
}
