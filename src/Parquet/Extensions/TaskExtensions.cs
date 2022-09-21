using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Parquet.Extensions {
    static class TaskExtensions {
        public static async Task<T[]> SequentialWhenAll<T>(this IEnumerable<Task<T>> collection) {
            var r = new List<T>();
            foreach(Task<T> item in collection) {
                T ri = await item;
                r.Add(ri);
            }
            return r.ToArray();
        }
    }
}
