#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

// Licensed to the Apache Software Foundation(ASF) under one
// or more contributor license agreements.See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Collections;
using System.Text;

namespace Thrift.Protocol {


    static class ToStringExtensions {
        public static void ToString(this object? self, StringBuilder sb, bool first = true) {
            if(!first)
                sb.Append(", ");

            bool first_child = true;
            if(self is string) // string is IEnumerable
            {
                sb.Append('"');
                sb.Append(self);
                sb.Append('"');
            }
            else if(self is IDictionary id) {
                sb.Append("{ ");
                foreach(DictionaryEntry? pair in id) {
                    if(pair == null)
                        continue;
                    if(first_child)
                        first_child = false;
                    else
                        sb.Append(',');

                    sb.Append("{ ");
                    pair?.Key.ToString(sb);
                    sb.Append(", ");
                    pair?.Value.ToString(sb);
                    sb.Append('}');
                }
                sb.Append('}');
            }
            else if(self is IEnumerable ie) {
                sb.Append("{ ");
                foreach(object? elm in ie) {
                    elm.ToString(sb, first_child);
                    first_child = false;
                }
                sb.Append('}');
            }
            else if(self is TBase tb) {
                sb.Append(tb.ToString());
            }
            else {
                sb.Append(self != null ? self.ToString() : "<null>");
            }
        }
    }
}