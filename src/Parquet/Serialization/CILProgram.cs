using System;
using System.Reflection;
using System.Reflection.Emit;
using static System.Reflection.Emit.OpCodes;

namespace Parquet.Serialization {
    internal class CILProgram<TArgType> {

        private readonly DynamicMethod _method;
        private readonly ILGenerator _il;

        public CILProgram(string name) {
            _method = new DynamicMethod(name, null, new Type[] { typeof(TArgType) }, GetType().GetTypeInfo().Module);

            _il = _method.GetILGenerator();
        }

        public void Op(params OpCode[] codes) {
            foreach(OpCode oc in codes) {
                _il.Emit(oc);
            }
        }

        /// <summary>
        /// https://learn.microsoft.com/en-us/dotnet/api/system.reflection.emit.opcodes.stfld?view=net-7.0
        /// Input stack:
        /// - object reference
        /// - value
        /// </summary>
        public void SetField(OpCode objRef, OpCode value, FieldInfo field) {
            _il.Emit(objRef);
            _il.Emit(value);
            _il.Emit(Stfld, field);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="method"></param>
        /// <param name="parameters">List of parameters, OpCode and LocalBuilder are supported</param>
        public void CallVirt(MethodInfo method, params object[] parameters) {
            if(parameters != null) {
                foreach(object param in parameters) {
                    switch(param) {
                        case OpCode oc:
                            _il.Emit(oc);
                            break;
                        case LocalBuilder lb:
                            _il.Emit(Ldloc, lb.LocalIndex);
                            break;
                        default:
                            throw new NotSupportedException("don't know how to treat parameter " + param?.GetType());
                    }
                }
            }

            _il.Emit(Callvirt, method);
        }

        public Action<TArgType> ToDelegate() {
            _il.Emit(Ret);
            return (Action<TArgType>)_method.CreateDelegate(typeof(Action<TArgType>));
        }
    }
}
