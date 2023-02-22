using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Parquet.Serialization;
using Xunit;
using static System.Reflection.Emit.OpCodes;

namespace Parquet.Test.Serialisation {
    public class CILProgramTest {

        class Arg1 {
            public int Number { get; set; }
        }

        [Fact]
        public void SimpleAssign() {
            var program = new CILProgram<Arg1>(nameof(SimpleAssign));

            // program begin

            //program.SetField(Ldarg_1, Ldc_I4_3, typeof(Arg1).GetField(nameof(Arg1.Number))!);
            program.CallVirt(typeof(Arg1).GetProperty(nameof(Arg1.Number)).SetMethod,
                Ldarg_0, Ldc_I4_3);

            program.Op(Ldc_I4_3);

            // program end

            Action<Arg1> dg = program.ToDelegate();

            var arg = new Arg1();
            dg(arg);

            Assert.Equal(3, arg.Number);
        }
    }
}
