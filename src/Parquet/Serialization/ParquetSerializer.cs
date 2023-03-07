using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization.Dremel;

namespace Parquet.Serialization {

    /// <summary>
    /// High-level object serialisation V2. Internal only while being worked on.
    /// Comes as a rewrite of ParquetConvert/ClrBridge/MSILGenerator and supports nested types as well.
    /// TODO:
    /// - lists
    /// - maps
    /// - structs
    /// - append to file
    /// </summary>
    internal static class ParquetSerializer {

        private static Expression SetProperty<TClass>(
            ParquetSchema schema, DataField df,
            ParameterExpression classInstanceVar, ParameterExpression valueVar) {

            Expression levelProperty = classInstanceVar;
            Type levelType = typeof(TClass);
            Field[] levelFields = schema.Fields.ToArray();
            var procreateExpressions = new List<Expression>();

            for(int i = 0; i < df.Path.Length; i++) {
                string cp = df.Path[i];
                Field? cf = levelFields.FirstOrDefault(x => x.Name == cp);
                if(cf == null)
                    break;

                bool isValue = i == df.Path.Length - 1;
                string name = cf.ClrPropName ?? cf.Name;
                levelProperty = Expression.Property(levelProperty, name);

                if(!isValue) {
                    PropertyInfo pi = levelType.GetProperty(name)!;

                    ConditionalExpression pro = Expression.IfThen(
                        Expression.Equal(levelProperty, Expression.Constant(null)), // test

                        // if null, create a new instance
                        Expression.Assign(levelProperty, Expression.New(pi.PropertyType))
                        );
                    procreateExpressions.Add(pro);

                    levelFields = cf.Children;
                    levelType = pi.PropertyType;
                }
            }

            // assemble
            BinaryExpression pas = Expression.Assign(levelProperty, valueVar);
            if(procreateExpressions.Count == 0) {
                return pas;
            }

            procreateExpressions.Add(pas);
            return Expression.Block(procreateExpressions);
        }

        private static Action<IEnumerable<TClass>, DataColumn> CreateColumnInjectionExpression<TClass>(
            ParquetSchema schema, DataField df) {

            ParameterExpression classesParam = Expression.Parameter(typeof(IEnumerable<TClass>), "classes");
            ParameterExpression dcParam = Expression.Parameter(typeof(DataColumn), "dc");

            // loop over collection of classes
            ParameterExpression enumeratorVar = Expression.Variable(typeof(IEnumerator<TClass>), "enumerator");
            MethodCallExpression getEnumeratorCall = Expression.Call(classesParam,
                typeof(IEnumerable<TClass>).GetMethod(nameof(IEnumerable.GetEnumerator))!);
            MethodCallExpression moveNextCall = Expression.Call(enumeratorVar,
                typeof(IEnumerator).GetMethod(nameof(IEnumerator.MoveNext))!);
            ParameterExpression classInstanceVar = Expression.Variable(typeof(TClass), "curr");
            LabelTarget loopBreakLabel = Expression.Label("loopBreak");

            ParameterExpression arrayElementVar = Expression.Variable(df.ClrNullableIfHasNullsType, "currProp");
            ParameterExpression arrayVar = Expression.Variable(df.ClrNullableIfHasNullsType.MakeArrayType(), "data");
            ParameterExpression arrayIndexVar = Expression.Variable(typeof(int), "dataIdx");


            LoopExpression loop = Expression.Loop(
                Expression.IfThenElse(

                    // test
                    Expression.Equal(moveNextCall, Expression.Constant(true)),

                    // if true
                    Expression.Block(
                        // the variables are scoped to this block, do not redefine variables from the outer block!
                        new[] { classInstanceVar, arrayElementVar },

                        // get class element into loopVar
                        Expression.Assign(classInstanceVar, Expression.Property(enumeratorVar, nameof(IEnumerator<TClass>.Current))),

                        // get array element value
                        Expression.Assign(arrayElementVar, 
                            Expression.ArrayAccess(
                                arrayVar,
                                Expression.PostIncrementAssign(arrayIndexVar))),


                        // assign value to class property
                        SetProperty<TClass>(schema, df, classInstanceVar, arrayElementVar)
                    ),

                    // if false
                    Expression.Break(loopBreakLabel)

                    ),
                loopBreakLabel);

            // final assembly

            BlockExpression block = Expression.Block(
                new[] { enumeratorVar, arrayVar, arrayIndexVar, },

                // get enumerator from class collection
                Expression.Assign(enumeratorVar, getEnumeratorCall),

                // initialise array vars
                Expression.Assign(arrayVar,
                    Expression.Convert(Expression.Property(dcParam, nameof(DataColumn.Data)), df.ClrNullableIfHasNullsType.MakeArrayType())),
                Expression.Assign(arrayIndexVar, Expression.Property(dcParam, nameof(DataColumn.Offset))),

                // loop over classes
                loop);

            return Expression.Lambda<Action<IEnumerable<TClass>, DataColumn>>(block, classesParam, dcParam).Compile();
        }

        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, Stream destination,
            ParquetSerializerOptions? options = null,
            CancellationToken cancellationToken = default) {

            Striper<T> striper = new Dremel1<T>().CreateStriper();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(striper.Schema, destination)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();

                foreach(FieldStriper<T> fs in striper.FieldStripers) {
                    DataColumn dc;
                    try {
                        ShreddedColumn sc = fs.Stripe(fs.Field, objectInstances);
                        dc = new DataColumn(fs.Field, sc.Data, sc.DefinitionLevels, sc.RepetitionLevels);
                        await rg.WriteColumnAsync(dc, cancellationToken);
                    } catch(Exception ex) {
                        throw new ApplicationException($"failed to serialise data column '{fs.Field.Path}'", ex);
                    }
                }
            }

            return striper.Schema;
        }

        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, string filePath,
            ParquetSerializerOptions? options = null,
            CancellationToken cancellationToken = default) {
            using FileStream fs = System.IO.File.Create(filePath);
            return await SerializeAsync(objectInstances, fs, options, cancellationToken);
        }

        public static async Task<IList<T>> DeserializeAsync<T>(Stream source,
            CancellationToken cancellationToken = default)
            where T : new() {

            Assembler<T> asm = new Dremel1<T>().CreateAssembler();
            var result = new List<T>();

            using ParquetReader reader = await ParquetReader.CreateAsync(source);
            for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {
                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(rgi);

                // add more empty class instances to the result
                int prevRowCount = result.Count;
                for(int i = 0; i < rg.RowCount; i++) {
                    var ne = new T();
                    result.Add(ne);
                }

                foreach(FieldAssembler<T> fasm in asm.FieldAssemblers) {
                    DataColumn dc = await rg.ReadColumnAsync(fasm.Field, cancellationToken);
                    try {
                        fasm.Assemble(result.Skip(prevRowCount), dc);
                    } catch(Exception ex) {
                        throw new InvalidOperationException($"failed to deserialise column '{fasm.Field.Path}', pseude code: ['{fasm.GetPseudoCode()}']", ex);
                    }
                }
            }

            return result;
        }
    }
}
