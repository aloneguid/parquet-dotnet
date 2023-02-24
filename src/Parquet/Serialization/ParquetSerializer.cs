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

namespace Parquet.Serialization {

    /// <summary>
    /// High-level object serialisation V2. Internal only while being worked on.
    /// Comes as a rewrite of ParquetConvert/ClrBridge/MSILGenerator
    /// TODO:
    /// - lists
    /// - maps
    /// - structs
    /// - append to file
    /// </summary>
    internal static class ParquetSerializer {

        private static Expression LogDebug(string s) {
            return Expression.Call(typeof(Console).GetMethod("WriteLine", new[] { typeof(string) })!, Expression.Constant(s));
        }

        private static Expression AccessProperty<TClass>(
            ParquetSchema schema, DataField df, ParameterExpression classElementVar) {

            Expression result = classElementVar;
            Field[] levelFields = schema.Fields.ToArray();

            for(int i = 0; i < df.Path.Length; i++) {
                string cp = df.Path[i];
                Field? cf = levelFields.FirstOrDefault(x => x.Name == cp);
                if(cf == null)
                    break;

                //bool isValue = i == df.Path.Length - 1;

                string name = cf.ClrPropName ?? cf.Name;
                result = Expression.Property(result, name);

                levelFields = cf.Children;
            }

            return result;
        }

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

        /// <summary>
        /// Responsible for creating instances of child properties
        /// </summary>
        private static Action<TClass>? Procreate<TClass>(ParquetSchema schema) {

            foreach(DataField df in schema.GetDataFields().Where(f => f.Path.Length > 0)) {
                // todo
            }

            return null;
        }

        private static Func<IEnumerable<TClass>, Array> CreateCollectionExpression<TClass>(Type listElementType,
            ParquetSchema schema, DataField df) {

            Type listType = typeof(List<>).MakeGenericType(listElementType);

            ParameterExpression classesParam = Expression.Parameter(typeof(IEnumerable<TClass>), "classes");
            ParameterExpression resultVar = Expression.Variable(listType, "values");

            // loop over collection
            ParameterExpression enumeratorVar = Expression.Variable(typeof(IEnumerator<TClass>), "enumerator");
            MethodCallExpression getEnumeratorCall = Expression.Call(classesParam,
                typeof(IEnumerable<TClass>).GetMethod(nameof(IEnumerable.GetEnumerator))!);
            MethodCallExpression moveNextCall = Expression.Call(enumeratorVar,
                typeof(IEnumerator).GetMethod(nameof(IEnumerator.MoveNext))!);
            ParameterExpression classElementVar = Expression.Variable(typeof(TClass), "curr");
            LabelTarget loopBreakLabel = Expression.Label("loopBreak");
            ParameterExpression classPropertyVar = Expression.Variable(listElementType, "currProp");


            // doc: Expression.Loop is an infinite loop that can be exited with "break"
            LoopExpression loop = Expression.Loop(
                Expression.IfThenElse(

                    // test
                    Expression.Equal(moveNextCall, Expression.Constant(true)),

                    // if true
                    Expression.Block(
                        new[] { classElementVar, classPropertyVar },

                        // get class element into loopVar
                        Expression.Assign(classElementVar, Expression.Property(enumeratorVar, nameof(IEnumerator<TClass>.Current))),

                        // get value of the property
                        Expression.Assign(classPropertyVar, AccessProperty<TClass>(schema, df, classElementVar)),

                        // add propVar to the result list
                        Expression.Call(resultVar, listType.GetMethod(nameof(IList.Add))!, classPropertyVar)
                        ),

                    // if false
                    Expression.Break(loopBreakLabel)
                    ), loopBreakLabel);

            // final assembly
            BlockExpression block = Expression.Block(
                new[] { resultVar, enumeratorVar },

                // create list instance directly in the batch
                Expression.Assign(resultVar, Expression.New(listType)),

                // get enumerator from class collection
                Expression.Assign(enumeratorVar, getEnumeratorCall),

                // loop over classes
                loop,

                // doc: When the block expression is executed, it returns the value of the last expression in the block.
                Expression.Call(resultVar, listType.GetMethod("ToArray")!)
                );

            return Expression.Lambda<Func<IEnumerable<TClass>, Array>>(block, classesParam).Compile();
        }

        private static Action<IEnumerable<TClass>, DataColumn> CreateColumnInjectionExpression<TClass>(
            ParquetSchema schema, DataField df) {

            bool isDictionary = typeof(TClass) == typeof(Dictionary<string, object>);

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


        private static DataColumn CreateDataColumn<TClass>(ParquetSchema schema, DataField df, IEnumerable<TClass> classes) {
            // we need to collect instance field into 2 collections:
            // 1. Actual list of values (including nulls, as DataColumn will pack them on serialization into definition levels)
            // 2. Repetition levels (for complex types only)

            // create destination list
            Type valueType = df.ClrNullableIfHasNullsType;

            // now extract the values
            Func<IEnumerable<TClass>, Array> cx = CreateCollectionExpression<TClass>(valueType, schema, df);
            Array data = cx(classes);
            return new DataColumn(df, data);
        }

        public static async Task<ParquetSchema> SerializeAsync<T>(IEnumerable<T> objectInstances, Stream destination,
            ParquetSerializerOptions? options = null,
            CancellationToken cancellationToken = default) {

            ParquetSchema schema = typeof(T).GetParquetSchema(false);
            DataField[] dataFields = schema.GetDataFields();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, destination)) {
                using ParquetRowGroupWriter rg = writer.CreateRowGroup();

                foreach(DataField df in dataFields) {

                    if(df.MaxRepetitionLevel > 0)
                        throw new NotImplementedException("complex types are not implemented yet");

                    DataColumn dc = CreateDataColumn(schema, df, objectInstances);
                    await rg.WriteColumnAsync(dc, cancellationToken);
                }
            }

            return schema;
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

            var result = new List<T>();
            ParquetSchema cschema = typeof(T).GetParquetSchema(true);
            Action<T>? procreate = Procreate<T>(cschema);
            using ParquetReader reader = await ParquetReader.CreateAsync(source);
            DataField[] dataFields = reader.Schema.GetDataFields();

            for(int rgi = 0; rgi < reader.RowGroupCount; rgi++) {
                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(rgi);

                // add more empty class instances to the result
                int prevRowCount = result.Count;
                for(int i = 0; i < rg.RowCount; i++) {
                    var ne = new T();
                    if(procreate != null) procreate(ne);
                    result.Add(ne);
                }
                
                foreach(DataField df in dataFields) {
                    // todo: check if destination type contain this property?

                    DataColumn dc = await rg.ReadColumnAsync(df, cancellationToken);
                    Action<IEnumerable<T>, DataColumn> xtree = CreateColumnInjectionExpression<T>(reader.Schema, df);
                    xtree(result, dc);
                }
            }

            return result;
        }
    }
}
