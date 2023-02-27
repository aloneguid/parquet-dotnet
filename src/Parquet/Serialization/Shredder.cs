using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Serialization {

    class FieldStriper<TClass> {

        public FieldStriper(DataField field, Func<DataField, IEnumerable<TClass>, DataColumn> striper) {
            Field = field;
            Stripe = striper;
        }

        public DataField Field { get; }

        public Func<DataField, IEnumerable<TClass>, DataColumn> Stripe { get; }
    }

    class FieldAssembler<TClass> {

        public FieldAssembler(DataField field, Action<IEnumerable<TClass>, DataColumn> assembler) {
            Field = field;
            Assemble = assembler;
        }

        public DataField Field { get; }

        public Action<IEnumerable<TClass>, DataColumn> Assemble { get; }
    }

    /// <summary>
    /// Not a stripper
    /// </summary>
    class Striper<TClass> {

        public Striper(ParquetSchema schema) {
            Schema = schema;
        }

        public ParquetSchema Schema { get; }

        public List<FieldStriper<TClass>> FieldStripers { get; } = new();
    }

    class Assembler<TClass> {
        public Assembler(ParquetSchema schema) {
            Schema = schema;
        }

        public ParquetSchema Schema { get; }

        public List<FieldAssembler<TClass>> FieldAssemblers { get; } = new();
    }

    /// <summary>
    /// Record striping and assembly algorithm using expression trees.
    /// See also: https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper
    /// </summary>
    class Shredder<TClass> {

        /// <summary>
        /// The most interesting part of striping is here - this is where we receive a class instance and need to stripe the field
        /// </summary>
        private Expression CreateColumnStriper(
            ParquetSchema schema, DataField df,

            ParameterExpression classElementVar,
            
            ParameterExpression valuesVar,
            ParameterExpression dlsVar,
            ParameterExpression rlsVar) {

            // striper should look at two possibilities - a child is either a field or an instance of a repeated field

            Expression xprop = classElementVar;
            Type tprop = typeof(TClass);
            Field[] levelFields = schema.Fields.ToArray();
            var nullChecks = new List<Expression>();

            for(int i = 0; i < df.Path.Length; i++) {
                string cp = df.Path[i];
                Field? cf = levelFields.FirstOrDefault(x => x.Name == cp);
                if(cf == null)
                    break;

                bool isRepeated = cf.SchemaType == SchemaType.List || 
                    (cf.SchemaType == SchemaType.Data && cf is DataField fdf && fdf.IsArray);

                if(isRepeated) {
                    throw new NotImplementedException("repeated child");
                }

                bool isValue = i == df.Path.Length - 1;
                string name = cf.ClrPropName ?? cf.Name;
                tprop = tprop.GetProperty(name)!.PropertyType;

                if(!isValue) {
                    // generate null check
                    nullChecks.Add(Expression.NotEqual(Expression.Property(xprop, name), Expression.Constant(null)));

                    levelFields = cf.Children;
                }

                xprop = Expression.Property(xprop, name);
            }

            // here we have null checks and property getter expression ready
            if(nullChecks.Count > 0) {
                // if there are null checks, wrap xprop in null check condition i.e.:
                // xprop -> if(a == null || a.b == null || a.b.c == null) return default(xprop) else return xprop;
                Expression nullTest = nullChecks.First();
                foreach(Expression nullCheck in nullChecks.Skip(1)) {
                    nullTest = Expression.Or(nullTest, nullCheck);
                }

                // the result of CE is VOID! In C# "if" is not a real expression but a statement.
                ParameterExpression retVar = Expression.Variable(tprop);
                ConditionalExpression ce = Expression.IfThenElse(
                    nullTest,
                    Expression.Assign(retVar, xprop),
                    Expression.Assign(retVar, Expression.Default(tprop)));

                xprop = Expression.Block(new[] { retVar }, ce, retVar);
            }

            ParameterExpression valueVar = Expression.Variable(tprop);

            return Expression.Block(
                new[] {valueVar},

                // get property value
                Expression.Assign(valueVar, xprop),

                // add definition level to definition level list
                df.IsNullable
                    ? Expression.Call(dlsVar,

                        typeof(List<int>).GetMethod(nameof(IList.Add))!, // method to call

                        Expression.Condition(
                            Expression.Equal(valueVar, Expression.Constant(null)),
                            Expression.Constant(0),
                            Expression.Constant(df.MaxDefinitionLevel))
                        )

                    : Expression.Empty(),
                
                // add value to values list
                Expression.Call(valuesVar, typeof(List<>).MakeGenericType(tprop).GetMethod(nameof(IList.Add))!, valueVar));
        }

        private Expression CreateColumnAssembler(
            ParquetSchema schema, DataField df,

            ParameterExpression classElementVar,
            ParameterExpression dcParam,

            ParameterExpression arrayVar,
            ParameterExpression arrayIndexVar,
            ParameterExpression arrayElementVar) {

            // get array element value
            BinaryExpression fetchFromArray = Expression.Assign(arrayElementVar,
                Expression.ArrayAccess(
                    arrayVar,
                    Expression.PostIncrementAssign(arrayIndexVar)));

            Expression levelProperty = classElementVar;
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
            var blockExprs = new List<Expression>();
            blockExprs.Add(fetchFromArray);
            blockExprs.AddRange(procreateExpressions);
            blockExprs.Add(Expression.Assign(levelProperty, arrayElementVar));

            return Expression.Block(blockExprs);
        }

        /// <summary>
        /// Iterates over class instances, nothing else
        /// </summary>
        private Expression CreateClassLoop(
            ParameterExpression classesParam,
            ParameterExpression classElementVar,
            
            Expression loopAction) {

            ParameterExpression enumeratorVar = Expression.Variable(typeof(IEnumerator<TClass>), "enumerator");
            MethodCallExpression getEnumeratorCall = Expression.Call(classesParam,
                typeof(IEnumerable<TClass>).GetMethod(nameof(IEnumerable.GetEnumerator))!);
            MethodCallExpression moveNextCall = Expression.Call(enumeratorVar,
                typeof(IEnumerator).GetMethod(nameof(IEnumerator.MoveNext))!);
            LabelTarget loopBreakLabel = Expression.Label("loopBreak");

            // doc: Expression.Loop is an infinite loop that can be exited with "break"
            LoopExpression loop = Expression.Loop(
                Expression.IfThenElse(

                    // test
                    Expression.Equal(moveNextCall, Expression.Constant(true)),

                    // if true
                    Expression.Block(
                        //new[] { classElementVar },

                        // get class element into loopVar
                        Expression.Assign(classElementVar, Expression.Property(enumeratorVar, nameof(IEnumerator<TClass>.Current))),

                        loopAction),

                    // if false
                    Expression.Break(loopBreakLabel)
                    ), loopBreakLabel);

            return Expression.Block(
                new[] { enumeratorVar, classElementVar },

                // get enumerator from class collection
                Expression.Assign(enumeratorVar, getEnumeratorCall),

                // loop over classes
                loop);
        }

        private Func<DataField, IEnumerable<TClass>, DataColumn> CreateStriper(DataField df, ParquetSchema schema) {

            // input parameters
            ParameterExpression dfParam = Expression.Parameter(typeof(DataField), "df");
            ParameterExpression classesParam = Expression.Parameter(typeof(IEnumerable<TClass>), "classes");

            Type valuesListType = typeof(List<>).MakeGenericType(df.ClrType);
            ConstructorInfo? dcConstructor = typeof(DataColumn).GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic,
                null,
                CallingConventions.HasThis,
                new[] { typeof(DataField), typeof(Array), typeof(List<int>), typeof(int), typeof(List<int>), typeof(int) },
                null);
            if(dcConstructor == null)
                throw new InvalidOperationException($"could not find a correct construtor for {typeof(DataColumn)}");

            // create lists for values, definition levels and repetition levels
            ParameterExpression valuesVar = Expression.Variable(valuesListType, "values");
            ParameterExpression dlsVar = Expression.Variable(typeof(List<int>), "dls");
            ParameterExpression rlsVar = Expression.Variable(typeof(List<int>), "rls");

            // more vars
            ParameterExpression classElementVar = Expression.Variable(typeof(TClass), "curr");

            Expression columnStriper = CreateColumnStriper(schema, df, classElementVar, valuesVar, dlsVar, rlsVar);

            BlockExpression block = Expression.Block(
                new[] { valuesVar, dlsVar, rlsVar, classElementVar },

                // init 3 building blocks
                Expression.Block(
                    Expression.Assign(valuesVar, Expression.New(valuesListType)),
                    // only create list instances when actually required
                    df.MaxDefinitionLevel > 0
                        ? Expression.Assign(dlsVar, Expression.New(typeof(List<int>)))
                        : Expression.Empty(),
                    df.MaxRepetitionLevel > 0
                        ? Expression.Assign(rlsVar, Expression.New(typeof(List<int>)))
                        : Expression.Empty()),

                CreateClassLoop(classesParam, classElementVar, columnStriper),

                // result: use triple to construct DataColumn and return (last element in the block)
                Expression.New(dcConstructor,
                    dfParam,
                    Expression.Call(valuesVar, valuesListType.GetMethod("ToArray")!),
                    dlsVar, Expression.Constant(df.MaxDefinitionLevel),
                    rlsVar, Expression.Constant(df.MaxRepetitionLevel)));

            return Expression.Lambda<Func<DataField, IEnumerable<TClass>, DataColumn>>(block, dfParam, classesParam).Compile();
        }

        private Action<IEnumerable<TClass>, DataColumn> CreateAssembler(DataField df, ParquetSchema schema) {
            ParameterExpression classesParam = Expression.Parameter(typeof(IEnumerable<TClass>), "classes");
            ParameterExpression dcParam = Expression.Parameter(typeof(DataColumn), "dc");

            // more vars
            ParameterExpression classElementVar = Expression.Variable(typeof(TClass), "curr");
            ParameterExpression arrayElementVar = Expression.Variable(df.ClrNullableIfHasNullsType, "currProp");
            ParameterExpression arrayIndexVar = Expression.Variable(typeof(int), "dataIdx");
            ParameterExpression arrayVar = Expression.Variable(df.ClrNullableIfHasNullsType.MakeArrayType(), "data");

            Expression columnAssembler = CreateColumnAssembler(schema, df,
                classElementVar, dcParam,
                arrayVar, arrayIndexVar, arrayElementVar);

            BlockExpression block = Expression.Block(
                new[] { classElementVar, arrayElementVar, arrayIndexVar, arrayVar },

                // initialise array vars
                Expression.Assign(arrayVar,
                    Expression.Convert(Expression.Property(dcParam, nameof(DataColumn.Data)), df.ClrNullableIfHasNullsType.MakeArrayType())),
                Expression.Assign(arrayIndexVar, Expression.Property(dcParam, nameof(DataColumn.Offset))),

                CreateClassLoop(classesParam, classElementVar, columnAssembler));

            return Expression.Lambda<Action<IEnumerable<TClass>, DataColumn>>(block, classesParam, dcParam).Compile();
        }

        public Striper<TClass> CreateStriper() {
            ParquetSchema schema = typeof(TClass).GetParquetSchema(false);
            DataField[] dataFields = schema.GetDataFields();
            var result = new Striper<TClass>(schema);
            result.FieldStripers.AddRange(dataFields.Select(df => new FieldStriper<TClass>(df, CreateStriper(df, schema))));
            return result;
        }

        public Assembler<TClass> CreateAssembler() {
            ParquetSchema schema = typeof(TClass).GetParquetSchema(true);
            DataField[] dataFields = schema.GetDataFields();
            var result = new Assembler<TClass>(schema);
            result.FieldAssemblers.AddRange(dataFields.Select(df => new FieldAssembler<TClass>(df, CreateAssembler(df, schema))));
            return result;
        }
    }
}
