using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization.Dremel;

namespace Parquet.Serialization {

    class FieldWorker<TClass> {
        public DataField Field { get; }

        public Expression Expression { get; }

        public FieldWorker(DataField field, Expression expression) {
            Field = field;
            Expression = expression;
        }

        /// <summary>
        /// Gets internal property "DebugView" which is normally only available in Visual Studio debugging session
        /// </summary>
        /// <returns></returns>
        public string GetPseudoCode() {
            PropertyInfo? propertyInfo = typeof(Expression).GetProperty("DebugView", BindingFlags.Instance | BindingFlags.NonPublic);
            if(propertyInfo == null)
                return string.Empty;

            return (string)propertyInfo.GetValue(Expression)!;
        }
    }

    class FieldStriper<TClass> : FieldWorker<TClass> {

        public FieldStriper(DataField field, Func<DataField, IEnumerable<TClass>, ShreddedColumn> striper, Expression expression) 
            : base(field, expression) {
            Stripe = striper;
        }

        public Func<DataField, IEnumerable<TClass>, ShreddedColumn> Stripe { get; }
    }

    class FieldAssembler<TClass> : FieldWorker<TClass> {

        public FieldAssembler(DataField field, Action<IEnumerable<TClass>, DataColumn> assembler, Expression expression) : base(field, expression) {
            Assemble = assembler;
        }

        public Action<IEnumerable<TClass>, DataColumn> Assemble { get; }
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
    /// Original Dremel paper: https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36632.pdf
    /// </summary>
    class Dremel1<TClass> {

        private static readonly MethodInfo LevelsAddMethod = typeof(List<int>).GetMethod(nameof(IList.Add))!;

        private static Expression ThrowNotImplementedException(string message) {
            return Expression.Throw(Expression.Constant(new NotImplementedException(message)));
        }

        private Expression DissectValue(
            Expression valueVar,
            DataField df,

            Expression repetitionLevelVar,   // current repetition level

            ParameterExpression valuesVar,
            ParameterExpression dlsVar,
            ParameterExpression rlsVar) {

            Type tprop = df.ClrNullableIfHasNullsType;
            Type nntprop = df.ClrType;

            return Expression.Block(
                df.IsNullable
                    ? Expression.Block(
                        Expression.IfThenElse(
                            Expression.Equal(valueVar, Expression.Constant(null)),
                            Expression.Call(dlsVar, LevelsAddMethod, Expression.Constant(0)),
                            Expression.Block(
                                Expression.Call(dlsVar, LevelsAddMethod, Expression.Constant(df.MaxDefinitionLevel)),

                                // also add non-null value
                                Expression.Call(valuesVar,
                                    typeof(List<>).MakeGenericType(nntprop).GetMethod(nameof(IList.Add))!,
                                    // unwrap Nullable<T>
                                    tprop.IsSystemNullable()
                                        ? Expression.Property(valueVar, "Value")
                                        : valueVar))))

                    // for non-nullables it's sufficient to just add the value
                    : Expression.Block(
                        Expression.Call(valuesVar,
                            typeof(List<>).MakeGenericType(nntprop).GetMethod(nameof(IList.Add))!,
                            valueVar)),

                df.MaxRepetitionLevel > 0
                    ? Expression.Call(rlsVar, LevelsAddMethod, repetitionLevelVar)
                    : Expression.Empty());

        }

        private static List<string> GetChildPath(List<string> path, Field currentField) {

            if(currentField.SchemaType == SchemaType.List) {
                // element.list.element.child
                return path.Skip(3).ToList();
            }

            // element.child
            return path.Skip(1).ToList();
        }

        private static Field[] GetNextLevelFields(Field currentField) {

            if(currentField.SchemaType == SchemaType.List) {
                return currentField.Children[0].Children;
            }

            return currentField.Children;
        }

        /// <summary>
        /// Record dissection algorithm modeled from Dremel paper, Figure 16.
        /// </summary>
        private Expression DissectRecord(
            Field[] levelFields,
            List<string> path,
            Type rootType,
            DataField df,
            Expression rootVar,

            int treeDepth,
            ParameterExpression repetitionLevelVar,

            ParameterExpression valuesVar,
            ParameterExpression dlsVar,
            ParameterExpression rlsVar) {

            // enumerate values (L5)
            // "decoder" is either a class property accessor or collection enumerator
            string currentFieldName = path.First();
            Field? field = levelFields.FirstOrDefault(x => x.Name == currentFieldName);
            if(field == null)
                throw new NotSupportedException();

            bool isRepeated = field.SchemaType ==
                SchemaType.List ||
                (field.SchemaType == SchemaType.Data && field is DataField fdf && fdf.IsArray);

            string currentClassPropertyName = field.ClrPropName ?? field.Name;
            bool isAtomic = path.Count == 1;    // is this an atomic value (tree leaf)?

            // remove extra container in the list
            //if(field.SchemaType == SchemaType.List) {
            //    field = ((ListField)field).Item;
            //}

            // "seen" flag
            ParameterExpression isSeenVar = Expression.Variable(typeof(bool), "isSeen_" + currentClassPropertyName);

            // inside "while recorder" loop (L6 onwards)
            ParameterExpression chRepetitionLevelVar = Expression.Variable(typeof(int), "chRepetitionLevel");
            Expression striperElementVar;
            Type childType;

            if(isRepeated) {
                if(isAtomic) treeDepth++;
                if(!rootType.GetProperty(currentClassPropertyName)!.PropertyType.TryExtractEnumerableType(out Type? collectionElementType)) {
                    throw new NotSupportedException();
                }
                childType = collectionElementType!;
                striperElementVar = Expression.Variable(childType);
            } else {
                childType = rootType.GetProperty(currentClassPropertyName)!.PropertyType;
                // ternary null check condition:
                // rootVar == null ? null : rootVar.currentClassPropertyName;
                // replaces unsafe version of: striperElementVar = Expression.Property(rootVar, currentClassPropertyName);
                striperElementVar = Expression.Condition(
                    Expression.Equal(rootVar, Expression.Constant(null)),
                    Expression.Convert(Expression.Constant(null), childType),   // cast null to childType as argument types must match
                    Expression.Property(rootVar, currentClassPropertyName));
            }

            Expression valueIteration = Expression.Block(
                new[] { chRepetitionLevelVar },
                Expression.Assign(chRepetitionLevelVar, repetitionLevelVar), // int chRepetitionLevel = repetitionLevel (L8)

                Expression.IfThenElse(  // L9
                    Expression.IsTrue(isSeenVar),
                    Expression.Assign(chRepetitionLevelVar, Expression.Constant(treeDepth)),
                    Expression.Assign(isSeenVar, Expression.Constant(true))),

                isAtomic    // L14
                    ? DissectValue(striperElementVar, df, chRepetitionLevelVar,
                        valuesVar, dlsVar, rlsVar)
                    : DissectRecord(GetNextLevelFields(field), GetChildPath(path, field), childType, df,
                        striperElementVar, treeDepth + 1, chRepetitionLevelVar,
                        valuesVar, dlsVar, rlsVar)
                );


            Expression final;
            if(isRepeated) {
                MemberExpression collection = Expression.Property(rootVar, currentClassPropertyName);
                Expression loop = CreateLoop(childType, collection, (ParameterExpression)striperElementVar, valueIteration);
                final = loop;
            } else {
                final = valueIteration;
            }

            return Expression.Block(
                new[] { isSeenVar },
                Expression.Assign(isSeenVar, Expression.Constant(false)),
                final);
        }

        /// <summary>
        /// The most interesting part of striping is here - this is where we receive a class instance and need to stripe the field
        /// </summary>
        private Expression DissectRecord(
            ParquetSchema schema, DataField df,

            ParameterExpression classElementVar,
            ParameterExpression repetitionLevelVar,

            ParameterExpression valuesVar,
            ParameterExpression dlsVar,
            ParameterExpression rlsVar) {

            return Expression.Block(
                //new[] { repetitionLevelVar },
                Expression.Assign(repetitionLevelVar, Expression.Constant(0)),
                DissectRecord(schema.Fields.ToArray(), df.Path.ToList(), typeof(TClass), df,
                    classElementVar,
                    0, repetitionLevelVar,
                    valuesVar, dlsVar, rlsVar));
        }

        private Expression UninitRLArray(ParameterExpression rlState, int startFrom, int max) {
            var exprs = new List<Expression>();
            for(int i = startFrom; i < max; i++) {
                exprs.Add(Expression.Assign(Expression.ArrayAccess(rlState, Expression.Constant(i)), Expression.Constant(-1)));
            }
            return Expression.Block(exprs);
        }

        private Expression AccessCollection(MemberExpression targetPropertyVar,
            ParameterExpression rlVar, ParameterExpression rlStateVar, int treeDepth,
            Type childType) {

            Type elementType = childType.ExtractElementTypeFromEnumerableType();
            ParameterExpression listIndexVar = Expression.Variable(typeof(int), "listIndex");
            ParameterExpression childVar = Expression.Variable(elementType, "child");

            return Expression.Block(
                new[] { listIndexVar, childVar },

                // listIndexVar = rlStateVar[treeDepth]
                Expression.Assign(listIndexVar, Expression.ArrayAccess(rlStateVar, Expression.Constant(treeDepth))),

                // if(treeDepth >= rlVar) <move to new level>
                Expression.IfThen(
                    Expression.GreaterThanOrEqual(Expression.Constant(treeDepth), rlVar),

                    // make the move
                    Expression.Block(
                        // listIndexVar++
                        Expression.PostIncrementAssign(listIndexVar),

                        // if target collection does not exist, create it
                        Expression.IfThen(
                            // targetPropertyVar == null ?
                            Expression.Equal(targetPropertyVar, Expression.Constant(null)),

                            // targetPropertyVar = new List<...>();
                            Expression.Assign(targetPropertyVar, Expression.New(childType))),

                        // if element is missing, create it
                        Expression.IfThen(
                            // listIndexVar >= targetPropertyVar.Count ?
                            Expression.GreaterThanOrEqual(listIndexVar, Expression.Property(targetPropertyVar, "Count")),
                            
                            Expression.Call(targetPropertyVar, childType.GetGenericListAddMethod(), Expression.New(elementType))
                            )
                        )
                    ),

                // access the element
                // childVar = targetPropertyVar[listIndexVar]
                Expression.Assign(childVar, Expression.Property(targetPropertyVar, "Item", listIndexVar)),

                childVar
                );
        }

        private Expression AssembleRecord(Field[] levelFields,
            List<string> path,
            Type recordInstanceType,
            DataField df,
            Expression recordInstanceVar,
            int treeDepth,
            ParameterExpression dataElementVar,
            ParameterExpression? rlVar,
            ParameterExpression? rlStateVar) {

            string currentFieldName = path.First();
            Field? field = levelFields.FirstOrDefault(x => x.Name == currentFieldName);
            if(field == null)
                throw new NotSupportedException();

            bool isRepeated = field.SchemaType ==
                SchemaType.List ||
                (field.SchemaType == SchemaType.Data && field is DataField fdf && fdf.IsArray);

            string currentClassPropertyName = field.ClrPropName ?? field.Name;
            bool isAtomic = path.Count == 1;    // is this an atomic value (tree leaf)?

            // regardless of repetition, it's always a class property
            MemberExpression targetPropertyVar = Expression.Property(recordInstanceVar, currentClassPropertyName);
            Type childType = recordInstanceType.GetProperty(currentClassPropertyName)!.PropertyType;

            Expression valueIteration;

            if(isAtomic) {
                // atomic leaf reached
                if(isRepeated) {
                    // repeated atomic simply adds element to the list
                    valueIteration = Expression.Block(
                        Expression.IfThen(
                            Expression.Equal(targetPropertyVar, Expression.Constant(null)),
                            Expression.Assign(targetPropertyVar, Expression.New(childType))),
                        Expression.Call(targetPropertyVar, childType.GetGenericListAddMethod(), dataElementVar));
                } else {
                    // non-repeated atomic is a simple property assign
                    valueIteration = Expression.Assign(targetPropertyVar, dataElementVar);
                }
            } else {
                if(isRepeated) {
                    // repeated non-atomic, the most interesting use for repetition levels
                    // this indicates collection element, not the collection itself!
                    Type elementType = childType.ExtractElementTypeFromEnumerableType();
                    valueIteration = AssembleRecord(
                        GetNextLevelFields(field),
                        GetChildPath(path, field),
                        elementType, df,
                        AccessCollection(targetPropertyVar, rlVar!, rlStateVar!, treeDepth, childType),
                        treeDepth + 1, dataElementVar, rlVar, rlStateVar);
                } else {
                    // non-repeated non-atomic - just dive in
                    valueIteration = Expression.Block(
                          // if(targetPropertyVar == null) targetPropertyVar = new childType();
                          Expression.IfThen(
                              Expression.Equal(targetPropertyVar, Expression.Constant(null)),
                              Expression.Assign(targetPropertyVar, Expression.New(childType))),
                          // assemble child
                          AssembleRecord(GetNextLevelFields(field), GetChildPath(path, field), childType, df, targetPropertyVar, treeDepth + 1,
                              dataElementVar, rlVar, rlStateVar));
                }
            }

            return valueIteration;
        }

        /// <summary>
        /// Puts next value for data and repetition level.
        /// Returns boolean flag (false indicates there are no more values left)
        /// </summary>
        private Expression TakeCurrentValues(
            DataField df,
            ParameterExpression dcParam,
            ParameterExpression arrayVar,
            ParameterExpression arrayIndexVar,
            ParameterExpression? rlIndexVar,
            
            ParameterExpression dataElementVar,
            ParameterExpression? rlVar) {

            ParameterExpression flag = Expression.Variable(typeof(bool), "successFlag");
            return Expression.Block(
                new[] { flag },
                Expression.IfThenElse(
                    // arrayIndexVar < dcParam.Data.Length ?
                    Expression.LessThan(arrayIndexVar, Expression.Property(Expression.Property(dcParam, nameof(DataColumn.Data)), nameof(Array.Length))),
                    
                    Expression.Block(

                        // get array value: arrayElementVar = arrayVar[arrayIndexVar];
                        Expression.Assign(dataElementVar, Expression.ArrayAccess(arrayVar, arrayIndexVar)),

                        rlVar != null && rlIndexVar != null
                            // get repetition level value: rlVar = dcParam.RepetitionLevels[rlIndexVar];
                            ? Expression.Assign(rlVar, Expression.ArrayAccess(Expression.Property(dcParam, nameof(DataColumn.RepetitionLevels)), rlIndexVar))
                            : Expression.Empty(),

                        // flag = true
                        Expression.Assign(flag, Expression.Constant(true))),

                    // flag = false
                    Expression.Assign(flag, Expression.Constant(false))),

                flag);
        }

        /// <summary>
        /// Called for each class element in a loop
        /// </summary>
        private Expression CreateColumnAssembler(
            ParquetSchema schema, DataField df,

            ParameterExpression classElementVar,
            ParameterExpression dcParam,

            ParameterExpression arrayVar,
            ParameterExpression arrayIndexVar,
            ParameterExpression? rlIndexVar,
            ParameterExpression? rlStateVar) {

            // column assembler for a single class instance may take more than one value from data array and rls array
            // when repetition levels are used, therefore responsibility for taking next value lies with column assembler,
            // and not class loop.

            // repetition level basically says at which level to start creating new lists
            // for new records it always starts from zero (0).

            ParameterExpression dataElementVar = Expression.Variable(df.ClrNullableIfHasNullsType, "currProp");
            ParameterExpression? rlVar = rlIndexVar == null
                ? null
                : Expression.Variable(typeof(int), "repetitionLevel");

            // if it's not a repeated column, the code is really simple with no loops whatsoever
            // so we're returning simple code here for performance reasons
            if(rlIndexVar == null || rlVar == null) {
                return Expression.Block(
                    new[] { dataElementVar },
                    TakeCurrentValues(df, dcParam, arrayVar, arrayIndexVar, rlIndexVar, dataElementVar, rlVar),
                    AssembleRecord(schema.Fields.ToArray(), df.Path.ToList(), typeof(TClass), df,
                        classElementVar, 0,
                        dataElementVar, rlVar, rlStateVar),
                    Expression.PreIncrementAssign(arrayIndexVar));
            }

            
            LabelTarget loopBreakLabel = Expression.Label("loopBreak");
            ParameterExpression canReadVar = Expression.Variable(typeof(bool), "canRead");
            ParameterExpression isFirstVar = Expression.Variable(typeof(bool), "isFirst");
            return Expression.Block(
                new[] { dataElementVar, canReadVar, isFirstVar, rlVar! },

                // isFirst = true
                Expression.Assign(isFirstVar, Expression.Constant(true)),

                // loop for value + repetition level
                Expression.Loop(
                        Expression.Block(

                            // canRead = TakeNextValue(...);
                            Expression.Assign(canReadVar, TakeCurrentValues(df, dcParam, arrayVar, arrayIndexVar, rlIndexVar, dataElementVar, rlVar)),

                            // if(!canRead) break;
                            Expression.IfThen(
                                Expression.IsFalse(canReadVar),
                                Expression.Break(loopBreakLabel)),

                            // if(isFirstVar && rlVar == 0) break;
                            Expression.IfThen(
                                Expression.IsTrue(Expression.And(Expression.Not(isFirstVar), Expression.Equal(rlVar!, Expression.Constant(0)))),
                                Expression.Break(loopBreakLabel)),

                            // isFirst = false;
                            Expression.Assign(isFirstVar, Expression.Constant(false)),

                            // assemble!
                            AssembleRecord(schema.Fields.ToArray(), df.Path.ToList(), typeof(TClass), df,
                                classElementVar, 0,
                                dataElementVar, rlVar, rlStateVar),

                            // increment indexes, as we have used this value
                            Expression.PreIncrementAssign(arrayIndexVar),
                            Expression.PreIncrementAssign(rlIndexVar)

                            ),
                        loopBreakLabel));
        }

        private Expression CreateLoop<TElement>(Expression classesParam,
            ParameterExpression classElementVar,
            Expression loopAction) {
            return CreateLoop(typeof(TElement), classesParam, classElementVar, loopAction);
        }

        /// <summary>
        /// Iterates over class instances, nothing else
        /// </summary>
        private Expression CreateLoop(Type elementType,
            Expression classesParam,
            ParameterExpression classElementVar,
            
            Expression loopAction) {

            Type enumeratorGenericType = typeof(IEnumerator<>).MakeGenericType(elementType);
            Type enumerableGenericType = typeof(IEnumerable<>).MakeGenericType(elementType);

            ParameterExpression enumeratorVar = Expression.Variable(enumeratorGenericType, "enumerator");
            MethodCallExpression getEnumeratorCall = Expression.Call(classesParam,
                enumerableGenericType.GetMethod(nameof(IEnumerable.GetEnumerator))!);
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
                        Expression.Assign(classElementVar, Expression.Property(enumeratorVar, nameof(IEnumerator.Current))),

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

        private Func<DataField, IEnumerable<TClass>, DataColumn> CreateStriper(DataField df, ParquetSchema schema, out Expression expression) {

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
            ParameterExpression repetitionLevelVar = Expression.Variable(typeof(int), "repetitionLevel");

            Expression columnStriper = DissectRecord(schema, df,
                classElementVar, repetitionLevelVar,
                valuesVar, dlsVar, rlsVar);

            BlockExpression block = Expression.Block(
                new[] { valuesVar, dlsVar, rlsVar, classElementVar, repetitionLevelVar },

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

                CreateLoop<TClass>(classesParam, classElementVar, columnStriper),

                // result: use triple to construct DataColumn and return (last element in the block)
                Expression.New(dcConstructor,
                    dfParam,
                    Expression.Call(valuesVar, valuesListType.GetMethod("ToArray")!),
                    dlsVar, Expression.Constant(df.MaxDefinitionLevel),
                    rlsVar, Expression.Constant(df.MaxRepetitionLevel)));

            expression = block;

            return Expression.Lambda<Func<DataField, IEnumerable<TClass>, DataColumn>>(block, dfParam, classesParam).Compile();
        }

        private Action<IEnumerable<TClass>, DataColumn> CreateAssembler(DataField df, ParquetSchema schema, out Expression expression) {
            ParameterExpression classesParam = Expression.Parameter(typeof(IEnumerable<TClass>), "classes");
            ParameterExpression dcParam = Expression.Parameter(typeof(DataColumn), "dc");

            // more vars
            ParameterExpression classElementVar = Expression.Variable(typeof(TClass), "curr");

            ParameterExpression dataArrayIndexVar = Expression.Variable(typeof(int), "dataIdx");
            ParameterExpression dataArrayVar = Expression.Variable(df.ClrNullableIfHasNullsType.MakeArrayType(), "data");
            ParameterExpression? rlIndexVar = df.MaxRepetitionLevel > 0
                ? Expression.Variable(typeof(int), "repetitionLevelIndex")
                : null;
            ParameterExpression? rlStateVar = df.MaxRepetitionLevel > 0
                ? Expression.Variable(typeof(int[]), "rlState")
                : null;

            Expression columnAssembler = CreateColumnAssembler(schema, df,
                classElementVar, dcParam,
                dataArrayVar, dataArrayIndexVar, rlIndexVar, rlStateVar);

            var blockVars = new List<ParameterExpression> { classElementVar, dataArrayIndexVar, dataArrayVar };
            if(rlIndexVar != null)
                blockVars.Add(rlIndexVar);
            if(rlStateVar != null)
                blockVars.Add(rlStateVar);
            BlockExpression block = Expression.Block(
                blockVars,

                // initialise array vars
                Expression.Assign(dataArrayVar,
                    Expression.Convert(Expression.Property(dcParam, nameof(DataColumn.Data)), df.ClrNullableIfHasNullsType.MakeArrayType())),
                Expression.Assign(dataArrayIndexVar, Expression.Property(dcParam, nameof(DataColumn.Offset))),

                rlIndexVar == null
                    ? Expression.Empty()
                    : Expression.Assign(rlIndexVar, Expression.Constant(0)),

                rlStateVar == null
                    ? Expression.Empty()
                    : Expression.Assign(rlStateVar, Expression.NewArrayBounds(typeof(int), Expression.Constant(df.MaxRepetitionLevel))),

                rlStateVar == null
                    ? Expression.Empty()
                    : UninitRLArray(rlStateVar, 0, df.MaxRepetitionLevel),

                CreateLoop<TClass>(classesParam, classElementVar, columnAssembler));

            expression = block;

            return Expression.Lambda<Action<IEnumerable<TClass>, DataColumn>>(block, classesParam, dcParam).Compile();
        }

        public Striper<TClass> CreateStriper() => new Striper<TClass>(typeof(TClass).GetParquetSchema(false));

        public Assembler<TClass> CreateAssembler() {
            ParquetSchema schema = typeof(TClass).GetParquetSchema(true);
            DataField[] dataFields = schema.GetDataFields();
            var result = new Assembler<TClass>(schema);
            result.FieldAssemblers.AddRange(dataFields.Select(df => new FieldAssembler<TClass>(df, CreateAssembler(df, schema, out Expression code), code)));
            return result;
        }
    }
}
