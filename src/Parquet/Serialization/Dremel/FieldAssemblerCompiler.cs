using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    class FieldAssemblerCompiler<TClass> {

        private static readonly Expression Zero = Expression.Constant(0);
        private static readonly Expression One = Expression.Constant(1);

        private readonly ParquetSchema _schema;
        private readonly DataField _df;

        private readonly bool _isUntypedClass = typeof(TClass) == typeof(Dictionary<string, object>);

        private readonly ParameterExpression _dcParam = Expression.Parameter(typeof(DataColumn), "dc");

        private readonly ParameterExpression _classElementVar = Expression.Variable(typeof(TClass), "curr");

#if DEBUG
        private readonly MethodInfo _injectLevelDebugMethod;
        private readonly MethodInfo _messageDebugMethod;
        private int _debugWrapIdx = 0;

        // control whether to inject debug code blocks (you might not want them in debug mode all the time)
        private readonly bool _enableDebug = (Environment.GetEnvironmentVariable("PARQUET_NET_ENABLE_DEBUG") != null)
            || false;    
#endif


        #region [ Data Pointers ]

        private readonly ParameterExpression _dataIdxVar = Expression.Variable(typeof(int), "dataIdx");
        private readonly ParameterExpression _dataVar;
        private readonly ParameterExpression _dataElementVar;

        private readonly ParameterExpression _rlIdxVar = Expression.Variable(typeof(int), "rlIdx");
        private readonly ParameterExpression _rlVar = Expression.Variable(typeof(int), "rl");

        private readonly ParameterExpression _dlIdxVar = Expression.Variable(typeof(int), "dlIdx");
        private readonly ParameterExpression _dlVar = Expression.Variable(typeof(int), "dl");

        private readonly ParameterExpression _hasData = Expression.Variable(typeof(bool), "hasData");

        #endregion

        // Repetition State Machine.
        // Stores collection indexes for each repetition level on where to operate on.
        // Size of the machine is Max Repetition Level (level 0  is a special case that clears entire machine).
        private readonly ParameterExpression _rsmVar = Expression.Variable(typeof(int[]), "rsm");

        private readonly bool _hasReps;
        private readonly bool _hasDefs;


        public FieldAssemblerCompiler(ParquetSchema schema, DataField df) {
            _schema = schema;
            _df = df;

            // expecting non-nullable elements, because definition levels are handled by this algorithm
            _dataVar = Expression.Variable(_df.ClrType.MakeArrayType(), "data");
            _dataElementVar = Expression.Variable(_df.ClrType, "dataElement");

#if DEBUG
            _injectLevelDebugMethod = GetType().GetMethod(nameof(InjectLevelDebug), BindingFlags.NonPublic | BindingFlags.Static)!;
            _messageDebugMethod = GetType().GetMethod(nameof(MessageDebug), BindingFlags.NonPublic | BindingFlags.Static)!;
#endif
            _hasReps = df.MaxRepetitionLevel > 0;
            _hasDefs = df.MaxDefinitionLevel > 0;
        }

        private Expression GetDataLength() {
            return Expression.Property(Expression.Property(_dcParam, nameof(DataColumn.DefinedData)), nameof(Array.Length));
        }

        private Expression GetRlLength() {
            return Expression.Property(Expression.Property(_dcParam, nameof(DataColumn.RepetitionLevels)), nameof(Array.Length));
        }

        private Expression GetDLLength() {
            return Expression.Property(Expression.Property(_dcParam, nameof(DataColumn.DefinitionLevels)), nameof(Array.Length));
        }

        private Expression GetRLAt(Expression index) {
            return Expression.ArrayAccess(Expression.Property(_dcParam, nameof(DataColumn.RepetitionLevels)), index);
        }

        private Expression GetDLAt(Expression index) {
            return Expression.ArrayAccess(Expression.Property(_dcParam, nameof(DataColumn.DefinitionLevels)), index);
        }


        private Expression GetCurrentRLOr0() {
            return _hasReps
                ? Expression.Condition(
                    Expression.LessThan(_rlIdxVar, GetRlLength()),
                    GetRLAt(_rlIdxVar),
                    Zero)
                : Zero;
        }

        private Expression TakeCurrentValuesAndAdvance() {

            Expression dataIdxLessThanExpr = Expression.LessThan(_dataIdxVar, GetDataLength());

            return Expression.IfThenElse(
                // _dataIdxVar < dcParam.Data.Length || _dlIdxVar < dcParam.DefinitionLevels.Length
                _df.MaxDefinitionLevel > 0
                    ? Expression.Or(
                        Expression.LessThan(_dlIdxVar, GetDLLength()),
                        dataIdxLessThanExpr)
                    : dataIdxLessThanExpr,

                Expression.Block(

                    // get definition level value: _dlVar = _dcParam.DefinitionLevels[dlIdxVar];
                    _df.MaxDefinitionLevel > 0
                        ? Expression.Assign(_dlVar, GetDLAt(Expression.PostIncrementAssign(_dlIdxVar)))
                        : Expression.Empty(),

                    // get array value, but only if definiton level is right
                    // _dataElementVar = _dataVar[_dataIdxVar];
                    _df.MaxDefinitionLevel > 0
                        ? Expression.IfThen(
                            Expression.Equal(_dlVar, Expression.Constant(_df.MaxDefinitionLevel)),
                            Expression.Assign(_dataElementVar, Expression.ArrayAccess(_dataVar, Expression.PostIncrementAssign(_dataIdxVar))))
                        : Expression.Assign(_dataElementVar, Expression.ArrayAccess(_dataVar, Expression.PostIncrementAssign(_dataIdxVar))),

                    // get repetition level value: rlVar = dcParam.RepetitionLevels[rlIndexVar];
                    _df.MaxRepetitionLevel > 0
                        ? Expression.Assign(_rlVar, GetRLAt(Expression.PostIncrementAssign(_rlIdxVar)))
                        : Expression.Empty(),

                    // flag = true
                    Expression.Assign(_hasData, Expression.Constant(true))
                    ),

                // flag = false
                Expression.Assign(_hasData, Expression.Constant(false)));
        }

#if DEBUG
        private static void InjectLevelDebug(string levelPropertyName,
            object value, int dataIdx,
            int dl, int rl,
            int dlDepth, int rlDepth,
            int[] rsm) {

            // dump all the variables to debug output
            Console.WriteLine($"DEBUG: {levelPropertyName} | dataIdx={dataIdx} | dl={dl}/{dlDepth} | rl={rl}/{rlDepth} | value={value} | rsm=[{string.Join(",", rsm)}]");
        }

        private Expression CallInjectLevelDebug(Expression expr,
            Field schemaField,
            int dlDepth, int rlDepth) {
            if(!_enableDebug)
                return expr;

            return Expression.Block(
                Expression.Call(_injectLevelDebugMethod,
                    Expression.Constant(schemaField.Path.ToString()),
                    Expression.Convert(_dataElementVar, typeof(object)),
                    _dataIdxVar,
                    _dlVar,
                    _rlVar,
                    Expression.Constant(dlDepth),
                    Expression.Constant(rlDepth),
                    _rsmVar),
                expr);
        }

        private static void MessageDebug(string message) {
            Console.WriteLine($"DEBUG: {message}");
        }

        private Expression CallMessageDebug(string message) {
            return Expression.Call(_messageDebugMethod, Expression.Constant(message));
        }

        private Expression DebugWrap(Expression expr, string message, bool withEnd = true) {
            if(!_enableDebug)
                return expr;

            int idx = _debugWrapIdx++;

            if(withEnd) {

                return Expression.Block(
                    CallMessageDebug($"{idx}: BEGIN {message}"),
                    expr,
                    CallMessageDebug($"{idx}: END   {message}"));
            } else {
                // if we need to preserve return type
                return Expression.Block(
                    CallMessageDebug($"{idx}: {message}"),
                    expr);
            }
        }
#endif

        /// <summary>
        /// Transitions RSM for current RL iteration
        /// </summary>
        private Expression TransitionRSM() {
            return Expression.IfThenElse(
                Expression.Equal(_rlVar, Zero),
                _rsmVar.ClearArray(),

                Expression.Block(
                    // +1 to current RL
                    Expression.PostIncrementAssign(Expression.ArrayAccess(_rsmVar, Expression.Subtract(_rlVar, One))),
                    // zero out the rest of the elements on the right
                    _rsmVar.ClearArray(_rlVar)));
        }

        private IEnumerable<Expression> CollectionAddNewDefaultItem(
            Expression collection,  Type collectionType,
            Expression element,     Type elementType) {


            if(collectionType.IsGenericIList()) {
                yield return Expression.Assign(element, Expression.New(elementType));
                yield return collection.IListAdd(collectionType, element, elementType);
                yield break;
            }

            if(collectionType.IsGenericIDictionary()) {
                yield return Expression.Assign(element, Expression.New(elementType));
                yield break;

                //if(!collectionType.TryExtractDictionaryType(out Type? keyType, out Type? valueType) || keyType == null || valueType == null) {
                //    throw new NotSupportedException($"Cannot extract key/value types from dictionary type {collectionType}");
                //}

                //Type kvType = typeof(ParquetMapElement<,>).MakeGenericType(keyType, valueType);
                //Type dictType = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
                //ConstructorInfo? ctor = kvType.GetConstructor([dictType]);
                //if(ctor == null) {
                //    throw new NotSupportedException($"Cannot find constructor for type {kvType} that takes parameter of type {dictType}");
                //}

                //yield return Expression.Assign(element, Expression.New(ctor, collection));
            }

            throw new NotSupportedException();
        }

        private Expression CollectionGetItemByIndex(
            Expression collection, Type collectionType,
            Type elementType,
            Expression idx) {

            if(collectionType.IsGenericIList()) {
                PropertyInfo? indexerProperty = collectionType.GetProperty("Item", [typeof(int)]);

                if(indexerProperty == null) {
                    throw new NotSupportedException($"cannot find indexer property on type {collectionType}");
                }

                return Expression.Property(collection, indexerProperty, idx);
            }

            throw new NotSupportedException();
        }

        private Expression AddOrGetCollectionItem(
            Expression collection, int rlDepth,
            Type collectionType, Type elementType) {

            // realistically this method will be called only for IList<T> and IDictionary<K,V>

            ParameterExpression paramIdx = Expression.Variable(typeof(int), "idx");
            ParameterExpression paramResult = Expression.Variable(elementType, "result");
            Expression downcastedCollection = Expression.Convert(collection, collectionType);
            ParameterExpression paramDC = Expression.Variable(collectionType, "coll");
            return Expression.Block(
                [paramIdx, paramResult, paramDC],

                // C#: index = rsm[dlDepth - 1]
                Expression.Assign(paramIdx, Expression.ArrayAccess(_rsmVar, Expression.Constant(rlDepth - 1))),
                Expression.Assign(paramDC, downcastedCollection),

                Expression.IfThenElse(
                    Expression.LessThanOrEqual(paramDC.CollectionCount(collectionType), paramIdx),

                    Expression.Block(CollectionAddNewDefaultItem(paramDC, collectionType, paramResult, elementType)),

                    Expression.Assign(paramResult, CollectionGetItemByIndex(paramDC, collectionType, elementType, paramIdx))),

                paramResult);
        }

        private static Type SanitizeType(Type t, out Type elementType) {
            // handle IDictionary<K,V>
            if(t.TryExtractDictionaryType(out Type? keyType, out Type? valueType)) {
                elementType = typeof(ParquetMapKV<,>).MakeGenericType(keyType!, valueType!);
                return typeof(ParquetMap<,>).MakeGenericType(keyType!, valueType!);
            }

            //// handle IList<IDictionary<K, V>>
            //if(t.TryExtractIListType(out Type? elementType1)) {
            //    if(elementType1!.TryExtractDictionaryType(out Type? kType, out Type? vType)) {
            //        Type dictType = typeof(ParquetMap<,>).MakeGenericType(kType!, vType!);
            //        elementType = dictType;
            //        return typeof(List<>).MakeGenericType(dictType);
            //    }
            //}

            if(t.TryExtractIEnumerableType(out Type? et)) {
                elementType = et!;
            } else {
                elementType = t;
            }
            return t;
        }

        //private static void ReplaceIDictionaryTypes(Type t, out Type dictionaryType, out Type elementType) {
        //    if(!t.TryExtractDictionaryType(out Type? keyType, out Type? valueType)) {
        //        throw new NotSupportedException($"{t} is not a dictionary");
        //    }

        //    dictionaryType = typeof(ParquetDictionary<,>).MakeGenericType(keyType!, valueType!);
        //    elementType = typeof(ParquetDictionary<,>.ParquetDictionaryElement).MakeGenericType(keyType!, valueType!);
        //}

        private static void GetReadLevels(Field f, out int dlDepth, out int rlDepth) {
            if(f is ListField lf && lf.Item.IsAtomic) {
                dlDepth = lf.Item.MaxDefinitionLevel;
                rlDepth = lf.Item.MaxRepetitionLevel;
            } else {
                dlDepth = f.MaxDefinitionLevel;
                rlDepth = f.MaxRepetitionLevel;
            }
        }

        private static Type GetIdealUntypedType(Field f) {
            switch(f.SchemaType) {
                case SchemaType.Data:
                    var df = (DataField)f;
                    if(df.IsArray) {
                        return typeof(List<>).MakeGenericType(df.ClrNullableIfHasNullsType);
                    }
                    return df.ClrNullableIfHasNullsType;
                case SchemaType.Map:
                    var fmap = (MapField)f;
                    return typeof(IDictionary<,>).MakeGenericType(GetIdealUntypedType(fmap.Key), GetIdealUntypedType(fmap.Value));
                case SchemaType.Struct:
                    return typeof(Dictionary<string, object>);
                case SchemaType.List:
                    return typeof(List<>).MakeGenericType(GetIdealUntypedType(((ListField)f).Item));
                default:
                    throw new NotSupportedException($"schema type {f.SchemaType} is not supported");
            }
        }

        record ClassMember(Expression Accessor, Expression IsNull, Type Type);

        private Expression CreateInstance(Type t) {
            Expression r = t.IsArray
                ? Expression.NewArrayBounds(t.GetElementType()!, Zero)
                : Expression.New(t);

#if DEBUG
            r = DebugWrap(r, $"CreateInstance of {t}", false);
#endif

            return r;
        }

        /// <summary>
        /// Re-creates array with one extra element and copies old elements into it, then adds new element at the end
        /// </summary>
        private static Expression RebuildArray(Expression arrayAccessor, Type arrayType, Expression newElement) {

            ParameterExpression na = Expression.Variable(arrayType, "newArray");

            // get array length
            Expression arrayLength = Expression.ArrayLength(arrayAccessor);

            return Expression.Block(
                [na],

                // create new array
                Expression.Assign(na, Expression.NewArrayBounds(arrayType.GetElementType()!, Expression.Add(arrayLength, One))),

                // copy old array to new array
                Expression.Call(
                    typeof(Array).GetMethod(nameof(Array.Copy), new[] { typeof(Array), typeof(Array), typeof(int) })!,
                    arrayAccessor,
                    na,
                    arrayLength),

                // add new element to the end
                Expression.Assign(Expression.ArrayAccess(na, arrayLength), newElement),

                na);
        }
        private ClassMember GetClassMember(Type rootType, Expression rootVar, Field? parentField, Field field, string name) {
            if(_isUntypedClass) {
                Type type = GetIdealUntypedType(field);
                Expression accessor;
                Expression isNull;

                // is this a key or a value?
                if(parentField != null && parentField.SchemaType == SchemaType.Map && field.SchemaType == SchemaType.Data) {
                    accessor = Expression.Property(rootVar, field.Name);
                    isNull = Expression.Equal(Expression.Convert(accessor, typeof(object)), Expression.Constant(null));
                } else {
                    accessor = Expression.Property(rootVar, "Item", Expression.Constant(name));
                    MethodInfo ckm = rootType.GetMethod("ContainsKey")!;
                    isNull = Expression.Not(Expression.Call(rootVar, ckm, Expression.Constant(name)));
                }

                return new ClassMember(accessor,
                    isNull,
                    type);

            } else {

                // Dictionary is a special case, because it cannot be constructed independently in one go, so the client needs to know it a dictionary

                Type type = rootType;
                Expression accessor = rootVar;

                if(type.IsSystemNullable()) {
                    type = type.GetNonNullable();
                    accessor = Expression.Property(accessor, "Value");
                }

                PropertyInfo? pi = type.GetProperty(name);
                if(pi != null) {
                    type = pi.PropertyType;
                    accessor = Expression.Property(accessor, name);
                }

                if(pi == null) {
                    FieldInfo? fi = rootType.GetField(name);
                    if(fi != null) {
                        type = fi.FieldType;
                        accessor = Expression.Field(accessor, name);
                    }
                }

                if(type == null || accessor == null) {
                    throw new NotSupportedException($"There is no property of field called '{name}'.");
                }

                bool isGenericDictionary = type.IsGenericIDictionary();

                if(isGenericDictionary) {
                    // Tell the pipeline we *expect* ParquetDictionary<,>
                    //ReplaceIDictionaryTypes(type, out Type expectedDictType, out _);
                    Type expectedDictType = SanitizeType(type, out _);

                    // Treat "null OR wrong runtime type" as uninitialized
                    BinaryExpression isNullOrWrongType =
                        Expression.OrElse(
                            Expression.Equal(accessor, Expression.Constant(null, type)),
                            Expression.IsFalse(Expression.TypeIs(accessor, expectedDictType)));

                    return new ClassMember(
                        accessor,
                        isNullOrWrongType,
                        expectedDictType);
                }

                return new ClassMember(accessor,
                    type.IsValueType
                        ? Expression.Constant(false)
                        : Expression.Equal(accessor, Expression.Constant(null)),
                    type);
            }
        }


        private Expression AssembleRecord(
            Expression rootVar, Type rootType, Field? parentField,
            IReadOnlyCollection<Field> schemaPath) {

            Field schemaField = schemaPath.First();
            GetReadLevels(schemaField, out int dlDepth, out int rlDepth);

            bool isRepeated = schemaField.IsCollection;
            bool isAtomic = schemaField.IsAtomicFieldOrCollectionItem;

            // ----

            Expression currentVar;
            Type currentVarType;
            Expression? nullCheck;
            string nameTag = schemaField.Path.ToString("_", null);
            if(parentField != null && parentField.SchemaType == SchemaType.List) {
                currentVarType = rootType;
                currentVar = rootVar;
                nullCheck = null;
            } else {
                string levelPropertyName = schemaField.ClrPropName ?? schemaField.Name;
                ClassMember classProperty = GetClassMember(rootType, rootVar, parentField, schemaField, levelPropertyName);
                currentVarType = SanitizeType(classProperty.Type, out _);
                currentVar = classProperty.Accessor;
                nullCheck = classProperty.IsNull;
            }

            // ----
            
            Expression iteration = Expression.Empty();

            if(isRepeated) {
                Expression rsmAccess = Expression.ArrayAccess(_rsmVar, Expression.Constant(rlDepth - 1));
                currentVarType = SanitizeType(currentVarType, out Type collectionElementType);

                //bool isDictionary = schemaField.SchemaType == SchemaType.Map;
                //Type collectionElementType;
                //if(isDictionary) {
                //    ReplaceIDictionaryTypes(currentVarType, out Type newDictionaryType, out collectionElementType);
                //    currentVarType = newDictionaryType;
                //} else {
                //    collectionElementType = currentVarType.ExtractElementTypeFromEnumerableType();
                //}

                Expression leafExpr;

                if(isAtomic) {
                    if(currentVarType.IsArray) {
                        // add element to array
                        leafExpr = Expression.Assign(
                            currentVar,
                            RebuildArray(currentVar, currentVarType, Expression.Convert(_dataElementVar, collectionElementType)));
                    } else {
                        // add element to collection - end here
                        leafExpr = Expression.Call(Expression.Convert(currentVar, currentVarType),
                            currentVarType.GetMethod(nameof(IList.Add))!,
                            Expression.Convert(_dataElementVar, collectionElementType));
                    }

#if DEBUG
                    leafExpr = DebugWrap(leafExpr, "atomic");
#endif
                } else {
                    // Map is also repeated type, but key and value cannot be constructed independently.
                    ParameterExpression collectionElementVar = Expression.Variable(
                        collectionElementType, $"collElement_{nameTag}");
                    leafExpr = Expression.Block(
                        [collectionElementVar],

                        Expression.Assign(collectionElementVar,
                            AddOrGetCollectionItem(currentVar, rlDepth, currentVarType, collectionElementType)),

                        // keep traversing the tree
                        AssembleRecord(collectionElementVar, collectionElementType,
                            schemaField, schemaField.NextDotPropertyPath(schemaPath)));

#if DEBUG
                    leafExpr = DebugWrap(leafExpr, "non-atomic");
#endif
                }

                iteration = leafExpr;

#if DEBUG
                iteration = DebugWrap(iteration, "repeated");
#endif

            } else {
                if(isAtomic) {
                    // conversion compensates for nullable types and maybe implicit conversions (below transforms "_dataElementVar" to "(classPropertyType.Type)_dataElementVar")
                    UnaryExpression x = _isUntypedClass && parentField?.SchemaType != SchemaType.Map
                        ? Expression.Convert(_dataElementVar, typeof(object))
                        : Expression.Convert(_dataElementVar, currentVarType);

                    // C#: if(dlDepth == _dlVar) currentVar = x;
                    iteration =
                        Expression.IfThen(
                            Expression.Equal(Expression.Constant(dlDepth), _dlVar),
                            Expression.Assign(currentVar, x)
                        );

#if DEBUG
                    iteration = DebugWrap(iteration, "atomic");
#endif
                } else {
                    ParameterExpression deepVar = Expression.Variable(currentVarType);

                    iteration = Expression.Block(
                        [deepVar],

                        Expression.Assign(deepVar, Expression.Convert(currentVar, currentVarType)),

                        AssembleRecord(deepVar, currentVarType,
                            schemaField,
                            schemaField.NextDotPropertyPath(schemaPath)));

#if DEBUG
                    iteration = DebugWrap(iteration, "non-atomic");
#endif
                }

#if DEBUG
                iteration = DebugWrap(iteration, "non-repeated");
#endif
            }

            // know when to stop
            if(!isAtomic || isRepeated) {

                var x = new List<Expression>();
                if(nullCheck != null) {
                    x.Add(Expression.IfThen(nullCheck, Expression.Assign(currentVar, CreateInstance(currentVarType))));
                }
                x.Add(iteration);

                iteration = Expression.IfThen(

                    // C#: _dlVar >= dlDepth?
                    Expression.GreaterThanOrEqual(_dlVar, Expression.Constant(dlDepth)),

                    x.Count == 1 ? x.First() : Expression.Block(x));

                if(isRepeated) {
                    x.Clear();
                    if(nullCheck != null) {
                        x.Add(Expression.IfThen(
                                nullCheck,
                                Expression.Assign(currentVar, CreateInstance(currentVarType))));
                    }
                    x.Add(iteration);

                    iteration = Expression.IfThen(
                        Expression.GreaterThanOrEqual(_dlVar, Expression.Constant(dlDepth - 1)),
                        x.Count == 1 ? x.First() : Expression.Block(x));
                }
            }

#if DEBUG
            iteration = CallInjectLevelDebug(
                iteration,
                schemaField,
                dlDepth,
                rlDepth);
#endif

            return iteration;
        }

        private Expression InjectColumn() {
            LabelTarget rlBreakLabel = Expression.Label();

            // process current value tuple (_dataVar, _dlVar, _rlVar)
            IReadOnlyCollection<Field> dataFieldPath = _df.BuildClrPath(_schema);
            Expression body =
                AssembleRecord(_classElementVar, typeof(TClass), null, dataFieldPath);

            return Expression.Block(

                Expression.Loop(Expression.Block(
                    TakeCurrentValuesAndAdvance(),

                    // break out if no values available
                    Expression.IfThen(Expression.IsFalse(_hasData), Expression.Break(rlBreakLabel)),

                    _hasReps
                        ? TransitionRSM()
                        : Expression.Empty(),

                    // only proceed when value if defined (if definition levels are used)
                    //_df.MaxDefinitionLevel > 0
                    //    ? Expression.IfThen(Expression.Equal(_dlVar, Expression.Constant(_df.MaxDefinitionLevel)), body)
                    //    : body,
                    body,


                    // be careful to check for NEXT RL, not the current one
                    // repeat until RL == 0 (always zero for non-repeated fields so we are OK here in any situation)
                    Expression.IfThen(
                        Expression.Equal(GetCurrentRLOr0(), Zero),
                        Expression.Break(rlBreakLabel))

                    ), rlBreakLabel));
        }

        public FieldAssembler<TClass> Compile() {
            ParameterExpression classesParam = Expression.Parameter(typeof(IEnumerable<TClass>), "classes");
            
            Expression iteration = InjectColumn();

            BlockExpression block = Expression.Block(
                [_classElementVar,
                    _dataVar, _dataIdxVar, _dataElementVar, _dlIdxVar, _dlVar, _rlIdxVar, _rlVar, _hasData, _rsmVar],

                // initialise array vars
                Expression.Assign(_dataVar,
                    Expression.Convert(Expression.Property(_dcParam, nameof(DataColumn.DefinedData)), _df.ClrType.MakeArrayType())),
                Expression.Assign(_dataIdxVar, Zero),
                Expression.Assign(_rlIdxVar, Zero),
                Expression.Assign(_rlVar, Zero),
                Expression.Assign(_dlIdxVar, Zero),
                Expression.Assign(_dlVar, Zero),

                // allocate state machine
                Expression.Assign(_rsmVar, Expression.NewArrayBounds(typeof(int), Expression.Constant(_df.MaxRepetitionLevel))),

                iteration.Loop(classesParam, typeof(TClass), _classElementVar));

            return new FieldAssembler<TClass>(_schema, _df,
                Expression.Lambda<Action<IEnumerable<TClass>, DataColumn>>(block, classesParam, _dcParam).Compile(),
                block, iteration);
        }
    }
}
