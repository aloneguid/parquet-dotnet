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

        private static void Discover(Field field, out bool isRepeated) {
            isRepeated =
                field.SchemaType == SchemaType.List ||
                field.SchemaType == SchemaType.Map ||
                (field.SchemaType == SchemaType.Data && field is DataField rdf && rdf.IsArray);
        }

#if DEBUG
        private static void InjectLevelDebug(string levelPropertyName,
            object value, int dataIdx,
            int dl, int rl,
            int dlDepth, int rlDepth,
            int[] rsm) {
            Console.WriteLine("debug");
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

        private Expression GetCollectionElement(Expression collection, int rlDepth,
            Type collectionType, Type elementType) {
            ParameterExpression indexVar = Expression.Variable(typeof(int), "index");
            ParameterExpression resultElementVar = Expression.Variable(elementType, "resultElement");
            Expression downcastedCollection = Expression.Convert(collection, collectionType);
            return Expression.Block(
                new[] { indexVar, resultElementVar },

                // C#: index = rsm[dlDepth - 1]
                Expression.Assign(indexVar, Expression.ArrayAccess(_rsmVar, Expression.Constant(rlDepth - 1))),

                Expression.IfThenElse(
                    Expression.LessThanOrEqual(downcastedCollection.CollectionCount(collectionType), indexVar),

                    Expression.Block(
                        Expression.Assign(resultElementVar, Expression.New(elementType)),
                        downcastedCollection.CollectionAdd(collectionType, resultElementVar, elementType)),

                    Expression.Assign(resultElementVar, Expression.Property(downcastedCollection, "Item", indexVar))
                    ),

                resultElementVar);
        }

        private static void ReplaceIDictionaryTypes(Type t, out Type dictionaryType, out Type elementType) {
            if(!t.TryExtractDictionaryType(out Type? keyType, out Type? valueType)) {
                throw new NotSupportedException($"{t} is not a dictionary");
            }

            dictionaryType = typeof(ParquetDictionary<,>).MakeGenericType(keyType!, valueType!);
            elementType = typeof(ParquetDictionary<,>.ParquetDictionaryElement).MakeGenericType(keyType!, valueType!);
        }

        private static void GetReadLevels(Field f, out int dlDepth, out int rlDepth) {
            if(f is ListField lf && lf.IsAtomic) {
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

        record ClassMember(Expression Accessor,
            Expression IsNull,
            Type Type,
            bool IsGenericDictionary,
            Expression CreateNew);

        private static Expression CreateInstance(Type t) {
            if(t.IsArray) {
                // create an empty array
                return Expression.NewArrayBounds(t.GetElementType()!, Zero);
            }

            return Expression.New(t);
        }

        private static Expression RebuildArray(Expression arrayAccessor, Type arrayType, Expression newElement) {

            ParameterExpression na = Expression.Variable(arrayType, "newArray");

            // get array length
            Expression arrayLength = Expression.ArrayLength(arrayAccessor);

            return Expression.Block(
                new[] { na },

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

        private ClassMember GetClassMember(Type rootType, Expression rootVar,
            Field? parentField, Field field,
            string name) {
            if(_isUntypedClass) {
                Type type = GetIdealUntypedType(field);
                bool isGenericDictionary = field.SchemaType == SchemaType.Map;
                Expression accessor;
                Expression isNull;

                // is this a key or a value?
                if(parentField != null && parentField.SchemaType == SchemaType.Map && field.SchemaType == SchemaType.Data) {
                    accessor = Expression.Property(rootVar, field.Name);
                    isNull = Expression.Equal(accessor, Expression.Constant(null));

                } else {
                    accessor = Expression.Property(rootVar, "Item", Expression.Constant(name));
                    MethodInfo ckm = rootType.GetMethod("ContainsKey")!;
                    isNull = Expression.Not(Expression.Call(rootVar, ckm, Expression.Constant(name)));
                }

                return new ClassMember(accessor,
                    isNull,
                    type,
                    isGenericDictionary,
                    Expression.Empty());

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
                return new ClassMember(accessor,
                    type.IsValueType
                        ? Expression.Constant(false)
                        : Expression.Equal(accessor, Expression.Constant(null)),
                    type,
                    isGenericDictionary,
                    Expression.Empty());
            }
        }


        private Expression InjectLevel(Expression rootVar, Type rootType, Field? parentField, Field[] levelFields, List<string> path) {

            string currentPathPart = path.First();
            Field? field = levelFields.FirstOrDefault(x => x.Name == currentPathPart);
            if(field == null)
                throw new NotSupportedException($"field '{currentPathPart}' not found");

            GetReadLevels(field, out int dlDepth, out int rlDepth);

            Discover(field, out bool isRepeated);
            bool isAtomic = field.IsAtomic;
            string levelPropertyName = field.ClrPropName ?? field.Name;

            ClassMember classProperty = GetClassMember(rootType, rootVar, parentField, field, levelPropertyName);   

            Expression iteration = Expression.Empty();

            if(isRepeated) {
                Expression rsmAccess = Expression.ArrayAccess(_rsmVar, Expression.Constant(rlDepth - 1));

                Type levelPropertyElementType;
                if(classProperty.IsGenericDictionary) {
                    ReplaceIDictionaryTypes(classProperty.Type, out Type newType, out levelPropertyElementType);
                    classProperty = classProperty with { Type = newType };
                } else {
                    levelPropertyElementType = classProperty.Type.ExtractElementTypeFromEnumerableType();
                }

                Expression leafExpr;

                if(isAtomic) {

                    if(classProperty.Type.IsArray) {

                        // add element to array
                        leafExpr = Expression.Assign(
                            classProperty.Accessor,
                            RebuildArray(classProperty.Accessor, classProperty.Type, Expression.Convert(_dataElementVar, levelPropertyElementType)));
                    } else {

                        // add element to collection - end here
                        leafExpr = Expression.Call(Expression.Convert(classProperty.Accessor, classProperty.Type),
                            classProperty.Type.GetMethod(nameof(IList.Add))!,
                            Expression.Convert(_dataElementVar, levelPropertyElementType));
                    }

                } else {

                    // Map is also repeated type, but key and value cannot be constructed independently.

                    ParameterExpression collectionElementVar = Expression.Variable(levelPropertyElementType, "collectionElement");
                    leafExpr = Expression.Block(
                        new[] { collectionElementVar },

                        Expression.Assign(collectionElementVar,
                            GetCollectionElement(classProperty.Accessor, rlDepth, classProperty.Type, levelPropertyElementType)),

                        // keep traversing the tree
                        InjectLevel(collectionElementVar, levelPropertyElementType,
                            field, field.NaturalChildren, field.GetNaturalChildPath(path))

                        );
                }

                iteration = leafExpr;

            } else {
                if(isAtomic) {

                    // conversion compensates for nullable types and maybe implicit conversions
                    UnaryExpression x = _isUntypedClass && parentField?.SchemaType != SchemaType.Map
                        ? Expression.Convert(_dataElementVar, typeof(object))
                        : Expression.Convert(_dataElementVar, classProperty.Type);

                    // C#: dlDepth == _dlVar?
                    iteration =
                        Expression.IfThen(
                            Expression.Equal(Expression.Constant(dlDepth), _dlVar),
                            // levelProperty = (levelPropertyType)_dataElementVar
                            Expression.Assign(classProperty.Accessor, x)
                        );
                } else {
                    ParameterExpression deepVar = Expression.Variable(classProperty.Type);

                    iteration = Expression.Block(
                        new[] { deepVar },

                        Expression.Assign(deepVar, Expression.Convert(classProperty.Accessor, classProperty.Type)),

                        InjectLevel(deepVar, classProperty.Type,
                            field,
                            field.NaturalChildren,
                            field.GetNaturalChildPath(path)));
                }
            }

            // know when to stop
            if(!isAtomic || isRepeated) {

                iteration = Expression.IfThen(

                    // C#: _dlVar >= dlDepth?
                    Expression.GreaterThanOrEqual(_dlVar, Expression.Constant(dlDepth)),

                    Expression.Block(
                        Expression.IfThen(
                            classProperty.IsNull,
                            Expression.Assign(classProperty.Accessor, CreateInstance(classProperty.Type))),

                        iteration));

                if(isRepeated) {
                    iteration = Expression.IfThen(
                        Expression.GreaterThanOrEqual(_dlVar, Expression.Constant(dlDepth - 1)),
                        Expression.Block(
                            Expression.IfThen(
                                classProperty.IsNull,
                                Expression.Assign(classProperty.Accessor, CreateInstance(classProperty.Type))),

                            iteration));
                }
            }

            return Expression.Block(
#if DEBUG
                Expression.Call(_injectLevelDebugMethod,
                    Expression.Constant(levelPropertyName),
                    Expression.Convert(_dataElementVar, typeof(object)),
                    _dataIdxVar,
                    _dlVar,
                    _rlVar,
                    Expression.Constant(dlDepth),
                    Expression.Constant(rlDepth),
                    _rsmVar),
#endif

                iteration
                );


        }

        private Expression InjectColumn() {
            LabelTarget rlBreakLabel = Expression.Label();

            // process current value tuple (_dataVar, _dlVar, _rlVar)
            Expression body = 
                InjectLevel(_classElementVar, typeof(TClass), null, _schema.Fields.ToArray(), _df.Path.ToList());

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
                new[] { _classElementVar, _dataVar, _dataIdxVar, _dataElementVar, _dlIdxVar, _dlVar, _rlIdxVar, _rlVar, _hasData, _rsmVar },

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

                iteration.Loop(classesParam, typeof(TClass), _classElementVar)
                );

            return new FieldAssembler<TClass>(_schema, _df,
                Expression.Lambda<Action<IEnumerable<TClass>, DataColumn>>(block, classesParam, _dcParam).Compile(),
                block, iteration);
                
        }
    }
}
