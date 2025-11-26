using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Parquet.Extensions;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    class FieldStriperCompiler<TClass> {

        private static readonly MethodInfo LevelsAddMethod =
            typeof(List<int>).GetMethod(nameof(IList.Add))!;
        private static readonly MethodInfo IDictionaryTryGetValueMethod = 
            typeof(IDictionary<string, object>).GetMethod("TryGetValue")!;
        private readonly MethodInfo _valuesListAddMethod;
        private readonly bool _isUntypedClass = typeof(TClass) == typeof(IDictionary<string, object>);

        private readonly ParquetSchema _schema;
        private readonly DataField _df;
        private readonly bool _hasRls;
        private readonly bool _hasDls;

        // input parameters
        private readonly ParameterExpression _dfParam = Expression.Parameter(typeof(DataField), "df");
        private readonly ParameterExpression _classesParam = Expression.Parameter(typeof(IEnumerable<TClass>), "classes");
        private static readonly ConstructorInfo ShreddedColumnConstructor =
            typeof(ShreddedColumn).GetConstructor(BindingFlags.Instance | BindingFlags.Public, null,
                CallingConventions.HasThis,
                new[] { typeof(Array), typeof(List<int>), typeof(List<int>) },
                null)!;

        // create lists for values, definition levels and repetition levels
        private readonly Type _valuesListType;
        private readonly ParameterExpression _valuesVar;
        private readonly ParameterExpression _dlsVar;
        private readonly ParameterExpression _rlsVar;

        // currently iterated class element
        private readonly ParameterExpression _classElementVar = Expression.Variable(typeof(TClass), "curr");

        private static readonly Expression NullListOfInt = Expression.Convert(Expression.Constant(null), typeof(List<int>));

        public FieldStriperCompiler(ParquetSchema schema, DataField df) {

            _schema = schema;
            _df = df;
            _hasRls = _df.MaxRepetitionLevel > 0;
            _hasDls = _df.MaxDefinitionLevel > 0;

            //
            _valuesListType = typeof(List<>).MakeGenericType(df.ClrType);
            _valuesVar = Expression.Variable(_valuesListType, "values");
            _dlsVar = Expression.Variable(typeof(List<int>), "dls");
            _rlsVar = Expression.Variable(typeof(List<int>), "rls");

            //
            _valuesListAddMethod = typeof(List<>).MakeGenericType(_df.ClrType).GetMethod(nameof(IList.Add))!;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="valueVar"></param>
        /// <param name="dl">Definition level if value is defined. For optional atoms that are null it must be -1.</param>
        /// <param name="currentRlVar"></param>
        /// <param name="isLeaf"></param>
        /// <param name="isAtomic">Value is atomic i.e. having real data value and not just RLs and DLs</param>
        /// <returns></returns>
        private Expression WriteValue(ParameterExpression valueVar,
            int dl, Expression currentRlVar,
            ParameterExpression isLeaf, bool isAtomic) {

            if(isAtomic) {
                if(_df.IsNullable) {

                    Expression getNonNullValue = _df.ClrNullableIfHasNullsType.IsSystemNullable()
                        ? Expression.Property(valueVar, "Value")
                        : valueVar;

                    // cast if required
                    getNonNullValue = Expression.Convert(getNonNullValue, _df.ClrType);

                    return Expression.IfThenElse(
                        // value == null?
                        Expression.Equal(valueVar, Expression.Constant(null)),

                        // only need RL and DL-1
                        Expression.Block(
                            _hasDls ? Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl - 1)) : Expression.Empty(),
                            _hasRls ? Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar) : Expression.Empty()),

                        // everything, but value must be non-null
                        Expression.Block(
                            Expression.Call(_valuesVar, _valuesListAddMethod, getNonNullValue),
                            _hasDls ? Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl)) : Expression.Empty(),
                            _hasRls ? Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar) : Expression.Empty()));

                } else {

                    // cast if required
                    UnaryExpression converted = Expression.Convert(valueVar, _df.ClrType);

                    // required atomics are simple - add value, RL and DL as is
                    return Expression.Block(
                        Expression.Call(_valuesVar, _valuesListAddMethod, converted),
                        _hasDls ? Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl)) : Expression.Empty(),
                        _hasRls ? Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar) : Expression.Empty());
                }
            }


            // non-atomics still need RL and DL dumped
            return Expression.Block(
                _hasDls
                    ? Expression.Call(_dlsVar, LevelsAddMethod,
                        Expression.Condition(isLeaf, Expression.Constant(dl -1), Expression.Constant(dl)))
                    : Expression.Empty(),
                _hasRls ? Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar) : Expression.Empty());

        }

        private Expression WriteMissingValue(int dl, Expression currentRlVar) {
            return Expression.Block(
                _hasDls ? Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl)) : Expression.Empty(),
                _hasRls ? Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar) : Expression.Empty());
        }

        /// <summary>
        /// Corresponds to L6-L20 of Dremel paper Appendix A (Column-Striping Algorithm)
        /// </summary>
        private Expression Decode(Expression element, Type elementType,
            bool isAtomic, int dl, ParameterExpression currentRlVar,
            ParameterExpression seenFieldsVar, Field field, int rlDepth, IReadOnlyCollection<Field> chainPath) {

            // dl is DL of current element in path, not end DataField

            string suffix = field.Path.ToString().Replace(".", "_");
            ParameterExpression chRepetitionLevelVar = Expression.Variable(typeof(int), $"chRepetitionLevel_{suffix}");
            ParameterExpression valueVar = Expression.Variable(elementType, $"value_{suffix}");
            ParameterExpression isLeafVar = Expression.Variable(typeof(bool), $"isLeaf_{suffix}");
            return Expression.Block(
                new[] { chRepetitionLevelVar, valueVar, isLeafVar },

                // L8
                Expression.Assign(chRepetitionLevelVar, currentRlVar),

                // L9-13
                Expression.IfThenElse(
                    // if seenFields.Contains(field.Path)
                    //Expression.Call(seenFieldsVar, typeof(HashSet<string>).GetMethod("Contains")!, Expression.Constant(field.Path.ToString())),
                    Expression.IsTrue(seenFieldsVar),

                    // chRepetitionLevelVar = treeDepth
                    Expression.Assign(chRepetitionLevelVar, Expression.Constant(rlDepth)),

                    // seenFields.Add(field.Path)
                    //Expression.Call(seenFieldsVar, typeof(HashSet<string>).GetMethod("Add")!, Expression.Constant(field.Path.ToString()))
                    Expression.Assign(seenFieldsVar, Expression.Constant(true))
                    ),

                // L14-
                Expression.Assign(valueVar, element),

                isAtomic
                    ? Expression.Assign(isLeafVar, Expression.Constant(true))
                    : (elementType.IsValueType && !elementType.IsSystemNullable())
                        ? Expression.Assign(isLeafVar, Expression.Constant(false))
                        : Expression.Assign(isLeafVar, valueVar.IsNull()),

                Expression.IfThenElse(
                    Expression.IsTrue(isLeafVar),
                    WriteValue(valueVar, dl, chRepetitionLevelVar, isLeafVar, isAtomic),
                    isAtomic
                        ? Expression.Empty()
                        : DissectRecord(valueVar, elementType, field, field.NextDotPropertyPath(chainPath), rlDepth, chRepetitionLevelVar)
                )

            );
        }

        private static Type ExtractElementTypeFromEnumerableType(Type t) {
            if(t.TryExtractDictionaryType(out Type? keyType, out Type? valueType))
                return typeof(KeyValuePair<,>).MakeGenericType(keyType!, valueType!);

            if(t.TryExtractIEnumerableType(out Type? iet))
                return iet!;

            throw new ArgumentException($"type {t} is not single-element generic enumerable", nameof(t));
        }

        private static int GetWriteableDL(Field f) {
            if(f is ListField lf && lf.Item.IsAtomic)
                return lf.Item.MaxDefinitionLevel;

            return f.MaxDefinitionLevel;
        }

        private static int GetNullCollectionDL(Field f)
        {
            return f.MaxDefinitionLevel - 2;    
        }
        
        private static int GetEmptyCollectionDL(Field f) {
            return f.MaxDefinitionLevel - 1;
        }

        private static Type GetIdealUntypedType(Field f) {
            switch(f.SchemaType) {
                case SchemaType.Data:
                    return ((DataField)f).ClrNullableIfHasNullsType;
                case SchemaType.Map:
                    var fmap = (MapField)f;
                    return typeof(IDictionary<,>).MakeGenericType(GetIdealUntypedType(fmap.Key), GetIdealUntypedType(fmap.Value));
                case SchemaType.Struct:
                    return typeof(IDictionary<string, object>);
                case SchemaType.List:
                    return typeof(IEnumerable<>).MakeGenericType(GetIdealUntypedType(((ListField)f).Item));
                default:
                    throw new NotSupportedException($"schema type {f.SchemaType} is not supported");
            }
        }

        private Expression GetClassMemberAccessorAndType(
            Type rootType,
            Expression rootVar,
            SchemaType? parentSchemaType,
            Field field,
            string name,
            out Type type) {
            if(_isUntypedClass) {

                if(parentSchemaType != null && parentSchemaType == SchemaType.Map && field.SchemaType == SchemaType.Data) {
                    type = GetIdealUntypedType(field);
                    return Expression.Property(rootVar, name);
                }

                type = GetIdealUntypedType(field);

                /*
                 * Take into account that key may not be present in the dictionary.
                 * In this case, code would look like:
                 * 
                 * object value;
                 * return dict.TryGetValue(name, out value) ? (T)value : default(T);
                 */

                ParameterExpression retVal = Expression.Variable(typeof(object), "value");
                return Expression.Block(
                    new[] { retVal },
                    
                    Expression.Condition(
                        Expression.Call(rootVar, IDictionaryTryGetValueMethod, Expression.Constant(name), retVal),
                        Expression.Convert(retVal, type),
                        Expression.Default(type)));
            }

            Expression? result = rootVar;
            type = rootType;

            if(rootType.IsSystemNullable()) {
                result = Expression.Property(result, "Value");
                type = rootType.GetNonNullable();
            }

            PropertyInfo? pi = type.GetProperty(name);
            FieldInfo? fi = type.GetField(name);

            if(pi != null) {
                type = pi.PropertyType;
                result = Expression.Property(result, name);
            } else if(fi != null) {
                type = fi.FieldType;
                result = Expression.Field(result, name);
            } else {
                throw new NotSupportedException($"Failed to look up {field}: there is no class property of field called '{name}' in '{type}'.");
            }

            return result;
        }

        private Expression DissectRecord(
            Expression rootVar, Type rootType, Field? parentField,
            IReadOnlyCollection<Field> schemaPath,
            int rlDepth, ParameterExpression currentRlVar) {

            // walk schema, not class instance
            // this means value must be propagated down the tree, even if it's not present

            Field schemaField = schemaPath.First();
            int dl = GetWriteableDL(schemaField);

            bool isRepeated = schemaField.IsCollection;
            bool isAtomic = schemaField.IsAtomicFieldOrCollectionItem;
            if(isRepeated)
                rlDepth += 1;

            // while "decoder" (L5)

            // We need to build "while has more field values" loop, which in case of atomics is simple - just one value per record, so it's not even worth looping.
            // For collections, we need to iterate over the elements, however the body is the same as for atomics.
            // What is the element type in the loop? for atomics it's the field type, for collections - the item type (child)
            // - if it's repeated, just iterate over the collection
            // - if it's not repeated, we can do single iteration with one element being the field

            Expression currentVar;
            Type currentVarType;
            string nameTag = schemaField.Path.ToString();
            if(parentField != null && parentField.SchemaType == SchemaType.List) {
                // no need to push into class hierarchy - we are already at collection item level
                currentVar = rootVar;
                currentVarType = rootType;
            } else {
                string levelPropertyName = schemaField.ClrPropName ?? schemaField.Name;
                currentVar = GetClassMemberAccessorAndType(rootType, rootVar,
                    parentField?.SchemaType, schemaField, levelPropertyName, out currentVarType);
            }

            ParameterExpression seenVar = Expression.Variable(typeof(bool), $"seen_{nameTag}");

            Expression body;
            if(isRepeated) {
                Type collectionElementType = ExtractElementTypeFromEnumerableType(currentVarType);
                Expression collection = currentVar;
                ParameterExpression elementHolder = Expression.Variable(collectionElementType, $"element_{nameTag}");
                ParameterExpression loopCounter = Expression.Variable(typeof(int), $"count_{nameTag}");
                Expression elementProcessor = Decode(elementHolder, collectionElementType,
                    isAtomic, dl, currentRlVar, seenVar, schemaField, rlDepth,
                    schemaPath);
                body = elementProcessor.Loop(collection, collectionElementType, elementHolder, loopCounter);

                // if levelProperty (collection) is null, we need extra iteration with null value (which rep and def level?)
                // we do this iteration with non-collection condition below, so need to be done for collection as well.
                body = Expression.Block(
                    [loopCounter],
                    Expression.Assign(loopCounter, Expression.Constant(0)),
                    Expression.IfThenElse(
                        Expression.Equal(currentVar, Expression.Constant(null)),
                        WriteMissingValue(GetNullCollectionDL(schemaField), currentRlVar),
                        Expression.Block(
                            body,
                            // check if no elements are written and write out empty list if so
                            Expression.IfThen(
                                Expression.Equal(loopCounter, Expression.Constant(0)),
                                WriteMissingValue(GetEmptyCollectionDL(schemaField), currentRlVar))
                            )));
            } else {
                body = Decode(currentVar, currentVarType,
                    isAtomic, dl, currentRlVar, seenVar, schemaField, rlDepth, schemaPath);
            }

            return Expression.Block(
                [seenVar],
                Expression.Assign(seenVar, Expression.Constant(false)),
                body);
        }

        public FieldStriper<TClass> Compile() {

            ParameterExpression currentRepetitionLevel = Expression.Variable(typeof(int), "currentRl");

            IReadOnlyCollection<Field> dataFieldPath = _df.BuildExperimentalPath(_schema);

            // compile iteration to shred one class instance (it can contain multiple values)
            Expression iteration = DissectRecord(
                _classElementVar, typeof(TClass),
                null,
                dataFieldPath,
                0, currentRepetitionLevel);
            Expression iterationLoop = iteration.Loop(_classesParam, typeof(TClass), _classElementVar);

            BlockExpression block = Expression.Block(
                [_valuesVar, _dlsVar, _rlsVar, _classElementVar, currentRepetitionLevel],

                Expression.Assign(currentRepetitionLevel, Expression.Constant(0)),

                // init 3 building blocks
                Expression.Block(
                    Expression.Assign(_valuesVar, Expression.New(_valuesListType)),
                    Expression.Assign(_dlsVar, _hasDls ? Expression.New(typeof(List<int>)) : NullListOfInt),
                    Expression.Assign(_rlsVar, _hasRls ?  Expression.New(typeof(List<int>)) : NullListOfInt)),

                iterationLoop,

                // result: use triple to construct ShreddedColumn and return (last element in the block)
                Expression.New(ShreddedColumnConstructor,
                    Expression.Call(_valuesVar, _valuesListType.GetMethod("ToArray")!),
                        _dlsVar,
                        _rlsVar)
                );

            Func<DataField, IEnumerable<TClass>, ShreddedColumn> lambda = Expression
                .Lambda<Func<DataField, IEnumerable<TClass>, ShreddedColumn>>(block, _dfParam, _classesParam)
                .Compile();

            return new FieldStriper<TClass>(_schema, _df, lambda, block, iteration);
        }
    }
}
