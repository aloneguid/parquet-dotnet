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

        private static readonly MethodInfo LevelsAddMethod = typeof(List<int>).GetMethod(nameof(IList.Add))!;
        private readonly MethodInfo _valuesListAddMethod;

        private readonly ParquetSchema _schema;
        private readonly DataField _df;

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

        public FieldStriperCompiler(ParquetSchema schema, DataField df) {

            _schema = schema;
            _df = df;

            //
            _valuesListType = typeof(List<>).MakeGenericType(df.ClrType);
            _valuesVar = Expression.Variable(_valuesListType, "values");
            _dlsVar = Expression.Variable(typeof(List<int>), "dls");
            _rlsVar = Expression.Variable(typeof(List<int>), "rls");

            //
            _valuesListAddMethod = typeof(List<>).MakeGenericType(_df.ClrType).GetMethod(nameof(IList.Add))!;
        }

        private void Discover(Field field, out bool isRepeated, out bool hasDefinition) {
            isRepeated = field.SchemaType ==
                SchemaType.List ||
                (field.SchemaType == SchemaType.Data && field is DataField rdf && rdf.IsArray);

            // The current definitionLevel is uniquely determined by the tree position of the current writer,
            // as the sum of the number of optional and repeated fields in the field’s path.
            hasDefinition = (isRepeated ||
                (field.SchemaType == SchemaType.Struct) ||  // structs are always optional
                (field.SchemaType == SchemaType.Map) ||
                (field.SchemaType == SchemaType.Data && field is DataField ddf && ddf.IsNullable));
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

                    return Expression.IfThenElse(
                        // value == null?
                        Expression.Equal(valueVar, Expression.Constant(null)),

                        // only need RL and DL-1
                        Expression.Block(
                            Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl - 1)),
                            Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar)),

                        // everything, but value must be non-null
                        Expression.Block(
                            Expression.Call(_valuesVar, _valuesListAddMethod, getNonNullValue),
                            Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl)),
                            Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar)));

                } else {
                    // required atomics are simple - add value, RL and DL as is
                    return Expression.Block(
                        Expression.Call(_valuesVar, _valuesListAddMethod, valueVar),
                        Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl)),
                        Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar));
                }
            }

            // non-atomics still need RL and DL dumped
            return Expression.Block(
                Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl)),
                Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar));

        }

        private Expression WriteMissingValue(int dl, Expression currentRlVar) {
            return Expression.Block(
                Expression.Call(_dlsVar, LevelsAddMethod, Expression.Constant(dl)),
                Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar));
        }

        private Expression WhileBody(Expression element, bool isAtomic, int dl, ParameterExpression currentRlVar, ParameterExpression seenFieldsVar, Field field, int rlDepth, Type elementType, List<string> path) {
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
                    Expression.Call(seenFieldsVar, typeof(HashSet<string>).GetMethod("Contains")!, Expression.Constant(field.Path.ToString())),

                    // chRepetitionLevelVar = treeDepth
                    Expression.Assign(chRepetitionLevelVar, Expression.Constant(rlDepth)),

                    // seenFields.Add(field.Path)
                    Expression.Call(seenFieldsVar, typeof(HashSet<string>).GetMethod("Add")!, Expression.Constant(field.Path.ToString()))
                    ),

                // L14-
                Expression.Assign(valueVar, element),

                isAtomic
                    ? Expression.Assign(isLeafVar, Expression.Constant(true))
                    : Expression.Assign(isLeafVar, Expression.Equal(valueVar, Expression.Constant(null))),

                Expression.IfThenElse(
                    Expression.IsTrue(isLeafVar),
                    WriteValue(valueVar, dl, chRepetitionLevelVar, isLeafVar, isAtomic),
                    isAtomic
                        ? Expression.Empty()
                        : DissectRecord(valueVar, field.NaturalChildren, field.GetNaturalChildPath(path), elementType, rlDepth, dl, chRepetitionLevelVar)
                )

            ); ;
        }

        private Expression DissectRecord(
            Expression rootVar,
            Field[] levelFields,
            List<string> path,
            Type rootType,
            int rlDepth,
            int dl,
            ParameterExpression currentRlVar) {

            // walk schema, not class instance
            // this means value must be propagated down the tree, even if it's not present

            string currentPathPart = path.First();
            Field? field = levelFields.FirstOrDefault(x => x.Name == currentPathPart);
            if(field == null)
                throw new NotSupportedException("field not found");

            Discover(field, out bool isRepeated, out bool hasDefinition);
            bool isAtomic = path.Count == 1;
            if(hasDefinition)
                dl += 1;
            if(isRepeated)
                rlDepth += 1;

            // --

            // while "decoder"

            string levelPropertyName = field.ClrPropName ?? field.Name;
            Expression levelProperty = Expression.Property(rootVar, levelPropertyName);
            Type levelPropertyType = rootType.GetProperty(levelPropertyName)!.PropertyType;
            ParameterExpression seenFieldsVar = Expression.Variable(typeof(HashSet<string>), $"seenFieldsVar_{levelPropertyName}");

            Expression extraBody;
            if(isRepeated) {
                Type elementType = levelPropertyType.ExtractElementTypeFromEnumerableType();
                Expression collection = levelProperty;
                ParameterExpression element = Expression.Variable(elementType, "element");
                Expression elementProcessor = WhileBody(element, isAtomic, dl, currentRlVar, seenFieldsVar, field, rlDepth, elementType, path);
                extraBody = elementProcessor.Loop(collection, elementType, element);

                // todo: if levelProperty (collection) is null, we need extra iteration with null value (which rep and def level?)
                // we do this iteration with non-collection condition below, so need to be done for collection as well.
                extraBody = Expression.IfThenElse(
                        Expression.Equal(levelProperty, Expression.Constant(null)),
                        WriteMissingValue(dl - 1, currentRlVar),
                        extraBody);
            } else {
                Expression element = levelProperty;
                extraBody = WhileBody(element, isAtomic, dl, currentRlVar, seenFieldsVar, field, rlDepth, levelPropertyType, path);
            }

            return Expression.Block(
                new[] { seenFieldsVar },
                Expression.Assign(seenFieldsVar, Expression.New(typeof(HashSet<string>))),
                extraBody);
        }

        public FieldStriper<TClass> Compile() {

            ParameterExpression currentRl = Expression.Variable(typeof(int), "currentRl");

            Expression iteration = DissectRecord(_classElementVar, _schema.Fields.ToArray(), _df.Path.ToList(), typeof(TClass), 0, 0, currentRl);
            Expression iterationLoop = iteration.Loop(_classesParam, typeof(TClass), _classElementVar);

            BlockExpression block = Expression.Block(
                new[] { _valuesVar, _dlsVar, _rlsVar, _classElementVar, currentRl },

                Expression.Assign(currentRl, Expression.Constant(0)),

                // init 3 building blocks
                Expression.Block(
                    Expression.Assign(_valuesVar, Expression.New(_valuesListType)),
                    Expression.Assign(_dlsVar, Expression.New(typeof(List<int>))),
                    Expression.Assign(_rlsVar, Expression.New(typeof(List<int>)))),

                iterationLoop,

                // result: use triple to construct ShreddedColumn and return (last element in the block)
                Expression.New(ShreddedColumnConstructor,
                    Expression.Call(_valuesVar, _valuesListType.GetMethod("ToArray")!),
                        _df.MaxDefinitionLevel == 0 ? Expression.Convert(Expression.Constant(null), typeof(List<int>)) : _dlsVar,
                        _df.MaxRepetitionLevel == 0 ? Expression.Convert(Expression.Constant(null), typeof(List<int>)) : _rlsVar)
                );

            Func<DataField, IEnumerable<TClass>, ShreddedColumn> lambda = Expression
                .Lambda<Func<DataField, IEnumerable<TClass>, ShreddedColumn>>(block, _dfParam, _classesParam)
                .Compile();

            return new FieldStriper<TClass>(_df, lambda, block, iteration);
        }
    }
}
