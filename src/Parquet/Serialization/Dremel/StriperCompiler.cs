using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Parquet.Data;
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

        private Expression AddValue(
           Expression valueVar,
           int dl,
           Expression currentRlVar) {

            Type tprop = _df.ClrNullableIfHasNullsType;
            ParameterExpression atomVar = Expression.Variable(tprop, "atom");
            return Expression.Block(
                new[] { atomVar },

                Expression.Assign(atomVar, valueVar),

                _df.IsNullable
                    ? Expression.IfThen(
                        // atomVar != null
                        Expression.NotEqual(atomVar, Expression.Constant(null)),
                            // also add non-null value
                            Expression.Call(_valuesVar,
                                _valuesListAddMethod,
                                // unwrap Nullable<T>
                                tprop.IsSystemNullable()
                                    ? Expression.Property(atomVar, "Value")
                                    : atomVar))
                    : Expression.Call(_valuesVar, _valuesListAddMethod, atomVar),

                _df.MaxDefinitionLevel > 0
                    ? Expression.Call(_dlsVar, LevelsAddMethod,
                        Expression.Condition(Expression.Equal(atomVar, Expression.Constant(null)), Expression.Constant(0), Expression.Constant(dl)))
                    : Expression.Empty(),

                _df.MaxRepetitionLevel > 0
                    ? Expression.Call(_rlsVar, LevelsAddMethod, currentRlVar)
                    : Expression.Empty()
                );
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

        private Expression DissectRecord(
            Expression rootVar,
            Field[] levelFields,
            List<string> path,
            Type rootType,
            int treeDepth,
            int dl,
            ParameterExpression currentRlVar) {

            // walk schema, not class instance
            // this means value must be propagated down the tree, even if it's not present

            string currentFieldName = path.First();
            Field? field = levelFields.FirstOrDefault(x => x.Name == currentFieldName);
            if(field == null)
                throw new NotSupportedException();

            Discover(field, out bool isRepeated, out bool hasDefinition);

            if(hasDefinition)
                dl += 1;

            string currentClassPropertyName = field.ClrPropName ?? field.Name;
            bool isAtomic = path.Count == 1;    // is this an atomic value (tree leaf)?
            Type childType = rootType.GetProperty(currentClassPropertyName)!.PropertyType;
            Type elementType = childType;

            // ternary null check condition:
            // rootVar == null ? null : rootVar.currentClassPropertyName;
            // replaces unsafe version of: striperElementVar = Expression.Property(rootVar, currentClassPropertyName);
            Expression elementAccessExpr = Expression.Condition(
                Expression.Equal(rootVar, Expression.Constant(null)),
                Expression.Convert(Expression.Constant(null), elementType),   // cast null to childType as argument types must match
                Expression.Property(rootVar, currentClassPropertyName));

            if(isRepeated) {
                // as per Dremel paper, we are going to enumerate children and apply identical rules
                elementType = childType.ExtractElementTypeFromEnumerableType();
                ParameterExpression loopElementVar = Expression.Variable(elementType,"loopElement");
                Expression iteration = Expression.Block(
                    // todo: capture rl and seenFields
                    isAtomic
                        ? AddValue(loopElementVar, dl, currentRlVar)
                        : DissectRecord(loopElementVar,
                            field.NaturalChildren, field.GetNaturalChildPath(path),
                            elementType, treeDepth + 1, dl, currentRlVar)
                    );
                return iteration.Loop(elementAccessExpr, elementType, loopElementVar);
            } else {
                // single element processor
                ParameterExpression vVar = Expression.Variable(elementType, "child");
                Expression iteration = Expression.Block(
                    // todo: capture rl and seenFields
                    new[] { vVar },
                    Expression.Assign(vVar, elementAccessExpr),
                    isAtomic
                        ? AddValue(vVar, dl, currentRlVar)
                        : DissectRecord(vVar,
                            field.NaturalChildren, field.GetNaturalChildPath(path),
                            elementType, treeDepth + 1, dl, currentRlVar)
                    );
                return iteration;
            }
        }

        public FieldStriper<TClass> Compile() {

            ParameterExpression currentRl = Expression.Variable(typeof(int), "currentRl");

            BlockExpression block = Expression.Block(
                new[] { _valuesVar, _dlsVar, _rlsVar, _classElementVar, currentRl },
                // init 3 building blocks
                Expression.Block(

                    Expression.Assign(_valuesVar, Expression.New(_valuesListType)),
                    // only create list instances when actually required
                    _df.MaxDefinitionLevel > 0
                        ? Expression.Assign(_dlsVar, Expression.New(typeof(List<int>)))
                        : Expression.Empty(),
                    _df.MaxRepetitionLevel > 0
                        ? Expression.Assign(_rlsVar, Expression.New(typeof(List<int>)))
                        : Expression.Empty()),

                // 
                DissectRecord(_classElementVar, _schema.Fields.ToArray(), _df.Path.ToList(), typeof(TClass), 0, 0, currentRl)
                    .Loop(_classesParam, typeof(TClass), _classElementVar),

                // result: use triple to construct DataColumn and return (last element in the block)
                Expression.New(ShreddedColumnConstructor,
                    Expression.Call(_valuesVar, _valuesListType.GetMethod("ToArray")!), _dlsVar, _rlsVar)
                );

            Func<DataField, IEnumerable<TClass>, ShreddedColumn> lambda = Expression
                .Lambda<Func<DataField, IEnumerable<TClass>, ShreddedColumn>>(block, _dfParam, _classesParam)
                .Compile();

            return new FieldStriper<TClass>(_df, lambda, block);
        }
    }
}
