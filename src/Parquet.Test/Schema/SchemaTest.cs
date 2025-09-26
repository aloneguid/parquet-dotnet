using Parquet.Data;
using System;
using Xunit;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Parquet.Schema;
using System.Linq;
using TT = Parquet.Meta.Type;
using CT = Parquet.Meta.ConvertedType;
using Parquet.File;
using Parquet.Meta;

namespace Parquet.Test.Schema {
    public class SchemaTest : TestBase {
        [Fact]
        public void Creating_element_with_unsupported_type_throws_exception() {
            Assert.Throws<NotSupportedException>(() => new DataField<Enum>("e"));
        }

        [Fact]
        public void SchemaElement_are_equal() {
            Assert.True(new DataField<int>("id").Equals(new DataField<int>("id")));
        }

        [Fact]
        public void SchemaElement_different_names_not_equal() {
            Assert.NotEqual(new DataField<int>("id1"), new DataField<int>("id"));
        }

        [Fact]
        public void SchemaElement_different_types_not_equal() {
            Assert.NotEqual(new DataField<int>("id"), (Field)new DataField<double>("id"));
        }

        [Fact]
        public void Schemas_identical_equal() {
            var schema1 = new ParquetSchema(new DataField<int>("id"), new DataField<string>("city"));
            var schema2 = new ParquetSchema(new DataField<int>("id"), new DataField<string>("city"));

            Assert.Equal(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_not_equal() {
            var schema1 = new ParquetSchema(new DataField<int>("id"), new DataField<string>("city"));
            var schema2 = new ParquetSchema(new DataField<int>("id"), new DataField<string>("city2"));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_differ_only_in_repeated_fields_not_equal() {
            var schema1 = new ParquetSchema(new DataField<int>("id"), new DataField<string>("cities"));
            var schema2 = new ParquetSchema(new DataField<int>("id"), new DataField<IEnumerable<string>>("cities"));

            Assert.NotEqual(schema1, schema2);
        }


        [Fact]
        public void Schemas_identical_structs_equal() {
            var schema1 = new ParquetSchema(new StructField("struct", new DataField<int>("id"), new DataField<string>("city")));
            var schema2 = new ParquetSchema(new StructField("struct", new DataField<int>("id"), new DataField<string>("city")));

            Assert.Equal(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_structs_by_path_not_equal() {
            var schema1 = new ParquetSchema(new StructField("struct", new DataField<int>("id"), new DataField<string>("city")));
            var schema2 = new ParquetSchema(new StructField("struct", new DataField<int>("id"), new DataField<string>("city2")));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_structs_by_type_not_equal() {
            var schema1 = new ParquetSchema(new StructField("struct", new DataField<int>("id"), new DataField<string>("city")));
            var schema2 = new ParquetSchema(new StructField("struct", new DataField<string>("id"), new DataField<string>("city")));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_structs_by_nullable_type_not_equal() {
            var schema1 = new ParquetSchema(new StructField("struct", new DataField<int>("id"), new DataField<int>("count")));
            var schema2 = new ParquetSchema(new StructField("struct", new DataField<int>("id"), new DataField<int>("count", nullable: true)));

            Assert.NotEqual(schema1, schema2);
        }


        [Fact]
        public void Schemas_identical_maps_equal() {
            var schema1 = new ParquetSchema(new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<int>("value")));
            var schema2 = new ParquetSchema(new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<int>("value")));

            Assert.Equal(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_maps_by_path_not_equal() {
            var schema1 = new ParquetSchema(new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<int>("value")));
            var schema2 = new ParquetSchema(new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<int>("other_value")));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_maps_by_type_not_equal() {
            var schema1 = new ParquetSchema(new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<int>("value")));
            var schema2 = new ParquetSchema(new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<double>("value")));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_maps_by_nullable_type_not_equal() {
            var schema1 = new ParquetSchema(new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<int>("value")));
            var schema2 = new ParquetSchema(new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<int>("value", nullable: true)));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_identical_simple_lists_equal() {
            var schema1 = new ParquetSchema(new ListField("list_of_items", new DataField<int>("id")));
            var schema2 = new ParquetSchema(new ListField("list_of_items", new DataField<int>("id")));

            Assert.Equal(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_simple_lists_by_path_not_equal() {
            var schema1 = new ParquetSchema(new ListField("list_of_items", new DataField<int>("id")));
            var schema2 = new ParquetSchema(new ListField("list_of_items", new DataField<int>("id2")));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_simple_lists_by_type_not_equal() {
            var schema1 = new ParquetSchema(new ListField("list_of_items", new DataField<int>("id")));
            var schema2 = new ParquetSchema(new ListField("list_of_items", new DataField<string>("id")));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_simple_lists_by_nullable_type_not_equal() {
            var schema1 = new ParquetSchema(new ListField("list_of_items", new DataField<int>("id")));
            var schema2 = new ParquetSchema(new ListField("list_of_items", new DataField<int>("id", nullable: true)));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_identical_complex_lists_equal() {
            var schema1 = new ParquetSchema(new ListField("list_of_structs", new StructField("struct", new DataField<int>("id"), new DataField<string>("city"))));
            var schema2 = new ParquetSchema(new ListField("list_of_structs", new StructField("struct", new DataField<int>("id"), new DataField<string>("city"))));

            Assert.Equal(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_complex_lists_by_path_equal() {
            var schema1 = new ParquetSchema(new ListField("list_of_structs", new StructField("struct", new DataField<int>("id"), new DataField<string>("city"))));
            var schema2 = new ParquetSchema(new ListField("list_of_structs", new StructField("struct", new DataField<int>("id"), new DataField<string>("city2"))));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_complex_lists_by_type_equal() {
            var schema1 = new ParquetSchema(new ListField("list_of_structs", new StructField("struct", new DataField<int>("id"), new DataField<string>("city"))));
            var schema2 = new ParquetSchema(new ListField("list_of_structs", new StructField("struct", new DataField<string>("id"), new DataField<string>("city2"))));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_different_complex_lists_by_nullable_type_not_equal() {
            var schema1 = new ParquetSchema(new ListField("list_of_structs", new StructField("struct", new DataField<int>("id"), new DataField<int>("count"))));
            var schema2 = new ParquetSchema(new ListField("list_of_structs", new StructField("struct", new DataField<int>("id"), new DataField<int>("count", nullable: true))));

            Assert.NotEqual(schema1, schema2);
        }

        [Fact]
        public void Schemas_differ_by_nullability() {
            Assert.NotEqual<Field>(
               new DataField<int>("id"),
               new DataField<int?>("id"));
        }

        [Fact]
        public void String_is_always_nullable() {
            var se = new DataField<string>("id");

            Assert.True(se.IsNullable);
        }

        [Fact]
        public void Datetime_is_not_nullable_by_default() {
            var se = new DataField<DateTime>("id");

            Assert.False(se.IsNullable);
        }

        [Fact]
        public void Generic_dictionaries_are_not_allowed() {
            Assert.Throws<NotSupportedException>(() => new DataField<IDictionary<int, string>>("dictionary"));
        }

        [Fact]
        public void Invalid_dictionary_declaration() {
            Assert.Throws<NotSupportedException>(() => new DataField<Dictionary<int, int>>("d"));
        }

        [Fact]
        public void But_i_can_declare_a_dictionary() {
            new MapField("dictionary", new DataField<int>("key"), new DataField<string>("value"));
        }

        [Fact]
        public void Map_fields_with_same_types_are_equal() {
            Assert.Equal(
                new MapField("dictionary",
                    new DataField<int>("key"),
                    new DataField<string>("value")),
                new MapField("dictionary",
                    new DataField<int>("key"),
                    new DataField<string>("value")));
        }

        [Fact]
        public void Map_fields_with_different_types_are_unequal() {

            var map1 = new MapField("dictionary",
                    new DataField<string>("key", false),
                   new DataField<string>("value"));


            var map2 = new MapField("dictionary",
                new DataField<int>("key"),
                new DataField<int>("value"));

            Assert.False(map1.Equals(map2));
        }

        [Fact]
        public void Byte_array_schema_is_a_bytearray_type() {
            var field = new DataField<byte[]>("bytes");
            Assert.Equal(typeof(byte[]), field.ClrType);
            Assert.False(field.IsArray);
            Assert.True(field.IsNullable);
        }

        [Fact]
        public void Cannot_create_struct_with_no_elements() {
            Assert.Throws<ArgumentException>(() => new StructField("struct"));
        }

        [Fact]
        public void Identical_structs_field_are_equal() {
            Assert.Equal(
               new StructField("f1", new DataField<string>("name")),
               new StructField("f1", new DataField<string>("name")));
        }

        [Fact]
        public void Structs_named_different_are_not_equal() {
            Assert.NotEqual(
               new StructField("f1", new DataField<string>("name")),
               new StructField("f2", new DataField<string>("name")));
        }

        [Fact]
        public void Structs_with_different_children_are_not_equal() {
            Assert.NotEqual(
               new StructField("f1", new DataField<string>("name")),
               new StructField("f1", new DataField<int>("name")));
        }

        [Fact]
        public void Lists_are_equal() {
            Assert.Equal(
               new ListField("item", new DataField<int>("id")),
               new ListField("item", new DataField<int>("id"))
            );
        }

        [Fact]
        public void Lists_are_not_equal_by_name() {
            Assert.NotEqual(
               new ListField("item", new DataField<int>("id")),
               new ListField("item1", new DataField<int>("id"))
            );
        }

        [Fact]
        public void Lists_are_not_equal_by_item() {
            Assert.NotEqual(
               new ListField("item", new DataField<int>("id")),
               new ListField("item", new DataField<string>("id"))
            );
        }

        [Fact]
        public void List_maintains_path_prefix() {
            var list = new ListField("List", new DataField<int>("id"));
            list.PathPrefix = new FieldPath("Parent");

            Assert.Equal(new FieldPath("Parent", "List", "list", "id"), list.Item.Path);
        }

        [Fact]
        public void Plain_data_field_0R_0D() {
            var schema = new ParquetSchema(new DataField<int>("id"));

            Assert.Equal(0, schema[0].MaxRepetitionLevel);
            Assert.Equal(0, schema[0].MaxDefinitionLevel);
        }

        [Fact]
        public void Plain_nullable_data_field_0R_1D() {
            var schema = new ParquetSchema(new DataField<int?>("id"));

            Assert.Equal(0, schema[0].MaxRepetitionLevel);
            Assert.Equal(1, schema[0].MaxDefinitionLevel);
        }

        [Fact]
        public void Map_of_required_key_and_optional_value_1R2D_1R3D() {
            var schema = new ParquetSchema(
               new MapField("numbers",
                  new DataField<int>("key"),
                  new DataField<string>("value")
                  ));

            Field key = ((MapField)schema[0]).Key;
            Field value = ((MapField)schema[0]).Value;

            Assert.Equal(1, key.MaxRepetitionLevel);
            Assert.Equal(2, key.MaxDefinitionLevel);

            Assert.Equal(1, value.MaxRepetitionLevel);
            Assert.Equal(3, value.MaxDefinitionLevel);
        }

        [Fact]
        public void List_of_structures_valid_levels() {
            var idField = new DataField<int>("id");
            var nameField = new DataField<string>("name");

            var schema = new ParquetSchema(
               new DataField<int>("topLevelId"),
               new ListField("structs",
                  new StructField("mystruct",
                     idField,
                     nameField)));

            Assert.Equal(0, schema[0].MaxRepetitionLevel);
            Assert.Equal(0, schema[0].MaxDefinitionLevel);

            Assert.Equal(1, idField.MaxRepetitionLevel);
            Assert.Equal(3, idField.MaxDefinitionLevel); // optional list + optional group + optional struct + required field

            Assert.Equal(1, nameField.MaxRepetitionLevel);
            Assert.Equal(4, nameField.MaxDefinitionLevel);
        }

        [Theory]
        [InlineData("legacy-list-onearray.parquet")]
        [InlineData("legacy-list-onearray.v2.parquet")]
        public async Task BackwardCompat_list_with_one_array(string parquetFile) {
            using(Stream input = OpenTestFile(parquetFile))
            using(ParquetReader reader = await ParquetReader.CreateAsync(input)) {
                ParquetSchema schema = reader.Schema;

                //validate schema
                Assert.Equal("impurityStats", schema[3].Name);
                Assert.Equal(SchemaType.List, schema[3].SchemaType);
                Assert.Equal("gain", schema[4].Name);
                Assert.Equal(SchemaType.Data, schema[4].SchemaType);

                //smoke test we can read it
                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    DataColumn values4 = await rg.ReadColumnAsync((DataField)schema[4]);
                }
            }
        }

        [Fact]
        public async Task Column_called_root() {

            var schema = new ParquetSchema(
                new DataField<string>("root"),
                new DataField<string>("other"));
            var columns = new List<DataColumn>();
            columns.Add(new DataColumn(schema.GetDataFields()[0], new string[] { "AAA" }));
            columns.Add(new DataColumn(schema.GetDataFields()[1], new string[] { "BBB" }));

            // the writer used to create structure type under "root" (https://github.com/aloneguid/parquet-dotnet/issues/143)
            var ms = new MemoryStream();
            using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(schema, ms))
            using(ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                foreach(DataColumn column in columns)
                    await groupWriter.WriteColumnAsync(column);

            ms.Position = 0;
            using ParquetReader parquetReader = await ParquetReader.CreateAsync(ms);
            DataField[] dataFields = parquetReader.Schema.GetDataFields();
            for(int i = 0; i < parquetReader.RowGroupCount; i++)
                using(ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
                    foreach(DataColumn column in columns) {
                        DataColumn c = await groupReader.ReadColumnAsync(column.Field);
                    }
        }

        [Fact]
        public async Task ReadSchemaActuallyEqualToWriteSchema() {
            var field = new DateTimeDataField("Date", DateTimeFormat.DateAndTime, isNullable: true);
            var schema = new ParquetSchema(field);

            using var memoryStream = new MemoryStream();

            using(ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(schema, memoryStream))
            using(ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup()) {
                var dataColumn = new DataColumn(field, new List<DateTime?>() { DateTime.Now }.ToArray());
                await groupWriter.WriteColumnAsync(dataColumn);
            }

            using(ParquetReader parquetReader = await ParquetReader.CreateAsync(memoryStream)) {
                parquetReader.Schema.Fields.ToString();

                Assert.Single(schema.Fields);
                Assert.Equal(schema.Fields.Count, parquetReader.Schema.Fields.Count);
                Assert.Equal(schema.Fields.First().GetType(), parquetReader.Schema.Fields.First().GetType());
            }
        }

        [Fact]
        public void Decode_list_normal() {
            ParquetSchema schema = ThriftFooter.Parse(
                new SchemaElement {
                    Name = "my_list",
                    ConvertedType = CT.LIST,
                    NumChildren = 1
                },
                new SchemaElement {
                    Name = "list",
                    RepetitionType = FieldRepetitionType.REPEATED,
                    NumChildren = 1
                },
                new SchemaElement {
                    Name = "element",
                    RepetitionType = FieldRepetitionType.REQUIRED,
                    Type = TT.INT32
                });

            Field f = schema[0];
            if(f is ListField lf) {
                Assert.Equal("my_list", lf.Name);
                Assert.Equal("element", lf.Item.Name);
            } else {
                Assert.Fail("list expected");
            }
        }

        [Fact]
        public void Decode_list_legacy_no_mid_group() {
            ParquetSchema schema = ThriftFooter.Parse(
                new SchemaElement {
                    Name = "my_list",
                    ConvertedType = CT.LIST
                },
                new SchemaElement {
                    Name = "list",
                    RepetitionType = FieldRepetitionType.REPEATED,
                    NumChildren = 1
                },
                new SchemaElement {
                    Name = "element",
                    RepetitionType = FieldRepetitionType.REQUIRED,
                    Type = TT.INT32
                });

            Field f = schema[0];
            if(f is ListField lf) {
                Assert.Equal("my_list", lf.Name);
                Assert.Equal("element", lf.Item.Name);
            } else {
                Assert.Fail("list expected");
            }
        }


        [Fact]
        public void Augment_changes_name_and_path() {
            var typeSchema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("name"));

            var fileSchema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("Name"));

            Assert.Equal("name", typeSchema[1].Name);
            Assert.Equal("name", typeSchema[1].ClrPropName);
            Assert.Equal("name", typeSchema[1].Path.ToString());
            typeSchema.Augment(fileSchema);
            Assert.Equal("Name", typeSchema[1].Name);
            Assert.Equal("name", typeSchema[1].ClrPropName);    // but should not change ClrPropName
            Assert.Equal("Name", typeSchema[1].Path.ToString());
        }

        [Fact]
        public void Augment_changes_struct_member_name_and_path() {
            var typeSchema = new ParquetSchema(
                new StructField("s",
                    new DataField<int>("id"),
                    new DataField<string>("name")));

            var fileSchema = new ParquetSchema(
                new StructField("s",
                    new DataField<int>("id"),
                    new DataField<string>("Name")));

            Assert.Equal("name", typeSchema[0].Children[1].Name);
            Assert.Equal("s/name", typeSchema[0].Children[1].Path.ToString());
            typeSchema.Augment(fileSchema);
            Assert.Equal("Name", typeSchema[0].Children[1].Name);
            Assert.Equal("s/Name", typeSchema[0].Children[1].Path.ToString());
        }

        [Fact]
        public void Augment_changes_list_member_name_and_path() {
            var typeSchema = new ParquetSchema(
                new ListField("l",
                    new DataField<int>("id")));

            var fileSchema1 = new ParquetSchema(
                new ListField("l",
                    new DataField<int>("Id")));

            // pre-checks
            ListField lf = (ListField)typeSchema[0];
            Assert.Equal("l", lf.Name);
            Assert.Equal("id", lf.Item.Name);
            Assert.Equal("l/list", lf.Path.ToString());
            Assert.Equal("l/list/id", lf.Item.Path.ToString());

            // augment
            typeSchema.Augment(fileSchema1);

            // post-checks
            Assert.Equal("l", lf.Name);
            Assert.Equal("Id", lf.Item.Name);
            Assert.Equal("l/list", lf.Path.ToString());
            Assert.Equal("l/list/Id", lf.Item.Path.ToString());
        }

        [Fact]
        public void Augment_changes_list_container_and_item_name_and_path() {
            var typeSchema = new ParquetSchema(
                new ListField("l",
                    new DataField<int>("id")));

            var fileSchema1 = new ParquetSchema(
                new ListField("L",
                    new DataField<int>("Id")));

            // pre-checks
            ListField lf = (ListField)typeSchema[0];
            Assert.Equal("l", lf.Name);
            Assert.Equal("id", lf.Item.Name);
            Assert.Equal("l/list", lf.Path.ToString());
            Assert.Equal("l/list/id", lf.Item.Path.ToString());

            // augment
            typeSchema.Augment(fileSchema1);

            // post-checks
            Assert.Equal("L", lf.Name);
            Assert.Equal("Id", lf.Item.Name);
            Assert.Equal("L/list", lf.Path.ToString());
            Assert.Equal("L/list/Id", lf.Item.Path.ToString());
        }

        [Fact]
        public void Augment_changes_map_key_and_value_name_and_path() {
            var typeSchema = new ParquetSchema(
                new MapField("m",
                    new DataField<int>("key"),
                    new DataField<string>("value")));

            var fileSchema = new ParquetSchema(
                new MapField("m",
                    new DataField<int>("Key"),
                    new DataField<string>("Value")));

            // pre-checks
            MapField mf = (MapField)typeSchema[0];
            Assert.Equal("m", mf.Name);
            Assert.Equal("key", mf.Key.Name);
            Assert.Equal("value", mf.Value.Name);
            Assert.Equal("m/key_value", mf.Path.ToString());
            Assert.Equal("m/key_value/key", mf.Key.Path.ToString());
            Assert.Equal("m/key_value/value", mf.Value.Path.ToString());

            // augment
            typeSchema.Augment(fileSchema);

            // post-checks
            Assert.Equal("m", mf.Name);
            Assert.Equal("Key", mf.Key.Name);
            Assert.Equal("Value", mf.Value.Name);
            Assert.Equal("m/key_value", mf.Path.ToString());
            Assert.Equal("m/key_value/Key", mf.Key.Path.ToString());
            Assert.Equal("m/key_value/Value", mf.Value.Path.ToString());
        }
    }
}