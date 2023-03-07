using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Serialization.Dremel {
    class ShreddedColumn {

        public ShreddedColumn(Array data, List<int>? definitionLevels, List<int>? repetitionLevels) {
            Data = data;
            DefinitionLevels = definitionLevels;
            RepetitionLevels = repetitionLevels;
        }

        public Array Data;
        public List<int>? DefinitionLevels { get; set; }
        public List<int>? RepetitionLevels { get; set; }
    }
}
