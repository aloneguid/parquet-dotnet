using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Test.Serialisation.Paper {

    class Document {
        public long DocId { get; set; }

        public Links? Links { get; set; }

        public List<Name>? Name { get; set; }
    }
}
