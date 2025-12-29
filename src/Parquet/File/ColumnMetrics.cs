using System.Collections.Generic;
using System.Linq;
using Parquet.Meta;

namespace Parquet.File;

class ColumnMetrics {
    public int CompressedSize;
    public int UncompressedSize;
    public readonly List<PageHeader> Pages = new();

    public List<Encoding> GetUsedEncodings() {
        var r = new HashSet<Encoding>();
        foreach(PageHeader page in Pages) {
            if(page.DictionaryPageHeader != null) {
                r.Add(page.DictionaryPageHeader.Encoding);
            }
            if(page.DataPageHeader != null) {
                r.Add(page.DataPageHeader.Encoding);
                r.Add(page.DataPageHeader.DefinitionLevelEncoding);
                r.Add(page.DataPageHeader.RepetitionLevelEncoding);
            }
            if(page.DataPageHeaderV2 != null) {
                r.Add(page.DataPageHeaderV2.Encoding);
            }
        }
        return r.ToList();
    }
}
