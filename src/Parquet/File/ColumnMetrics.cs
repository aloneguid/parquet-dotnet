using System.Collections.Generic;
using System.Linq;
using Parquet.Meta;

namespace Parquet.File;

readonly struct ColumnMetrics {
    public ColumnMetrics() {
    }

    private ColumnMetrics(PageHeader[] pages, int compressedSize, int uncompressedSize)
    {
        _pages = pages;
        CompressedSize = compressedSize;
        UncompressedSize = uncompressedSize;
    }

    readonly PageHeader[] _pages = [];
    public int CompressedSize { get; }
    public int UncompressedSize { get; }
    public IReadOnlyList<PageHeader> Pages => _pages;

    public ColumnMetrics WithAddedSizes((int compressedSize, int uncompressedSize) sizes) => new(_pages, sizes.compressedSize+CompressedSize, sizes.uncompressedSize + UncompressedSize);

    public ColumnMetrics WithAddedPage(PageHeader page) => new([.. _pages, page], CompressedSize, UncompressedSize);

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