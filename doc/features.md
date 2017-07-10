# Supported features

We are implementing Parquet features gradually, and the table below outlines the current status. If the feature is not listed here it's not supported yet.

|Feature|Reader|Writer|
|-------|------|------|
|Plain encoding|yes|partial|
|Bit Packed encoding|yes|no|
|RLE/Bitpacked Hybrid encoding|yes|no|
|Plain Dictionary encoding|yes|no|
|Delta encoding|no|no|
|Data-length byte array encoding|no|no|
|Delta strings encoding|no|no|
|GZIP Compression|yes|yes|
|SNAPPY Compression|no|no|

