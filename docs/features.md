# Supported features

We are implementing Parquet features gradually, and the table below outlines the current status. If the feature is not listed here it's not supported yet.

|Feature|Reader|Writer|
|-------|------|------|
|Plain encoding|yes|yes|
|Bit Packed encoding|yes|yes|
|RLE/Bitpacked Hybrid encoding|yes|yes|
|Plain Dictionary encoding|yes|yes|
|Delta encoding|no|no|
|Data-length byte array encoding|no|no|
|Delta strings encoding|no|no|
|Impala dates|yes|yes|
|Date with different precision|yes|yes|
|Time period|yes|yes|
|GZIP Compression|yes|yes|
|SNAPPY Compression|yes|yes|
|Row groups|yes|yes|
|Data pages|yes|yes|
|Append to files|-|yes|
|Simple repeatable fields|yes|yes|
|Nested structures|yes|yes|
|Map types|yes|yes|
