# 6.0.0-pre.1

> V6 is a substantial rewrite of the low-level API, which addresses memory and performance issues. It's time to forget about the past and target modern .NET with modern APIs. The high-level API (class serializer) is not affected by these changes and should work as before logically, however you will see a massive performance increase and much lower memory usage. Parquet.Net development was pretty much stale for the last year or two, due to requirement for backward compatibility all the way to V1, and so I had to make a choice - whether stop adding any features and improvements, or break backward compatibility and make the library better. I chose the latter, and I hope you will like the new version as much as I do.

## Breaking changes

- To enable further evolution of this library, like using Spans, direct memory access, SIMD support and so on, I am dropping support for .NET Standard and older .NET versions. The minimum supported version of .NET is .NET 8. Supporting anything lower (or Windows specific .NET, which only shares the name and not much more with THE .NET) would require a lot of effort which I can't give you.
- `ParquetWriter` and `ParquetReader` only supports `IAsyncDisposable` now, so you should use `await using` instead of `using` when writing row groups. This is because some of the operations during writing are asynchronous and it would be a shame to not take advantage of that. Previously, `IDisposable` was supported as well, but that would occassionally cause write deadlocks.
- `ParquetRowGroupWriter` now accepts `ReadOnlyMemory<T>` instead of untyped `DataColumn` (which is now removed). This solves old dangling issue with inflexible memory useage, as users of the low-level API had to unnecessarily allocate memory just to write a column, often resuling in making large redundant copies.
- Same goes for `ParquetRowGroupReader`, which uses direct memory access interface instead of allocating a lot of memory via DataColumn and adding a lot of GC pressure.
- `ParquetOptions.UseDictionaryEncoding` is removed to avoid trying to dictionary-encode everything, which is not always the best choice. Instead, you can specify encodings for each column in `ParquetOptions.DictionaryEncodedColumns`.
- `Utils` namespace removed, which used to provide a sub-par implementations of `FileMerger` and `FlatFileConverter`. Shout if you need them, as I can add more efficient versions of these utilities in the future, here or in a separate package. Both of these utilities were subobtimal and half-done, and I don't want to maintain them in the long run.
- As with the latest V5 minor release, I have high hopes for managed .NET compression libraries maintained by the community, so there will be absolutely zero native dependencies. They were created in C++ as a separate project in the times when .NET was young and didn't have good support for such things, but now there are some great high-performance libraries available. If I have time to spend on improving compression performance, I'd rather contribute to those projects.

## Improvements

- More APIs respect `CancellationToken` allowing you to cancel long-running parquet operations.

## Bug fixes

- Decoder will prioritise logical type metadata when reading files, because some readers (like Arrow v22) do not write backward-compatible metadata anymore, in #719, #716 by @mukuntu, @aloneguid.
- Decode Zstd chunk with wrong length successfully, by @aloneguid in #717.

## Performance

- Serializer uses significantly less memory when serializing large collections.
- Dictionary encoder will give up earlier if cardinality is too high, without iterating all values. Less memory is allocated on early exit.

## Other changes

- Greatly simplified versioning logic in CI/CD, now the only place to set version is in `docs/release-notes.md` file, which also supports pre-release version logic.
- DuckDB integration tests removed, they turned out to be pretty much useless.
- Some tests made much cleaner and more manageable by removing Theory and replacing it with Facts, when Theory validation had too many edge cases and actually made tests less maintainable.
- `Parquet.Data.DataAnalysis` logic greatly simplified, T4 template removed.

# 5.5.0

## Improvements

- **BREAKING:** ParquetSerializer deserialization generic methods now constrain the type parameter to `class, new()` (previously `new()` only). This explicitly prevents using value types as deserialization targets (#698).
- `TimeSpanDataField` constructor has an option to set `IsAdjustedToUTC` (#650).
- `IAsyncEnumerable<T>` is limited to .NET 10 and above now.
- Allow reading and writing really large decimal values from Parquet files (larger than 29 significant digits) (#689, #697).
- Column Chunk encodings are populated according to their use rather than being hardcoded (#628).
- Internally, compression/decompression logic has been changed to use managed external packages for Snappy and Zstandard algorithms. IronCompress dependency is now completely removed, because there are decent managed implementations available nowadays. This will also allow to upgrade compression libraries more easily in the future and optimise memory usage. There is a slight memory usage improvement (~5%).

# 5.4.0

## Improvements

- It's now possible to serialize nested lists (list of lists, `List<List<T>>`) in class serializer, by @aloneguid in #612. Thanks @Vannevelj.
- Support for implicit parquet lists in schema parser and serializer, by @aloneguid and @mukunku in #681.
- Added `IAsyncEnumerable` serializer with `System.Linq.AsyncEnumerable`, by @Arithmomaniac in #674.
- All the documentation has been condensed and moved back to README file, making it easier to find and read, by @aloneguid.

## Bugs fixed

- When encoding `bool` values, the results are now consistent, by @Kevin-Ross-ECC in #643.
- `ParquetRowGroupWriter.Dispose()` will not throw exceptions as it conflicts with `try/catch/finally` ideology. You should call `CompleteValidate` after writing all columns instead. Thanks to @rkarim-nnk in #666.
- Required struct members were always annotated as optional, by @aloneguid in #582.

# 5.3.0

## New features

- Added support for BYTE_STREAM_SPLIT encoding, by @alex-harper in #539.
- DELTA_BINARY_PACKED is supported on more data types (short, ushort, ulong, uint), by @ee-naveen and @wickedmachinator in #599.

## Improvements

- Added cancellation token to `ReadEntireRowGroupAsync` for consistency with the rest of the API, by @kiloOhm in #647.
- Added validation to prevent writing malformed Parquet files, by @Kuinox in #656.
- Necessary fixes to get .NET 10 ready.

## Bug fixes

- `DataPageHeaderV2.IsCompressed` is defaulting to false on read even though the spec seems to say it should be true, by @ngbrown in #625.
- Class deserialiser can handle pre-initialised dictionaries, by @kiloOhm in #651.
- Add bounds check for `pageSize` in `RleBitpackedHybridEncoder.Decode`, by @mlptownsend in #662.

## Tests

- Added DuckDB for integration tests (#661 by @aloneguid).

# 5.2.0

## Improvements

- Exposed `FieldId` in `DataField` by @Hrachkata in #632.
- Handle decimals with no scale defined: `DECIMAL(X, 0)` by @mukunku in #602.
- Documentation fixes.

## Bug fixes

- Fixed parsing empty byte arrays in #616 by @Kuinox.
- Fixed `ArgumentException` "destination is too short" when reading a parquet file generated by DuckDB, by @BMehlkop in #636.
- Fixed class serialisation bug that didn't handle empty/null list of nullable type, by @mrinal-thomas in #630.
- Fix dictionary serialisation issue when key is non-nullable in #606 by @AshishGupta1995.

## Floor

- *floor* (experimental UI) is discontinued due to having massive performance issues with Avalonia. I don't think Avalonia is a suitable UI framework for low latency desktop applications.

# 5.1.1

## Improvements

- `RecyclableMemoryStreamManager` exposes memory settings in `ParquetOptions`, by @DrewMcArthur in #597.

## Bug fixes

- Fixed ParquetSchema equality collisions in #596 by @goeffthomas.
- Fixed failures when decoding LZ4Raw encoding. Big shoutout to @mukunku for saving us from the data gremlins! See mukunku/ParquetViewer#126.
- Make `isAdjustedToUTC` default to true and allow it to be set, by @cliedeman in #547.

## Floor

- Turns out, MacOSX binaries were having an identity crisis because they were built by a Linux build server. They just couldn't get along! But fear not, this has been fixed. Big thanks to @daningalla and @dmunch in #587. It's like a tech peace treaty!

![floor](https://github.com/user-attachments/assets/6a643a36-36cd-4c1d-892b-3c6bd2593f27)

# 5.1.0

## Improvements

- Class-reflected schemas for `map` complex types will generate key/value properties with `key` and `value` names, respectively. This is required in order to deserialise externally generated parquet files with dictionaries.
- Updated dependent packages.
- Class deserializer will check for type compatibility when deserialising incompatible types, which will prevent accidental data loss or narrowing. Thanks to @dkotov in #573.
- Write RowGroup total size and compressed size by @justinas-marozas in #580.
- DELTA_BYTE_ARRAY aka "Delta Strings" encoding can read byte array types in addition to string types.

## Bug fixes

- Adopted to breaking change in Parquet specification (technically it's a fault of upstream, not this library's) resulting in bugs like #576 and #579.
- Boolean fields encoded in RLE failed to read at all. Technically RLE can be used for integer and booleans, but we only ever supported integers.

## Changes

- Removed .NET 6 build target as it is [out of support](https://devblogs.microsoft.com/dotnet/dotnet-6-end-of-support/).

## Floor

- Compiled using .NET 9 (upgrade from .NET 8).

# 5.0.2

## New features

- Untyped serialisation supports async enumerable, thanks to @flambert860 in #566.

## Improvements

- Serialisation of CLR (not Parquet) structs and nullable structs is now properly handled and supported, thanks to @paulengineer.
- Microsoft.Data.Analysis related functionality was moved out to a separate nuget package - `Parquet.Net.Data.Analysis`. This is because it introduces quite a few dependencies which are not always needed with the slim main package.
- For Windows, run unit tests on x86 and x32 explicitly.
- Improved GHA build/release process, combining all workflows into one and simplifying it, most importantly release management.

## Floor

- Application slimmed down a bit, removing "File Explorer". Sticking to doing one thing and do it well - view Parquet files.
- Due to Avalonia startup times being not satisfactory when you are in the "mode" (1-2 seconds) Floor will reuse existing instance to open a file rather than starting the app again.

## Announcements 🎉

There is a new, very young project I've been thinking a lot for a long time and finally started - [DeltaIO](https://github.com/aloneguid/delta). It's attempting to do what Parquet.Net did for Apache Parquet but for Delta tables. It heavily relies on this library to read delta logs and data from it. It's still very young, but if you are interested in Delta with .NET, please check it out, bookmark, start and leave feedbacks/suggestions.

# 5.0.1

## New feature

You can deserialise "required" lists and "required" list elements, as raised by @akaloshych84 in #502. See [nullability and lists](https://aloneguid.github.io/parquet-dotnet/serialisation.html#nullability-and-lists).

## Improvements

- Better error reporting in case class serializer has mismatched definition and repetition levels (as per #502).
- Pass property attributes down to list data field, by @agaskill in #559.

## Bug fixed

- Compression/decompression would fail on some platforms like x86 or Linux x86 with musl runtime.

## Floor

- `boolean` columns display as checks.
- Structs display as expandable objects, with properly aligned keys.

# 5.0.0

## Support Parquet.Net

![](https://camo.githubusercontent.com/0e4a4c9c33927ac9cf93907ec12a9b5e22f2d825954b6f2c50290269d3fa3aea/68747470733a2f2f6d656469612e67697068792e636f6d2f6d656469612f76312e59326c6b505463354d4749334e6a4578626a593265576335593246305a5735364d6d5234615745306444687a5a586c696458526d59575a79616a6c3162575a6964585a7063795a6c634431324d563970626e526c636d35686246396e61575a66596e6c666157516d593351395a772f3837434b4471457256664d71592f67697068792e676966)

If you find the project helpful, you can support Parquet.Net by starring it.

## Breaking changes

- This is the first version without old Table/Row API, which is now completely removed. This API was one of the major headaches and source of bugs since being introduced in the very first version of this library. If you need a similar functionality, consider [untyped serializer](https://aloneguid.github.io/parquet-dotnet/untyped-serializer.html) which should be stable enough (Floor utility relies on this exclusively for quite some time).
- `ParquetSerializer`'s `SerializeAsync` was accepting `ParquetSerializerOptions` but `DeserializeAsync` was accepting `ParquetOptions`. This is now aligned for consistency so they both use `ParquetSerializerOptions`.

## New features

- Class deserializer can optionally [ignore property name casing](https://aloneguid.github.io/parquet-dotnet/serialisation.html#ignoring-property-casing) (#536).

## Improvements

- `ParquetWriter` supports asynchronous dispose pattern (`IAsyncDisposable`), thanks to @andagr in #479.
- `IronCompress` upstream dependency updated to 1.6.0.

## Bugs fixed

- Nullable `Enum`s were not correctly unwrapped to primitive types, by @cliedeman in #551.
- Reverting #537 due to it breaking binary compatibility in 4.25.0. Thanks to @NeilMacMullen for reporting this.

# 4.25.0

## Improvements
- File merger utility has `Stream` overload for non file-based operations.
- File merger utility has extra overload to choose compression codec and specify custom metadata, by @dxdjgl in #519.
- Timestamp logical type is supported, by @cliedeman in #521.
- More data types support encoding using Dictionary encoding, by @EamonHetherton in #531.
- Support for Roslyn nullable types, by @ErikApption in #537.
- internal: fix return of `Decode` methods to returning the actual destination length, by @artnim in #543.

# 4.24.0

## New features
- [Enum serialization](https://aloneguid.github.io/parquet-dotnet/serialisation.html#enum-support) is supported, using Enum's underlying type as a storage type.
- `[ParquetIgnore]` is supported in addition to `[JsonIgnore]` for class properties. This is useful when you want to ignore a property in Parquet serialization but not in JSON serialization. Thanks to @rhvieira1980 in #411.
- By popular demand, there is now a `FileMerger` utility which can merge multiple parquet files into a single file by either merging files or actual data together.

## Improvements

- Nullable `TimeSpan` support in `ParquetSerializer` by @cliedeman in #409.
- `DataFrame` support for `int16/uint16` types by @asmirnov82 in #469.
- Dropping build targets for .NET Core 3.1 and .NET 7.0 (STS). This should not affect anyone as .NET 6 and 8 are the LTS versions now.
- Added convenience methods to serialize/deserialize collections into a single row group in #506 by @piiertho. 
- Serialization of interfaces and interface member properties is now supported, see #513 thanks to @Pragmateek.
- `ParquetReader` is now easier to use in LINQ expressions thanks to @danielearwicker in #509.
- Upgraded to latest IronCompress dependency.

## Bug fixes

- Loop will read past the end of a block #487 by @alex-harper.
- Decimal scale condition check fixed in #504 by @sierzput.
- Class schema reflector was using single cache for reading and writing, which resulted in incorrect schema for writing. Thanks to @Pragmateek in #514.
- Incorrect definition level for null values in #516 by @greg0rym.

## Parquet Floor
- New feature "File explorer" lists filesystem using a panel on the left, allowing you to quickly load different files in the same directory and navigate to other directories.
- Hovering over title will show full file path and load time in milliseconds.
- Right-click on a row shows context menu allowing to copy the row to clipboard in text format.
- Icon updated to use the official Parquet logo.
- You will get a notification popup if a new version of Parquet Floor is available.
- Telemetry agreement changed and made clearer to understand.


# 4.23.5

## Bug fixes

- Reading decimal fields ignores precision and scale by @sierzput in #482.
- UUID logical type was not read correctly, it must always be in big-endian format. Thanks to @anatoliy-savchak in #496.

# 4.23.4

## Bug fixes

Fixed regression in schema discovery of nullables for `DateTime`, `DateOnly`, `TimeOnly`.

# 4.23.3

Fixed regression in schema discovery of nullable `decimal` data types. Thanks to @stefer in #465 for investigating and reporting this.

# 4.23.2

## Bug fixes

- Avoid file truncation when serializing with Append = true by @danielearwicker in #462. 
- Failure to read Parquet file with `FIXED_LEN_BYTE_ARRAY` generated by Python in #463 thanks to @AndrewDavidLees by @aloneguid.

# 4.23.1

## Improvement

- Flat file converter understands simple arrays and lists.

# 4.23.0

## New features

- Class serializer now supports fields, in addition to properties (#405).
- New helper class `ParquetToFlatTableConverter` to simplify conversion of parquet files to flat data destinations.

## Bugs fixed

- .NET >= 6 specific types `DateOnly` and `TimeOnly` deserialization was failing due to schema validation errors (#395).
- `TimeOnly` nullability wasn't respected.
- Custom attributes like `[ParquetTimestamp]`, `[ParquetMicroSecondsTime]` or `[ParquetDecimal]` were ignored for nullable class properties (408).

## Floor

- Remembers theme variant - "light" or "dark".
- Ask for permission to send anonymous telemetry data on start.
- New button - reload file from disk.
- Simple conversion to CSV.
- Implemented version check on start.

# 4.22.1

## Improvements

- Deserialization into array of primitives is now supported on root class level in #456.
- Untyped deserialiser supports legacy arrays.

## Parquet Floor

- Schema will still be loaded even if a file has failed to load.
- Legacy arrays can be viewed.

# 4.22.0

## Improvements

- Added `ParquetSerializer` `DeserializeAsync` overloads accepting local file path (#379)

## Bug fixes

- `DataFrameReader` did not handle files with multiple row groups (#365)

## Parquet Floor

- Reduced binary size after enabling partial trimming.
- `byte[]` columns are left-aligned.
- Increased data cell top and bottom padding by 2.

# 4.21.0

## New features

- **Parquet Floor** can display low-level metadata.

## Fixes

- `NetBox` was exposing some internal types (#451)

## Experimental

**Parquet Floor** (reference implementation of desktop viewer) user interface improvements.

![](https://github.com/aloneguid/parquet-dotnet/blob/master/docs/img/floor.png?raw=true)

# 4.20.0

## New features

Support Writing Int64 timestamp MICROS unit (#362).

## Experimental features

Cross-platform desktop app called **Floor** is published as a part of this release.

# 4.19.0

## Improvements

- Pre-allocate result list capacity when serializing by @Arithmomaniac in #444.

## Experimental features

1. This release has [experimental API for a new "dictionary serializer"](https://aloneguid.github.io/parquet-dotnet/untyped-serializer.html) (name might change) to get you a taste of the future before row API will be deprecated in a very far future.
2. Codebase also includes an experimental cross-platform desktop application written in Avalonia to view parquet files. It's in very early stages but works for basic use cases. Avalonia app was included in the solution because it does not require any IDE add-ons, SDKs and so on and just builds with stock .NET 8 SDK. In the future the app will be pre-built for Linux, Windows (and possibly Mac with community help) and included in the build artifacts.

Looking forward to your thoughts!

# 4.18.1

Critical bug fix: reverting #423 as it introducing some side effects that prevent from generating correct files.

# 4.18.0

This is the next stability improvements release, and a big thanks to everyone who contributed! Without you this project would not be possible.

Please don't forget to star this project on GitHub if you like it, this helps the project grow and motivates the fellow contributors to keep contributing!

## Improvements

- Explicitly use invariant culture when encoding number types, eliminating the potential for generating invalid JSON by @rachied in #438.
- Added DeserializeAllAsync in #433 by @Arithmomaniac.
- Added option to reduce flushing of streams during write operation in #432 by @dxdjgl. 
- Added explicit target for `.NET 8` by @aloneguid.

## Bug fixes

- `DataFrameMapper` returns incompatible `DataFrameColumn` by @aloneguid (#343).

# 4.17.0

## Improvements

- Allow deserialization from open `RowGroupReader`s by @ddrinka in #423/#422.

## Bugs fixed

- Gracefully handle malformed fields with trailing bytes in the data by @mukunku in #413.
- `ParquetSerializer` doesn't support different `JsonPropertyName` and `ClrPropertyName` on struct fields by @mrinal-thomas in #410.
- `ParquetSerializer` can sometimes fail when populating `_typeToAssembler` cache in parallel by @scottfavre in #420/#411.

# 4.16.4

Class serializer was writing map key as optional (#396). Schema reflector for class serializer now emits non-nullable keys.

Validation for maps keys in schema was also added.

# 4.16.3

Delta encoding can be optionally turned off (thanks to @itayfisz for suggestion in #392).

# 4.16.2

**Critical Bug Fix in DELTA_BINARY_PACKED Decoding**: Adding first value to destination array before reading the block, by @ee-naveen in #391.

# 4.16.1

## Critical Bug Fixes

- Ensuring delta encoding footer blocks are complete And Handle Overflow by @ee-naveen in #387.
- Use PLAIN encoding for columns without defined data by @spanglerco in #388.

# 4.16.0

## New

- Markdown documentation fully migrated to [GitHub Pages](https://aloneguid.github.io/parquet-dotnet/). It was becoming slightly unmanageable and also recent GitHub updates made markdown files look awful. Also I kind of wanted to try [Writerside by JetBrains](https://lp.jetbrains.com/writerside/), and publish docs with pride ;) @aloneguid
- Class deserializer will now skip class properties and not throw an exception if they are missing in the source parquet file. Thanks to @greenlynx in #361.
- Column statistics can be read with zero cost without reading the data. Thanks to @mirosuav in #252, #368. 
- Support for `DELTA_BINARY_PACKED` encoding on write. This encoding is now default when writing `INT32` and `INT64` columns. Most of the work done by @ee-naveen in #382.

## Improvements

- IronCompress was updated to v1.5.1 by @aloneguid.

## Fixes

- Fix precision issues writing `DateTime` as milliseconds by @spanglerco in #312.
- In `DataColumnWriter`, `RecycableMemoryStream` wasn't used in a particular case, and instead `MemoryStream` was initialized directly. Thanks to @itayfisz in #373.
- Bitpacked Hybrid decoder was failing on columns containing exactly one value.

# 4.15.0

## Bugs Fixed

- strings **must** be null by default (#360). Thanks @waf!

## New Stuff

- You can force optionality of a schema field using the `[ParquetRequired]` attribute.
- `ParquetSerializer` validates class schema against actual file schema on deserialization and throws a helpful exception, like `System.IO.InvalidDataException : property 'Id' is declared as Id (System.String?) but source data has it as Id (System.String)` (you can spot the difference in nullability here).

# 4.14.0

- Added support for reading legacy array primitives collection serialized via legacy `ParquetConvert` class or some other legacy system, thanks to @PablitoCBR. This work was effectively taken [from his PR](https://github.com/aloneguid/parquet-dotnet/pull/351) and integrated more natively into this library. Thank you very much!

- Fixed deserializing parquet generated by Azure Data Explorer non-native writer by @mcbos in #357.

- re-worked build pipeline to separate build and release stage.
- use handcrafted release notes file and cut out last version notes with `grep`/`head`/`tail` on release. This is in order to improve release notes experience as autogenerated ones are often of sub-par quality.

# 4.13.0

- Add support for deserializing required strings by [@mcbos](https://github.com/mcbos) in [#341](https://github.com/aloneguid/parquet-dotnet/pull/341)
- Add support to .NET 6 TimeOnly type by [@ramon-garcia](https://github.com/ramon-garcia) in [#352](https://github.com/aloneguid/parquet-dotnet/pull/352)
- Support for reading and writing column chunk metadata by [@aloneguid](https://github.com/aloneguid) in [#354](https://github.com/aloneguid/parquet-dotnet/pull/354)