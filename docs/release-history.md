## 5.0.1

### New feature

You can deserialise "required" lists and "required" list elements, as raised by @akaloshych84 in #502.

### Improvements

- Better error reporting in case class serializer has mismatched definition and repetition levels (as per #502).
- Pass property attributes down to list data field, by @agaskill in #559.

## 5.0.0

### Support Parquet.Net

![](https://camo.githubusercontent.com/0e4a4c9c33927ac9cf93907ec12a9b5e22f2d825954b6f2c50290269d3fa3aea/68747470733a2f2f6d656469612e67697068792e636f6d2f6d656469612f76312e59326c6b505463354d4749334e6a4578626a593265576335593246305a5735364d6d5234615745306444687a5a586c696458526d59575a79616a6c3162575a6964585a7063795a6c634431324d563970626e526c636d35686246396e61575a66596e6c666157516d593351395a772f3837434b4471457256664d71592f67697068792e676966)

If you find the project helpful, you can support Parquet.Net by starring it.

### Breaking changes

- This is the first version without old Table/Row API, which is now completely removed. This API was one of the major headaches and source of bugs since being introduced in the very first version of this library. If you need a similar functionality, consider [untyped serializer](https://aloneguid.github.io/parquet-dotnet/untyped-serializer.html) which should be stable enough (Floor utility relies on this exclusively for quite some time).
- `ParquetSerializer`'s `SerializeAsync` was accepting `ParquetSerializerOptions` but `DeserializeAsync` was accepting `ParquetOptions`. This is now aligned for consistency so they both use `ParquetSerializerOptions`.

### New features

- Class deserializer can optionally [ignore property name casing](https://aloneguid.github.io/parquet-dotnet/serialisation.html#ignoring-property-casing) (#536).

### Improvements

- `ParquetWriter` supports asynchronous dispose pattern (`IAsyncDisposable`), thanks to @andagr in #479.
- `IronCompress` upstream dependency updated to 1.6.0.

### Bugs fixed

- Nullable `Enum`s were not correctly unwrapped to primitive types, by @cliedeman in #551.
- Reverting #537 due to it breaking binary compatibility in 4.25.0. Thanks to @NeilMacMullen for reporting this.

## 4.25.0

### Improvements
- File merger utility has `Stream` overload for non file-based operations.
- File merger utility has extra overload to choose compression codec and specify custom metadata, by @dxdjgl in #519.
- Timestamp logical type is supported, by @cliedeman in #521.
- More data types support encoding using Dictionary encoding, by @EamonHetherton in #531.
- Support for Roslyn nullable types, by @ErikApption in #537.
- internal: fix return of `Decode` methods to returning the actual destination length, by @artnim in #543.

## 4.24.0

### New features
- [Enum serialization](https://aloneguid.github.io/parquet-dotnet/serialisation.html#enum-support) is supported, using Enum's underlying type as a storage type.
- `[ParquetIgnore]` is supported in addition to `[JsonIgnore]` for class properties. This is useful when you want to ignore a property in Parquet serialization but not in JSON serialization. Thanks to @rhvieira1980 in #411.
- By popular demand, there is now a `FileMerger` utility which can merge multiple parquet files into a single file by either merging files or actual data together.

### Improvements

- Nullable `TimeSpan` support in `ParquetSerializer` by @cliedeman in #409.
- `DataFrame` support for `int16/uint16` types by @asmirnov82 in #469.
- Dropping build targets for .NET Core 3.1 and .NET 7.0 (STS). This should not affect anyone as .NET 6 and 8 are the LTS versions now.
- Added convenience methods to serialize/deserialize collections into a single row group in #506 by @piiertho. 
- Serialization of interfaces and interface member properties is now supported, see #513 thanks to @Pragmateek.
- `ParquetReader` is now easier to use in LINQ expressions thanks to @danielearwicker in #509.
- Upgraded to latest IronCompress dependency.

### Bug fixes

- Loop will read past the end of a block #487 by @alex-harper.
- Decimal scale condition check fixed in #504 by @sierzput.
- Class schema reflector was using single cache for reading and writing, which resulted in incorrect schema for writing. Thanks to @Pragmateek in #514.
- Incorrect definition level for null values in #516 by @greg0rym.

### Parquet Floor
- New feature "File explorer" lists filesystem using a panel on the left, allowing you to quickly load different files in the same directory and navigate to other directories.
- Hovering over title will show full file path and load time in milliseconds.
- Right-click on a row shows context menu allowing to copy the row to clipboard in text format.
- Icon updated to use the official Parquet logo.
- You will get a notification popup if a new version of Parquet Floor is available.
- Telemetry agreement changed and made clearer to understand.


## 4.23.5

### Bug fixes

- Reading decimal fields ignores precision and scale by @sierzput in #482.
- UUID logical type was not read correctly, it must always be in big-endian format. Thanks to @anatoliy-savchak in #496.

## 4.23.4

### Bug fixes

Fixed regression in schema discovery of nullables for `DateTime`, `DateOnly`, `TimeOnly`.

## 4.23.3

Fixed regression in schema discovery of nullable `decimal` data types. Thanks to @stefer in #465 for investigating and reporting this.

## 4.23.2

### Bug fixes

- Avoid file truncation when serializing with Append = true by @danielearwicker in #462. 
- Failure to read Parquet file with `FIXED_LEN_BYTE_ARRAY` generated by Python in #463 thanks to @AndrewDavidLees by @aloneguid.

## 4.23.1

### Improvement

- Flat file converter understands simple arrays and lists.

## 4.23.0

### New features

- Class serializer now supports fields, in addition to properties (#405).
- New helper class `ParquetToFlatTableConverter` to simplify conversion of parquet files to flat data destinations.

### Bugs fixed

- .NET >= 6 specific types `DateOnly` and `TimeOnly` deserialization was failing due to schema validation errors (#395).
- `TimeOnly` nullability wasn't respected.
- Custom attributes like `[ParquetTimestamp]`, `[ParquetMicroSecondsTime]` or `[ParquetDecimal]` were ignored for nullable class properties (408).

### Floor

- Remembers theme variant - "light" or "dark".
- Ask for permission to send anonymous telemetry data on start.
- New button - reload file from disk.
- Simple conversion to CSV.
- Implemented version check on start.

## 4.22.1

### Improvements

- Deserialization into array of primitives is now supported on root class level in #456.
- Untyped deserialiser supports legacy arrays.

### Parquet Floor

- Schema will still be loaded even if a file has failed to load.
- Legacy arrays can be viewed.

## 4.22.0

### Improvements

- Added `ParquetSerializer` `DeserializeAsync` overloads accepting local file path (#379)

### Bug fixes

- `DataFrameReader` did not handle files with multiple row groups (#365)

### Parquet Floor

- Reduced binary size after enabling partial trimming.
- `byte[]` columns are left-aligned.
- Increased data cell top and bottom padding by 2.

## 4.21.0

### New features

- **Parquet Floor** can display low-level metadata.

### Fixes

- `NetBox` was exposing some internal types (#451)

### Experimental

**Parquet Floor** (reference implementation of desktop viewer) user interface improvements.

![](https://github.com/aloneguid/parquet-dotnet/blob/master/docs/img/floor.png?raw=true)

## 4.20.0

### New features

Support Writing Int64 timestamp MICROS unit (#362).

### Experimental features

Cross-platform desktop app called **Floor** is published as a part of this release.

## 4.19.0

### Improvements

- Pre-allocate result list capacity when serializing by @Arithmomaniac in #444.

### Experimental features

1. This release has [experimental API for a new "dictionary serializer"](https://aloneguid.github.io/parquet-dotnet/untyped-serializer.html) (name might change) to get you a taste of the future before row API will be deprecated in a very far future.
2. Codebase also includes an experimental cross-platform desktop application written in Avalonia to view parquet files. It's in very early stages but works for basic use cases. Avalonia app was included in the solution because it does not require any IDE add-ons, SDKs and so on and just builds with stock .NET 8 SDK. In the future the app will be pre-built for Linux, Windows (and possibly Mac with community help) and included in the build artifacts.

Looking forward to your thoughts!

## 4.18.1

Critical bug fix: reverting #423 as it introducing some side effects that prevent from generating correct files.

## 4.18.0

This is the next stability improvements release, and a big thanks to everyone who contributed! Without you this project would not be possible.

Please don't forget to star this project on GitHub if you like it, this helps the project grow and motivates the fellow contributors to keep contributing!

### Improvements

- Explicitly use invariant culture when encoding number types, eliminating the potential for generating invalid JSON by @rachied in #438.
- Added DeserializeAllAsync in #433 by @Arithmomaniac.
- Added option to reduce flushing of streams during write operation in #432 by @dxdjgl. 
- Added explicit target for `.NET 8` by @aloneguid.

### Bug fixes

- `DataFrameMapper` returns incompatible `DataFrameColumn` by @aloneguid (#343).

## 4.17.0

This is a community bugfix release. As a maintainer I have only approved PRs raised by this wonderful community. Thanks everyone, and keep doing what you do.

### Improvements

- Allow deserialization from open `RowGroupReader`s by @ddrinka in #423/#422.

### Bugs fixed

- Gracefully handle malformed fields with trailing bytes in the data by @mukunku in #413.
- `ParquetSerializer` doesn't support different `JsonPropertyName` and `ClrPropertyName` on struct fields by @mrinal-thomas in #410.
- `ParquetSerializer` can sometimes fail when populating `_typeToAssembler` cache in parallel by @scottfavre in #420/#411.

## 4.16.4

Class serializer was writing map key as optional (#396). Schema reflector for class serializer now emits non-nullable keys.

Validation for maps keys in schema was also added.

## 4.16.3

Delta encoding can be optionally turned off (thanks to @itayfisz for suggestion in #392).

## 4.16.2

**Critical Bug Fix in DELTA_BINARY_PACKED Decoding**: Adding first value to destination array before reading the block, by @ee-naveen in #391.

## 4.16.1

### Critical Bug Fixes

- Ensuring delta encoding footer blocks are complete And Handle Overflow by @ee-naveen in #387.
- Use PLAIN encoding for columns without defined data by @spanglerco in #388.

## 4.16.0

### New

- Markdown documentation fully migrated to [GitHub Pages](https://aloneguid.github.io/parquet-dotnet/). It was becoming slightly unmanageable and also recent GitHub updates made markdown files look awful. Also I kind of wanted to try [Writerside by JetBrains](https://lp.jetbrains.com/writerside/), and publish docs with pride ;) @aloneguid
- Class deserializer will now skip class properties and not throw an exception if they are missing in the source parquet file. Thanks to @greenlynx in #361.
- Column statistics can be read with zero cost without reading the data. Thanks to @mirosuav in #252, #368. 
- Support for `DELTA_BINARY_PACKED` encoding on write. This encoding is now default when writing `INT32` and `INT64` columns. Most of the work done by @ee-naveen in #382.

### Improvements

- IronCompress was updated to v1.5.1 by @aloneguid.

### Fixes

- Fix precision issues writing `DateTime` as milliseconds by @spanglerco in #312.
- In `DataColumnWriter`, `RecycableMemoryStream` wasn't used in a particular case, and instead `MemoryStream` was initialized directly. Thanks to @itayfisz in #373.
- Bitpacked Hybrid decoder was failing on columns containing exactly one value.

## 4.15.0

### Bugs Fixed

- strings **must** be null by default (#360). Thanks @waf!

### New Stuff

- You can force optionality of a schema field using the `[ParquetRequired]` attribute.
- `ParquetSerializer` validates class schema against actual file schema on deserialization and throws a helpful exception, like `System.IO.InvalidDataException : property 'Id' is declared as Id (System.String?) but source data has it as Id (System.String)` (you can spot the difference in nullability here).

## 4.14.0

- Added support for reading legacy array primitives collection serialized via legacy `ParquetConvert` class or some other legacy system, thanks to @PablitoCBR. This work was effectively taken [from his PR](https://github.com/aloneguid/parquet-dotnet/pull/351) and integrated more natively into this library. Thank you very much!

- Fixed deserializing parquet generated by Azure Data Explorer non-native writer by @mcbos in #357.

- re-worked build pipeline to separate build and release stage.
- use handcrafted release notes file and cut out last version notes with `grep`/`head`/`tail` on release. This is in order to improve release notes experience as autogenerated ones are often of sub-par quality.

## 4.13.0

- Add support for deserializing required strings by [@mcbos](https://github.com/mcbos) in [#341](https://github.com/aloneguid/parquet-dotnet/pull/341)
- Add support to .NET 6 TimeOnly type by [@ramon-garcia](https://github.com/ramon-garcia) in [#352](https://github.com/aloneguid/parquet-dotnet/pull/352)
- Support for reading and writing column chunk metadata by [@aloneguid](https://github.com/aloneguid) in [#354](https://github.com/aloneguid/parquet-dotnet/pull/354)