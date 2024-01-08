## 4.21.0

### New features

- **Parquet Floor** can display low-level metadata.

### Fixes

- `NetBox` was exposing some internal types (#451)

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