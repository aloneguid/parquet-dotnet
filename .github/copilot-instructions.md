# C# Development

## C# Instructions
- Always use the latest version C#, currently C# 14 features.
- Write clear and concise comments for each public member of the Parquet library.

## General Instructions
- Make only high confidence suggestions when reviewing code changes.
- Write code with good maintainability practices, including comments on why certain design decisions were made.
- Handle edge cases and write clear exception handling.
- For libraries or external dependencies, mention their usage and purpose in comments.
- Do not throw any exceptions in `Dispose()` methods.
- Make sure public member of the library have XML documentation comments, unless the containing type is private or internal.

## Compatibility
- Ensure compatibility with .NET Standard 2.0 and 2.1.
- Always use modern .NET 10 features, and polyfill for older frameworks when necessary using compiler directives.

## Formatting

- Apply code-formatting style defined in `.editorconfig`.
- Prefer file-scoped namespace declarations and single-line using directives.
- Use pattern matching and switch expressions wherever possible.
- Use nameof instead of string literals when referring to member names.

## Performance Optimization

- Always consider performance implications of code changes.
- Suggest interesting micro optimizations where applicable.

## Testing
- Always include test cases for critical paths of the application.
- Guide users through creating unit tests.
- Do not emit "Act", "Arrange" or "Assert" comments.
- Copy existing style in nearby files for test method names and capitalization.

## Mixed
- Do NOT check Parquet.Underfloor project for code reviews.