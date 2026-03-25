## Coding style
- Always use the latest version C#, currently C# 14 features.
- Write clear and concise comments for each public member of the Parquet library.
- Always use modern .NET 10 features, and polyfill for older frameworks when necessary using compiler directives.
- Apply code-formatting style defined in `.editorconfig`.

## General Instructions
- Make only high confidence suggestions when reviewing code changes.
- Write code with good maintainability practices, including comments on why certain design decisions were made.
- Handle edge cases and write clear exception handling.
- For libraries or external dependencies, mention their usage and purpose in comments.
- Do not throw any exceptions in `Dispose()` methods.
- Make sure public members of the library have XML documentation comments, unless the containing type is private or internal.

## Performance Optimization

- Always consider performance implications of code changes.
- Suggest interesting micro optimizations where applicable.

## Testing
- Always include test cases for critical paths of the application.
- Guide users through creating unit tests.
- Do not emit "Act", "Arrange" or "Assert" comments.
- Copy existing style in nearby files for test method names and capitalization.