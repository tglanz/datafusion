# Tal's Regex implementation and GroupBy analysis

[Datafusion's original readme](./ORIGINAL_README.md)

## Process / Thoughts / Considerations

### My workflow



#### Implementation of `regexp_extract` 

- [x] Found a similar UDF in the codebase [regexpmatch](./datafusion/functions/src/regex/regexpmatch.rs) (was actually looking in the string functions initially just to get inspiration about how to process string arrays and the regex module caught my eye).
- [x] Created an example entrypoint [regexp_extract](./datafusion-examples/examples/regexp_extract.rs) so that I could keep as a working baseline. 
- [x] Cloned the [regexpmatch](./datafusion/functions/src/regex/regexpmatch.rs) to [regexpextract](./datafusion/functions/src/regex/regexpextract.rs) as is. Added the new UDF to the list in the [regexp module](./datafusion/functions/src/regex/mod.rs) because datafusion automatically registers all those UDFs in [datafusion-functions](./datafusion\functions\src\lib.rs) at the `register_all` function.
- [x] Altered the example so that it will reflect what the target functionality is (i.e. add an index indicating the group to extract).
- [x] Enhanced the new UDF so that the example will behave like we want (signature and invoke logic).
- [ ] ~~For best practice purposes, modify the `regexpmatch` UDF to use the `regexpextract` UDF because it is a specific usecase of it (where the group index is always 1).~~ We diverged a bit from the `regexpmath`. Specifically:
  - We currently don't use arrows regexp kernels but the std Regex (which is less desirable, but I had some issues and didn't want to waste anymore time on it).
  - We don't fully support `Utf8View`. Either Ill learn how to handle different types better, or we need a similar implementation of the `concrete_regex_extract` to `StringViewArray` (since it is not a `GenericByteArray<GenericStringType<OffsetSizeImpl>>`).
- [x] Add `sqllogictests`

**Encountered issues / things that I think can be better**:

1. I tried, for about an hour, to use arrow's [regexp_match](https://docs.rs/arrow/latest/arrow/compute/kernels/regexp/fn.regexp_match.html) but had some difficulties because the returned arrays are of type List (since each element is the list of matches). Finally I used [Regex](https://docs.rs/regex/latest/regex/) which I incoked per element while looping on the arrays - which I believe is less efficient.

2. `Utf8View` is not supported. I saw that if I allow `Utf8View` in my signature, datafusion will pass me some arrays which are `StringViews`. I _belive_ it is some form of optimization (can't tell for sure yet). It would take me longer to implement (I wasted some time on it).

3. Havn't got to implement flags argument well (in the invoke, we need to check whether we have a fourth array).

### External vs. internal implementation

I was thinking about where to implement the `regexp_match` UDF.

The first alternative is to create a new library, external to datafusion which depends on the datafusion crate. Implement the UDF there (i.e. a struct that has the `ScalardUDFImpl` trait). Then, when creating datafusion's `SessionContext`, we would use the `register_udf` to register the new UDF to use it.

The second alternative, which is what I went with, is to implement the new UDF in datafusion itself and register it as part of the default UDFs (inside `datafusion-functions` crate).

## Running / Testing

Run the unit tests with

    cargo test --package datafusion-functions --lib -- regex::regexpextract::tests --show-output

Run the usage example with

    cargo run --package datafusion-examples --example regexp_extract

Run the relevant sqllogictests

    cargo test --test sqllogictests -- regexp

## References

- [Pyspark regexp_extract](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html)

- [Postgres' POSIX REGEXP functions](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP)

- [Creating UDFs](https://datafusion.apache.org/library-user-guide/adding-udfs.html)

- [Similar UDF, regexp_match](https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html#datafusion.functions.regexp_match)