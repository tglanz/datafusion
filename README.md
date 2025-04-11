# Tal's Regex implementation and GroupBy analysis

[Datafusion's original readme](./ORIGINAL_README.md)

## Process / Thoughts / Considerations

### My workflow

#### regexpextract implementation

- [x] Found a similar UDF in the codebase [regexpmatch](./datafusion/functions/src/regex/regexpmatch.rs) (was actually looking in the string functions initially just to get inspiration about how to process string arrays and the regex module caught my eye).
- [x] Created an example entrypoint [regexp_extract](./datafusion-examples/examples/regexp_extract.rs) so that I could keep as a working baseline. 
- [x] Cloned the [regexpmatch](./datafusion/functions/src/regex/regexpmatch.rs) to [regexpextract](./datafusion/functions/src/regex/regexpextract.rs) as is. Added the new UDF to the list in the [regexp module](./datafusion/functions/src/regex/mod.rs) because datafusion automatically registers all those UDFs in [datafusion-functions](./datafusion\functions\src\lib.rs) at the `register_all` function.
- [x] Altered the example so that it will reflect what the target functionality is (i.e. add an index indicating the group to extract).
- [ ] Enhanced the new UDF so that the example will behave like we want (signature and invoke logic).
- [ ] For best practice purposes, modify the `regexpmatch` UDF to use the `regexpextract` UDF because it is a specific usecase of it (where the group index is always 1).
- [ ] Add `sqllogictests`

### External vs. internal implementation

I was thinking about where to implement the `regexp_match` UDF.

The first alternative is to create a new library, external to datafusion which depends on the datafusion crate. Implement the UDF there (i.e. a struct that has the `ScalardUDFImpl` trait). Then, when creating datafusion's `SessionContext`, we would use the `register_udf` to register the new UDF to use it.

The second alternative, which is what I went with, is to implement the new UDF in datafusion itself and register it as part of the default UDFs (inside `datafusion-functions` crate).

## Running / Testing

Run the unit tests with

    cargo test --package datafusion-functions --lib -- regex::regexpextract::tests --show-output

Run the usage example with

    cargo run --package datafusion-examples --example regexp_extract

## References

- [Pyspark regexp_extract](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html)

- [Creating UDFs](https://datafusion.apache.org/library-user-guide/adding-udfs.html)

- [Similar UDF, regexp_match](https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html#datafusion.functions.regexp_match)