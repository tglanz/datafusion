# Tal's Regex implementation and GroupBy analysis

[Datafusion's original readme](./ORIGINAL_README.md)

## Process / Thoughts / Considerations

### External vs. internal implementation

I was thinking about where to implement the `regexp_match` UDF.

The first alternative is to create a new library, external to datafusion which depends on the datafusion crate. Implement the UDF there (i.e. a struct that has the `ScalardUDFImpl` trait). Then, when creating datafusion's `SessionContext`, we would use the `register_udf` to register the new UDF to use it.

The second alternative, which is what I went with, is to implement the new UDF in datafusion itself and register it as part of the default UDFs (inside `datafusion/functions` create).

## Running / Testing

Run the unit tests with

    cargo test --package datafusion-functions --lib -- regex::regexpextract::tests --show-output

Run the usage example with

    cargo run --package datafusion-examples --example regexp_extract

## References

- [Pyspark regexp_extract](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html)

- [Creating UDFs](https://datafusion.apache.org/library-user-guide/adding-udfs.html)

- [Similar UDF, regexp_match](https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html#datafusion.functions.regexp_match)