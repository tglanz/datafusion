# Tal's Regex implementation and GroupBy analysis

[Datafusion's original readme](./ORIGINAL_README.md)


## Implementation of `regexp_extract` 

### Process / Thoughts / Considerations

#### External vs. internal implementation

I was thinking about where to implement the `regexp_match` UDF.

The first alternative is to create a new library, external to datafusion which depends on the datafusion crate. Implement the UDF there (i.e. a struct that has the `ScalardUDFImpl` trait). Then, when creating datafusion's `SessionContext`, we would use the `register_udf` to register the new UDF to use it.

The second alternative, which is what I went with, is to implement the new UDF in datafusion itself and register it as part of the default UDFs (inside `datafusion-functions` crate).


### My workflow


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

### Running / Testing

Run the unit tests with

    cargo test --package datafusion-functions --lib -- regex::regexpextract::tests --show-output

Run the usage example with

    cargo run --package datafusion-examples --example regexp_extract

Run the relevant sqllogictests

    cargo test --test sqllogictests -- regexp

## GroupBy analysis

General Group By flow (I didn't fully observe the whole process in code):

- Assume we allocate $P$ partitions for every of the $W$ workers
- Each worker, scans the key value pairs $(k, v)$ and maps the pair according to some partitioning scheme over the keys (determined by a `Partitioner`). Meaning, it decides which partition to send each pair according to some function on the key.
- Now that pairs with equal keys are found on the same partitions, the cluster assigns a mapping from the $P$ partitions to the $W$ workers such that each workers owns some partition.
- Each worker sends it's local partitions to the corresponding workers (meaning that there are about $N - \frac{N}{W}$ partitions being sent from each worker and about $(W - 1) (N - \frac{N}{W})$ partitions being received to each worker. 
- The worker receives the partitions and merges the pairs locally
- They proceed to operate according to the plan such that pairs with the same keys are in the same partition on the same worker.

Taking into consideration this algorithm, we understand the following:

1. The more partitions the better (as long as we have enough workers and there are no more partition than distinct keys).
2. The more the keys are located on the workers their partition is assigned to the data movement over the network (shuffling) he have.

Spark tries to optimize according to the above points.

The JavaAPI documentation [here](https://spark.apache.org/docs/latest/api/java/org/apache/spark/api/java/JavaRDDLike.html#groupBy-org.apache.spark.api.java.function.Function-), found from the [API docs](https://spark.apache.org/docs/latest/api/java/index.html). From the github, through the JavaPairRDD we get to [Scalar's RDD](https://github.com/apache/spark/blob/adc42b4b99ac4ab091b17bc3d48f40613f036ac8/core/src/main/scala/org/apache/spark/rdd/RDD.scala#) which has the group by function.

There we see that they use a `defaultPartitioner`. This is a function that attempts to select the best strategy to partition keys on the current worker, by selecting the optimal [Partitioner](https://github.com/apache/spark/blob/adc42b4b99ac4ab091b17bc3d48f40613f036ac8/core/src/main/scala/org/apache/spark/Partitioner.scala#L47) found on the given RDDs.

The default partitioner attempts to find or create a partitioner that has the most partitions - by doing so it adheres to bullet (1) above.

In addition, it looks for existing partitioner first - existing partitioner can arise from previous operations. Because the group by is not necessarily the first operation, it is possible that the data is already partitioned in some way (for example the data is read from multple parquet files, or different hdfs partitions, or maybe even if we manually partitioned the RDD previously). In this scenario, the RDD will already have some existing Partitioner already assigned to it. The `defaultPartitioner` looks for such partitioners and by doing so it adhers to bullet (2) above. The reason is that there is already some partitioning of the pairs, and if we keep with the same parititioning, the pairs already located on the assigned workers which will lead to much less data movement.

> Note: I havn't seen all of the process in code (the shuffling and the merging). I broadly recall this from previous work with spark and from previous work on distributed database internals.

In addition, spark tells us that if we want to aggregate on the values it's best to use it instead of grouping by keys and then aggregating. The reason is that instead of keeping the pairs themselves, spark can store only the accumulator per each key locally and then in the shufflying there are only two items being sent over network per pair (the key and the accumulator).

Further optimizations can be done if we allow some error in the results by utilizing randomized algorithms. In the previous approach, spark still had to read all of the records (and only send less over the network). By utilizing randomized algorithms we can sample the dataset (according to the specific algorithm and usecase) and achieve sub linear complexities.

Some relevant algorithms are from the sketch family of algorithms:

1. [Count min sketch](https://spark.apache.org/docs/3.5.2/api/java/org/apache/spark/util/sketch/CountMinSketch.html) - Approximates the number of items
2. [Hyper log log](https://en.wikipedia.org/wiki/HyperLogLog) - Approximates the number of distinct items
3. [t-digest](https://github.com/tdunning/t-digest/blob/main/README.md) - Approximates the quantuiles of a data set

> Note: this is also not from spark itself but from previous work and academy

We can further optimize the group by, by providing a native implementation.

## References

- [Pyspark regexp_extract](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html)

- [Postgres' POSIX REGEXP functions](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP)

- [Creating UDFs](https://datafusion.apache.org/library-user-guide/adding-udfs.html)

- [Similar UDF, regexp_match](https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html#datafusion.functions.regexp_match)