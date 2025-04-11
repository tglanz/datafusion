use std::error::Error;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;

use datafusion::execution::context::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ctx = SessionContext::new();

    let a: ArrayRef = Arc::new(StringArray::from(vec![
        "abcdaxxxd",
        "asdf",
        "abab",
        "cdcd",
    ]));

    let b: ArrayRef = Arc::new(Int32Array::from(vec![
        10,
        20,
        24,
        27,
    ]));

    let batch = RecordBatch::try_from_iter(vec![
        ("a", a),
        ("b", b)
    ])?;

    ctx.register_batch("t", batch)?;

    let df = ctx
        .sql("SELECT a, regexp_extract(a, '(a.*d)', 1, 'i'), b FROM t")
        .await?;

    df.show().await?;

    Ok(())
}