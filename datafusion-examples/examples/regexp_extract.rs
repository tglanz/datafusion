use std::error::Error;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;

use datafusion::error::Result;
use datafusion::execution::context::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ctx = SessionContext::new();
    let match_pattern = "(a.*?b).*(c.*?d).*(e.*f)";

    let a: ArrayRef = Arc::new(StringArray::from(vec![
        "nmlkjkh", // no group, without a and without d
        "nmlakjkh", // no group, with a and without d
        "nmldkjkh", // no group, without a and with d
        "---axxxxb+++", // 1 group
        "---axxxxbcyyyyyd+++", // 2 groups
        "---axxxxbcyyyyydezzzzzzf+++", // 3 groups
    ]));

    let b: ArrayRef = Arc::new(Int32Array::from(vec![
        0,
        1,
        2,
        0,
        0,
        2,
    ]));

    let batch = RecordBatch::try_from_iter(vec![
        ("a", a),
        ("b", b)
    ])?;

    ctx.register_batch("t", batch)?;

    let scalar_cases = vec![
        ("no match 0", "axxxb__cyyyd---ezzzf", "nomatch", 0),
        ("no match 1", "axxxb__cyyyd---ezzzf", "nomatch", 1),
        ("no match 2", "axxxb__cyyyd---ezzzf", "nomatch", 2),
        ("no match 3", "axxxb__cyyyd---ezzzf", "nomatch", 3),

        ("match 0", "axxxb__cyyyd---ezzzf", match_pattern, 0),
        ("match 1", "axxxb__cyyyd---ezzzf", match_pattern, 1),
        ("match 2", "axxxb__cyyyd---ezzzf", match_pattern, 2),
        ("match 3", "axxxb__cyyyd---ezzzf", match_pattern, 3),
    ];

    for (name, scalar, pattern, group_index) in scalar_cases {
        println!("Scalar: {}", name);
        let query = format!("SELECT regexp_extract('{}', '{}', {})",
            scalar, pattern, group_index);

        ctx
            .sql(&query)
            .await?
            .show()
            .await?;
        println!();
    }

    let array_cases = vec![
        ("no match 0", "nomatch", 0),
        ("no match 1", "nomatch", 1),
        ("no match 2", "nomatch", 2),
        ("no match 3", "nomatch", 3),
        ("match 0", match_pattern, 0),
        ("match 1", match_pattern, 1),
        ("match 2", match_pattern, 2),
        ("match 3", match_pattern, 3),
    ];

    for (name, pattern, group_index) in array_cases {
        println!("Array: {}", name);
        let query = format!("SELECT a, regexp_extract(a, '{}', {}), b, '{}' as pattern FROM t",
            pattern, group_index, pattern);

        ctx
            .sql(&query)
            .await?
            .show()
            .await?;
        println!();
    }

    let array_cases_with_array_group_index= vec![
        ("no match", "nomatch"),
        ("match", match_pattern),
    ];

    for (name, pattern) in array_cases_with_array_group_index{
        println!("Array, array group: {}", name);
        let query = format!("SELECT a, regexp_extract(a, '{}', b), b, '{}' as pattern FROM t",
            pattern, pattern);

        ctx
            .sql(&query)
            .await?
            .show()
            .await?;
        println!();
    }

    Ok(())
}