// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Regex expressions
use arrow::array::{Array, ArrayRef, GenericStringArray, Int64Array, OffsetSizeTrait, StringBuilder};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_int64_array, as_large_string_array, as_string_array};
use datafusion_common::{exec_err, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use regex::Regex;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Returns the first [regular expression](https://docs.rs/regex/latest/regex/#syntax) matches in a string.",
    syntax_example = "regexp_extract(str, regexp[, flags])",
    sql_example = r#"```sql
            > select regexp_extract('Köln', '[a-zA-Z]ö[a-zA-Z]{2}');
            +---------------------------------------------------------+
            | regexp_extract(Utf8("Köln"), Utf8("[a-zA-Z]ö[a-zA-Z]{2}"), 1) |
            +---------------------------------------------------------+
            | [Köln]                                                  |
            +---------------------------------------------------------+
            SELECT regexp_extract('aBc', '(b|d)', 1);
            +---------------------------------------------------+
            | regexp_extract(Utf8("aBc"), Utf8("(b|d)"), 1) |
            +---------------------------------------------------+
            | [B]                                               |
            +---------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/regexp_extract.rs)
"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to match against.
            Can be a constant or column."
    ),
    argument(
        name = "group index",
        description = "A one-based index to the matching group to extract.
            If 0 is provided, will retrieve the full match.
            Must be a constant, or column."
    ),
)]
#[derive(Debug)]
pub struct RegexpExtractFunc {
    signature: Signature,
}

impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    // input, pattern, index
                    // TypeSignature::Exact(vec![Utf8View, Utf8View, Int64]), // TBD
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Same as input
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .map(|arg| arg.to_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = regexp_extract(&args)?;

        if is_scalar {
            let scalar_value = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(scalar_value))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn concrete_regexp_extract<T: OffsetSizeTrait>(
    string_array: &GenericStringArray<T>,
    pattern_array: &GenericStringArray<T>,
    group_index_array: &Int64Array,
) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(
        // We know the extact number of entries
        string_array.len(),
        // We even know an upper bound of the memory size since regex matching will yield substrings at most
        string_array.get_buffer_memory_size(),
    );

    // If it's a scalar we would like to compile the pattern only once.
    let scalar_regex = if pattern_array.len() == 1 {
        Some(
            Regex::new(pattern_array.value(0))
                .map_err(|_| DataFusionError::Execution(
                    format!("Unable to compile pattern '{}' into regex", pattern_array.value(0))))?
        )
    } else {
        None
    };
    
    for i in 0..string_array.len() {
        let group_index = if group_index_array.len() == 1 {
            group_index_array.value(0)
        } else {
            group_index_array.value(i)
        } as usize;

        let current_regex = match &scalar_regex {
            Some(scalar_regex) => scalar_regex,
            None => {
                &Regex::new(pattern_array.value(i))
                    .map_err(|_| DataFusionError::Execution(
                        format!("Unable to compile pattern '{}' into regex", pattern_array.value(i))))?
            }
        };

        let input = string_array.value(i);

        match current_regex.captures(input) {
            Some(captures) => {
                if group_index < captures.len() {
                    builder.append_value(captures.get(group_index).map(|m| m.as_str()).unwrap_or(""));
                } else {
                    builder.append_value("");
                }
            }
            None => builder.append_value(""),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)

}

pub fn regexp_extract(args: &[ArrayRef]) -> Result<ArrayRef> {
    let input_array = &args[0];
    let pattern_array = &args[1];
    let group_index_array = as_int64_array(&args[2])?;

    match input_array.data_type() {
        DataType::Utf8 => concrete_regexp_extract(
            as_string_array(input_array)
                .map_err(|_| DataFusionError::Execution("Failed to downcast input array to string array".into()))?,
            as_string_array(pattern_array)
                .map_err(|_| DataFusionError::Execution("Failed to downcast pattern array to string array".into()))?,
            group_index_array),
        DataType::LargeUtf8 => concrete_regexp_extract(
            as_large_string_array(input_array)
                .map_err(|_| DataFusionError::Execution("Failed to downcast input array to large string array".into()))?,
            as_large_string_array(pattern_array)
                .map_err(|_| DataFusionError::Execution("Failed to downcast pattern array to large string array".into()))?,
            group_index_array),
        _ => exec_err!("Unsupported input type: {}", input_array.data_type())
    }
}

#[cfg(test)]
mod tests {
    use crate::regex::regexpextract::regexp_extract;
    use arrow::array::{Array, Int64Array, StringArray, StringBuilder};
    use std::sync::Arc;

    #[test]
    fn test_case_sensitive_regexp_extract() {
        let values = StringArray::from(vec!["axb_cyd_ezf"; 5]);

        let patterns = StringArray::from(vec![
            "(a.*?b).*(c.*?d).*(e.*f)",
            "(a.*?b).*(c.*?d).*(e.*f)",
            "(a.*?b)",
            "(c.*?d)",
            "nomatch",
        ]);

        let group_indices = Int64Array::from(vec![
            0,
            2,
            2,
            1,
            0,
        ]);

        let expected = {
            let mut expected_builder = StringBuilder::with_capacity(
                values.len(),
                values.get_buffer_memory_size(),
            );
            expected_builder.append_value("axb_cyd_ezf");
            expected_builder.append_value("cyd");
            expected_builder.append_value("");
            expected_builder.append_value("cyd");
            expected_builder.append_value("");
            expected_builder.finish()
        };

        let actual = regexp_extract(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(group_indices),
        ]).unwrap();

        println!("{:?}", actual);

        assert_eq!(actual.as_ref(), &expected);
    }
}
