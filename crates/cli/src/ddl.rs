//! Query-engine DDL and views for schema-mode Parquet output.
//!
//! The generated text only maps the schema's own column names and types; it
//! never guesses a partition layout, because the writer does not partition.

use anyhow::Result;
use parquet_sink::{ColumnType, SchemaSpec};

/// Engines `emit-ddl` can target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Duckdb,
    Athena,
    Bigquery,
}

impl Target {
    pub fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "duckdb" => Self::Duckdb,
            "athena" => Self::Athena,
            "bigquery" => Self::Bigquery,
            other => anyhow::bail!("unknown target `{other}`; use duckdb, athena or bigquery"),
        })
    }
}

/// Athena column type for a schema type; decimals carry their precision.
fn athena_type(column: &parquet_sink::ColumnSpec) -> String {
    match column.column_type {
        ColumnType::Boolean => "boolean".to_string(),
        ColumnType::Int8 | ColumnType::Int16 => "smallint".to_string(),
        ColumnType::Int32 | ColumnType::UInt8 | ColumnType::UInt16 => "int".to_string(),
        ColumnType::Int64 | ColumnType::UInt32 | ColumnType::UInt64 | ColumnType::TimestampNs => {
            "bigint".to_string()
        }
        ColumnType::Float32 => "float".to_string(),
        ColumnType::Float64 => "double".to_string(),
        ColumnType::Utf8 => "string".to_string(),
        ColumnType::Binary => "binary".to_string(),
        ColumnType::Date32 => "date".to_string(),
        ColumnType::TimestampMs | ColumnType::TimestampUs => "timestamp".to_string(),
        ColumnType::Decimal128 => format!(
            "decimal({},{})",
            column
                .precision
                .unwrap_or(parquet_sink::DEFAULT_DECIMAL_PRECISION),
            column.scale.unwrap_or(0)
        ),
    }
}

/// BigQuery column type. Nanoseconds do not fit `TIMESTAMP` (microsecond
/// precision), so they stay `INT64`.
fn bigquery_type(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Boolean => "BOOL",
        ColumnType::Int8
        | ColumnType::Int16
        | ColumnType::Int32
        | ColumnType::UInt8
        | ColumnType::UInt16
        | ColumnType::UInt32 => "INT64",
        ColumnType::Int64 | ColumnType::UInt64 | ColumnType::TimestampNs => "INT64",
        ColumnType::Float32 | ColumnType::Float64 => "FLOAT64",
        ColumnType::Utf8 => "STRING",
        ColumnType::Binary => "BYTES",
        ColumnType::Date32 => "DATE",
        ColumnType::TimestampMs | ColumnType::TimestampUs => "TIMESTAMP",
        ColumnType::Decimal128 => "NUMERIC",
    }
}

fn quote(target: Target, name: &str) -> String {
    match target {
        // DuckDB accepts double quotes; Athena and BigQuery use backticks.
        Target::Duckdb => format!("\"{name}\""),
        Target::Athena | Target::Bigquery => format!("`{name}`"),
    }
}

/// Renders the statement for one target.
///
/// * DuckDB reads a glob, so it gets a `CREATE OR REPLACE VIEW`.
/// * Athena needs an external table over the location.
/// * BigQuery needs an external table with OPTIONS.
pub fn render(spec: &SchemaSpec, target: Target, table: &str, location: &str) -> Result<String> {
    if table.trim().is_empty() {
        anyhow::bail!("--table must not be empty");
    }
    match target {
        Target::Duckdb => {
            let columns: Vec<String> = spec
                .columns
                .iter()
                .map(|column| {
                    format!(
                        "  {} {}",
                        quote(target, &column.name),
                        duckdb_type(column.column_type)
                    )
                })
                .collect();
            let notes = columns.join(",\n  -- ");
            Ok(format!(
                "-- Parquet written by `tsdb2parqtel convert --schema`.\nCREATE OR REPLACE VIEW {table} AS\nSELECT * FROM read_parquet('{location}/*.parquet');\n\n-- Column types in the files:\n  -- {notes}\n"
            ))
        }
        Target::Athena => {
            if location.is_empty() {
                anyhow::bail!("Athena needs --location (the s3:// prefix holding the files)");
            }
            let columns: Vec<String> = spec
                .columns
                .iter()
                .map(|column| format!("  {} {}", quote(target, &column.name), athena_type(column)))
                .collect();
            Ok(format!(
                "CREATE EXTERNAL TABLE IF NOT EXISTS {table} (\n{}\n)\nSTORED AS PARQUET\nLOCATION '{location}/';\n",
                columns.join(",\n")
            ))
        }
        Target::Bigquery => {
            if location.is_empty() {
                anyhow::bail!("BigQuery needs --location (the gs:// prefix holding the files)");
            }
            let columns: Vec<String> = spec
                .columns
                .iter()
                .map(|column| {
                    format!(
                        "  {} {}",
                        quote(target, &column.name),
                        bigquery_type(column.column_type)
                    )
                })
                .collect();
            Ok(format!(
                "CREATE OR REPLACE EXTERNAL TABLE {table} (\n{}\n)\nOPTIONS (\n  format = 'PARQUET',\n  uris = ['{location}/*.parquet']\n);\n",
                columns.join(",\n")
            ))
        }
    }
}

fn duckdb_type(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Int8 => "TINYINT",
        ColumnType::Int16 => "SMALLINT",
        ColumnType::Int32 => "INTEGER",
        ColumnType::Int64 => "BIGINT",
        ColumnType::UInt8 => "UTINYINT",
        ColumnType::UInt16 => "USMALLINT",
        ColumnType::UInt32 => "UINTEGER",
        ColumnType::UInt64 => "UBIGINT",
        ColumnType::Float32 => "FLOAT",
        ColumnType::Float64 => "DOUBLE",
        ColumnType::Utf8 => "VARCHAR",
        ColumnType::Binary => "BLOB",
        ColumnType::Date32 => "DATE",
        ColumnType::TimestampMs | ColumnType::TimestampUs | ColumnType::TimestampNs => "TIMESTAMP",
        ColumnType::Decimal128 => "DECIMAL",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn spec() -> Result<SchemaSpec> {
        SchemaSpec::from_str(
            "columns:\n  - name: timestamp\n    type: timestamp_ms\n    source: timestamp\n  - name: metric\n    type: utf8\n    source: metric_name\n  - name: value\n    type: float64\n    source: value\n  - name: series_fingerprint\n    type: uint64\n    source: series_fingerprint\n",
        )
        .map_err(|error| anyhow::anyhow!("{error}"))
    }

    #[test]
    fn duckdb_view_reads_the_glob() -> Result<()> {
        let text = render(&spec()?, Target::Duckdb, "metrics", "/lake/metrics")?;
        assert!(text.contains("CREATE OR REPLACE VIEW metrics AS"));
        assert!(text.contains("read_parquet('/lake/metrics/*.parquet')"));
        assert!(text.contains("\"timestamp\" TIMESTAMP"));
        assert!(text.contains("\"series_fingerprint\" UBIGINT"));
        Ok(())
    }

    #[test]
    fn athena_table_uses_backticks_and_a_trailing_slash() -> Result<()> {
        let text = render(&spec()?, Target::Athena, "metrics", "s3://bucket/metrics")?;
        assert!(text.contains("CREATE EXTERNAL TABLE IF NOT EXISTS metrics ("));
        assert!(text.contains("`metric` string"));
        assert!(text.contains("LOCATION 's3://bucket/metrics/'"));
        assert!(text.contains("STORED AS PARQUET"));
        Ok(())
    }

    #[test]
    fn bigquery_keeps_nanoseconds_as_int64() -> Result<()> {
        let spec = SchemaSpec::from_str(
            "columns:\n  - name: ts\n    type: timestamp_ns\n    source: timestamp\n  - name: v\n    type: float64\n    source: value\n",
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;
        let text = render(&spec, Target::Bigquery, "m", "gs://b/m")?;
        assert!(text.contains("`ts` INT64"), "{text}");
        assert!(text.contains("uris = ['gs://b/m/*.parquet']"));
        Ok(())
    }

    #[test]
    fn locations_are_required_for_warehouse_targets() -> Result<()> {
        assert!(render(&spec()?, Target::Athena, "m", "").is_err());
        assert!(render(&spec()?, Target::Bigquery, "m", "").is_err());
        Ok(())
    }
}
