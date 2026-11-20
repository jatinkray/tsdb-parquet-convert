//! Column buffers and the conversion rules from a source value to a column type.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::TimeUnit;

use crate::spec::{ColumnSpec, ColumnType};
use crate::WriteError;

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// A value read from one series row, before it is cast to a column type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SourceValue<'a> {
    Missing,
    Text(&'a str),
    Float(f64),
    Int(i64),
    UInt(u64),
}

/// Error context for one value.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ValueContext<'a> {
    pub column: &'a str,
    pub series: u64,
    pub nullable: bool,
}

impl<'a> ValueContext<'a> {
    fn error(&self, message: impl Into<String>) -> WriteError {
        WriteError::Row {
            column: self.column.to_string(),
            series: self.series,
            message: message.into(),
        }
    }

    fn missing(&self) -> Option<WriteError> {
        (!self.nullable)
            .then(|| self.error("source value is missing and the column is not nullable"))
    }
}

/// One column's values, in row order.
pub(crate) enum ColumnData {
    Boolean(Vec<Option<bool>>),
    Int8(Vec<Option<i8>>),
    Int16(Vec<Option<i16>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    UInt8(Vec<Option<u8>>),
    UInt16(Vec<Option<u16>>),
    UInt32(Vec<Option<u32>>),
    UInt64(Vec<Option<u64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Utf8(Vec<Option<String>>),
    Binary(Vec<Option<Vec<u8>>>),
    Date32(Vec<Option<i32>>),
    Timestamp(TimeUnit, Option<Arc<str>>, Vec<Option<i64>>),
    Decimal128 {
        precision: u8,
        scale: i8,
        values: Vec<Option<i128>>,
    },
}

impl ColumnData {
    pub(crate) fn new(column: &ColumnSpec) -> Self {
        let timezone = column.timezone.clone().map(Arc::from);
        match column.column_type {
            ColumnType::Boolean => Self::Boolean(Vec::new()),
            ColumnType::Int8 => Self::Int8(Vec::new()),
            ColumnType::Int16 => Self::Int16(Vec::new()),
            ColumnType::Int32 => Self::Int32(Vec::new()),
            ColumnType::Int64 => Self::Int64(Vec::new()),
            ColumnType::UInt8 => Self::UInt8(Vec::new()),
            ColumnType::UInt16 => Self::UInt16(Vec::new()),
            ColumnType::UInt32 => Self::UInt32(Vec::new()),
            ColumnType::UInt64 => Self::UInt64(Vec::new()),
            ColumnType::Float32 => Self::Float32(Vec::new()),
            ColumnType::Float64 => Self::Float64(Vec::new()),
            ColumnType::Utf8 => Self::Utf8(Vec::new()),
            ColumnType::Binary => Self::Binary(Vec::new()),
            ColumnType::Date32 => Self::Date32(Vec::new()),
            ColumnType::TimestampMs => Self::Timestamp(TimeUnit::Millisecond, timezone, Vec::new()),
            ColumnType::TimestampUs => Self::Timestamp(TimeUnit::Microsecond, timezone, Vec::new()),
            ColumnType::TimestampNs => Self::Timestamp(TimeUnit::Nanosecond, timezone, Vec::new()),
            ColumnType::Decimal128 => Self::Decimal128 {
                precision: column.decimal_precision(),
                scale: column.decimal_scale(),
                values: Vec::new(),
            },
        }
    }

    /// Casts one source value into this column and appends it.
    pub(crate) fn push(
        &mut self,
        source: SourceValue<'_>,
        ctx: ValueContext<'_>,
    ) -> Result<(), WriteError> {
        if matches!(source, SourceValue::Missing) {
            if let Some(error) = ctx.missing() {
                return Err(error);
            }
        }
        match self {
            Self::Boolean(values) => values.push(as_bool(source, ctx)?),
            Self::Int8(values) => {
                values.push(as_int(source, ctx)?.map(|v| narrow(v, ctx)).transpose()?)
            }
            Self::Int16(values) => {
                values.push(as_int(source, ctx)?.map(|v| narrow(v, ctx)).transpose()?)
            }
            Self::Int32(values) => {
                values.push(as_int(source, ctx)?.map(|v| narrow(v, ctx)).transpose()?)
            }
            Self::Int64(values) => values.push(as_int(source, ctx)?),
            Self::UInt8(values) => {
                values.push(as_uint(source, ctx)?.map(|v| narrow(v, ctx)).transpose()?)
            }
            Self::UInt16(values) => {
                values.push(as_uint(source, ctx)?.map(|v| narrow(v, ctx)).transpose()?)
            }
            Self::UInt32(values) => {
                values.push(as_uint(source, ctx)?.map(|v| narrow(v, ctx)).transpose()?)
            }
            Self::UInt64(values) => values.push(as_uint(source, ctx)?),
            Self::Float32(values) => {
                let value = as_f64(source, ctx)?;
                values.push(value.map(|v| v as f32));
            }
            Self::Float64(values) => values.push(as_f64(source, ctx)?),
            Self::Utf8(values) => values.push(as_text(source, ctx)?),
            Self::Binary(values) => {
                let value = match source {
                    SourceValue::Missing => None,
                    SourceValue::Text(text) => Some(text.as_bytes().to_vec()),
                    SourceValue::Float(value) => Some(value.to_le_bytes().to_vec()),
                    SourceValue::Int(value) => Some(value.to_le_bytes().to_vec()),
                    SourceValue::UInt(value) => Some(value.to_le_bytes().to_vec()),
                };
                values.push(value);
            }
            Self::Date32(values) => {
                let nanos = as_nanos(source, ctx)?;
                match nanos {
                    None => values.push(None),
                    Some(nanos) => {
                        let days = nanos.div_euclid(NANOS_PER_DAY);
                        let days = i32::try_from(days).map_err(|_| {
                            ctx.error(format!(
                                "timestamp {nanos} ns is outside the `date32` range"
                            ))
                        })?;
                        values.push(Some(days));
                    }
                }
            }
            Self::Timestamp(unit, _, values) => {
                let nanos = as_nanos(source, ctx)?;
                let converted = match (nanos, unit) {
                    (None, _) => None,
                    (Some(nanos), TimeUnit::Nanosecond) => Some(nanos),
                    (Some(nanos), TimeUnit::Microsecond) => Some(nanos.div_euclid(1_000)),
                    (Some(nanos), TimeUnit::Millisecond) => Some(nanos.div_euclid(1_000_000)),
                    (Some(_), other) => {
                        return Err(ctx.error(format!("unsupported timestamp unit {other:?}")))
                    }
                };
                values.push(converted);
            }
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => values.push(as_decimal(source, *precision, *scale, ctx)?),
        }
        Ok(())
    }

    pub(crate) fn to_array(&self) -> Result<ArrayRef, WriteError> {
        Ok(match self {
            Self::Boolean(values) => Arc::new(BooleanArray::from(values.clone())),
            Self::Int8(values) => Arc::new(Int8Array::from(values.clone())),
            Self::Int16(values) => Arc::new(Int16Array::from(values.clone())),
            Self::Int32(values) => Arc::new(Int32Array::from(values.clone())),
            Self::Int64(values) => Arc::new(Int64Array::from(values.clone())),
            Self::UInt8(values) => Arc::new(UInt8Array::from(values.clone())),
            Self::UInt16(values) => Arc::new(UInt16Array::from(values.clone())),
            Self::UInt32(values) => Arc::new(UInt32Array::from(values.clone())),
            Self::UInt64(values) => Arc::new(UInt64Array::from(values.clone())),
            Self::Float32(values) => Arc::new(Float32Array::from(values.clone())),
            Self::Float64(values) => Arc::new(Float64Array::from(values.clone())),
            Self::Utf8(values) => {
                let mut builder = StringBuilder::new();
                for value in values {
                    builder.append_option(value.as_deref());
                }
                Arc::new(builder.finish())
            }
            Self::Binary(values) => {
                let mut builder = BinaryBuilder::new();
                for value in values {
                    builder.append_option(value.as_deref());
                }
                Arc::new(builder.finish())
            }
            Self::Date32(values) => Arc::new(Date32Array::from(values.clone())),
            Self::Timestamp(unit, timezone, values) => match unit {
                TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::from(values.clone())
                        .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::from(values.clone())
                        .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::from(values.clone())
                        .with_timezone_opt(timezone.clone()),
                ),
                other => {
                    return Err(WriteError::Column {
                        column: "<timestamp>".to_string(),
                        message: format!("unsupported timestamp unit {other:?}"),
                    })
                }
            },
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => Arc::new(
                Decimal128Array::from(values.clone())
                    .with_precision_and_scale(*precision, *scale)?,
            ),
        })
    }
}

/// Exact integer narrowing with a message that names the value and target.
trait Narrow: Sized {
    fn try_narrow(value: i128) -> Option<Self>;
}

macro_rules! narrow_impl {
    ($type:ty) => {
        impl Narrow for $type {
            fn try_narrow(value: i128) -> Option<Self> {
                <$type>::try_from(value).ok()
            }
        }
    };
}

narrow_impl!(i8);
narrow_impl!(i16);
narrow_impl!(i32);
narrow_impl!(i64);
narrow_impl!(u8);
narrow_impl!(u16);
narrow_impl!(u32);
narrow_impl!(u64);

fn narrow<T: Narrow, S: Into<i128>>(source: S, ctx: ValueContext<'_>) -> Result<T, WriteError> {
    let value: i128 = source.into();
    T::try_narrow(value).ok_or_else(|| {
        ctx.error(format!(
            "value {value} does not fit the column's integer type"
        ))
    })
}

fn parse_text_number(text: &str, ctx: ValueContext<'_>) -> Result<f64, WriteError> {
    let trimmed = text.trim();
    trimmed.parse::<f64>().map_err(|_| {
        ctx.error(format!(
            "`{trimmed}` is not a number, so it cannot fill this numeric column"
        ))
    })
}

fn as_f64(source: SourceValue<'_>, ctx: ValueContext<'_>) -> Result<Option<f64>, WriteError> {
    Ok(match source {
        SourceValue::Missing => None,
        SourceValue::Float(value) => Some(value),
        SourceValue::Int(value) => Some(value as f64),
        SourceValue::UInt(value) => Some(value as f64),
        SourceValue::Text(text) => Some(parse_text_number(text, ctx)?),
    })
}

fn rounded_i128(value: f64, ctx: ValueContext<'_>) -> Result<i128, WriteError> {
    let rounded = value.round();
    if !rounded.is_finite() {
        return Err(ctx.error(format!("{value} is not a finite number")));
    }
    if !(-1.7e38..1.7e38).contains(&rounded) {
        return Err(ctx.error(format!("{value} is outside the 128-bit integer range")));
    }
    Ok(rounded as i128)
}

fn as_int(source: SourceValue<'_>, ctx: ValueContext<'_>) -> Result<Option<i64>, WriteError> {
    Ok(match source {
        SourceValue::Missing => None,
        SourceValue::Int(value) => Some(value),
        SourceValue::UInt(value) => Some(narrow(value as i128, ctx)?),
        SourceValue::Float(value) => Some(narrow(rounded_i128(value, ctx)?, ctx)?),
        SourceValue::Text(text) => {
            let value = parse_text_number(text, ctx)?;
            Some(narrow(rounded_i128(value, ctx)?, ctx)?)
        }
    })
}

fn as_uint(source: SourceValue<'_>, ctx: ValueContext<'_>) -> Result<Option<u64>, WriteError> {
    let value = as_int_wide(source, ctx)?;
    Ok(match value {
        None => None,
        Some(value) if value < 0 => {
            return Err(ctx.error(format!(
                "value {value} is negative and the column is unsigned"
            )))
        }
        Some(value) => Some(narrow(value, ctx)?),
    })
}

fn as_int_wide(source: SourceValue<'_>, ctx: ValueContext<'_>) -> Result<Option<i128>, WriteError> {
    Ok(match source {
        SourceValue::Missing => None,
        SourceValue::Int(value) => Some(i128::from(value)),
        SourceValue::UInt(value) => Some(i128::from(value)),
        SourceValue::Float(value) => Some(rounded_i128(value, ctx)?),
        SourceValue::Text(text) => Some(rounded_i128(parse_text_number(text, ctx)?, ctx)?),
    })
}

fn as_bool(source: SourceValue<'_>, ctx: ValueContext<'_>) -> Result<Option<bool>, WriteError> {
    Ok(match source {
        SourceValue::Missing => None,
        SourceValue::Int(value) => Some(value != 0),
        SourceValue::UInt(value) => Some(value != 0),
        SourceValue::Float(value) => Some(value != 0.0),
        SourceValue::Text(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" | "yes" => Some(true),
            "false" | "f" | "0" | "no" => Some(false),
            other => {
                return Err(ctx.error(format!(
                    "`{other}` is not a boolean (use true/false, 1/0, yes/no)"
                )))
            }
        },
    })
}

fn as_text(source: SourceValue<'_>, ctx: ValueContext<'_>) -> Result<Option<String>, WriteError> {
    Ok(match source {
        SourceValue::Missing => None,
        SourceValue::Text(text) => Some(text.to_string()),
        SourceValue::Int(value) => Some(value.to_string()),
        SourceValue::UInt(value) => Some(value.to_string()),
        SourceValue::Float(value) => {
            if !value.is_finite() {
                return Err(ctx.error(format!("{value} is not a finite number")));
            }
            Some(value.to_string())
        }
    })
}

/// Timestamps as nanoseconds since the Unix epoch.
fn as_nanos(source: SourceValue<'_>, ctx: ValueContext<'_>) -> Result<Option<i64>, WriteError> {
    let seconds = match source {
        SourceValue::Missing => return Ok(None),
        SourceValue::Int(nanos) => return Ok(Some(nanos)),
        SourceValue::UInt(value) => {
            return i64::try_from(value).map(Some).map_err(|_| {
                ctx.error(format!(
                    "timestamp {value} ns does not fit a signed 64-bit integer"
                ))
            })
        }
        SourceValue::Float(seconds) => seconds,
        SourceValue::Text(text) => {
            let trimmed = text.trim();
            if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(trimmed) {
                let nanos = parsed.timestamp_nanos_opt().ok_or_else(|| {
                    ctx.error(format!(
                        "`{trimmed}` is outside the representable timestamp range"
                    ))
                })?;
                return Ok(Some(nanos));
            }
            parse_text_number(trimmed, ctx)?
        }
    };
    if !seconds.is_finite() {
        return Err(ctx.error(format!("{seconds} is not a finite timestamp")));
    }
    let nanos = seconds * 1e9;
    if !(-9.2e18..9.2e18).contains(&nanos) {
        return Err(ctx.error(format!(
            "timestamp {seconds}s is outside the 64-bit nanosecond range"
        )));
    }
    Ok(Some(nanos.round() as i64))
}

fn as_decimal(
    source: SourceValue<'_>,
    precision: u8,
    scale: i8,
    ctx: ValueContext<'_>,
) -> Result<Option<i128>, WriteError> {
    let value = match as_text(source, ctx)? {
        None => return Ok(None),
        Some(text) => text.trim().parse::<f64>().map_err(|_| {
            ctx.error(format!(
                "`{text}` is not a number, so it cannot fill a decimal column"
            ))
        })?,
    };
    if !value.is_finite() {
        return Err(ctx.error(format!("{value} is not a finite decimal value")));
    }
    let scaled = (value * 10f64.powi(i32::from(scale))).round();
    let limit = 10f64.powi(i32::from(precision));
    if scaled.abs() >= limit {
        return Err(ctx.error(format!(
            "{value} does not fit decimal({precision}, {scale})"
        )));
    }
    Ok(Some(scaled as i128))
}
