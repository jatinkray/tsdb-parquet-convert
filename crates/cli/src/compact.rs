//! Merges small Parquet files into fewer, larger ones.
//!
//! Row order is preserved, so a compacted dataset is the same sequence of rows
//! as its input. A per-series digest is computed over both sides and must
//! match, which makes a silent row loss or reordering a hard error.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet_sink::{SchemaSpec, WriteOptions};
use serde::Serialize;

use crate::verify::{Digest, SchemaLayout};

#[derive(Debug, Clone, Serialize)]
pub struct CompactReport {
    pub input_files: usize,
    pub output_files: usize,
    pub rows: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// Rewrites every `*.parquet` under `input_dir` into `output_dir`, packing
/// `rows_per_file` rows per output file. Rows are never reordered, so the
/// output is the same sequence of rows as the input.
pub fn compact(
    input_dir: &Path,
    output_dir: &Path,
    spec: &SchemaSpec,
    options: &WriteOptions,
    rows_per_file: usize,
) -> Result<CompactReport> {
    if rows_per_file == 0 {
        anyhow::bail!("--rows-per-file must be greater than 0");
    }
    let layout = SchemaLayout::resolve(spec)?;
    let mut files = collect_files(input_dir)?;
    if files.is_empty() {
        anyhow::bail!("no Parquet files under {}", input_dir.display());
    }
    files.sort();
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating {}", output_dir.display()))?;

    let schema = read_schema(&files[0])?;
    let properties =
        parquet_sink::writer_properties(options).map_err(|error| anyhow::anyhow!("{error}"))?;

    let mut digest: BTreeMap<u64, Digest> = BTreeMap::new();
    let mut written_digest: BTreeMap<u64, Digest> = BTreeMap::new();
    let mut bytes_before = 0u64;
    let mut output = Output::new(output_dir, Arc::clone(&schema), properties);

    for path in &files {
        bytes_before += std::fs::metadata(path)?.len();
        let file =
            std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        if builder.schema().as_ref() != schema.as_ref() {
            anyhow::bail!(
                "{} has a different schema than {}; compact one layout at a time",
                path.display(),
                files[0].display()
            );
        }
        for record_batch in builder.build()? {
            let record_batch = record_batch?;
            layout.update_digest(&record_batch, &mut digest)?;
            layout.update_digest(&record_batch, &mut written_digest)?;
            output.write(&record_batch, rows_per_file)?;
        }
    }
    output.close()?;
    let bytes_after = output.bytes;
    let output_files = output.files;

    if written_digest != digest {
        anyhow::bail!(
            "compaction changed the data: {} series in, {} series out; the input rows are not a faithful copy",
            digest.len(),
            written_digest.len()
        );
    }

    Ok(CompactReport {
        input_files: files.len(),
        output_files,
        rows: digest.values().map(|digest| digest.samples).sum(),
        bytes_before,
        bytes_after,
    })
}

fn collect_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(dir).max_depth(2).into_iter() {
        let entry = entry?;
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "parquet")
        {
            files.push(entry.path().to_path_buf());
        }
    }
    Ok(files)
}

fn read_schema(path: &Path) -> Result<SchemaRef> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.schema().clone())
}

/// One open output file at a time, closed as soon as it reaches the budget.
struct Output<'a> {
    dir: &'a Path,
    schema: SchemaRef,
    properties: parquet::file::properties::WriterProperties,
    writer: Option<(PathBuf, ArrowWriter<std::fs::File>)>,
    rows: usize,
    files: usize,
    bytes: u64,
}

impl<'a> Output<'a> {
    fn new(
        dir: &'a Path,
        schema: SchemaRef,
        properties: parquet::file::properties::WriterProperties,
    ) -> Self {
        Self {
            dir,
            schema,
            properties,
            writer: None,
            rows: 0,
            files: 0,
            bytes: 0,
        }
    }

    fn write(&mut self, batch: &RecordBatch, rows_per_file: usize) -> Result<()> {
        if self.writer.is_none() {
            self.open()?;
        }
        let Some((_, writer)) = self.writer.as_mut() else {
            anyhow::bail!("no open writer");
        };
        writer.write(batch)?;
        self.rows += batch.num_rows();
        if self.rows >= rows_per_file {
            self.close()?;
            self.rows = 0;
        }
        Ok(())
    }

    fn open(&mut self) -> Result<()> {
        let name = format!("data_compact_{:05}.parquet", self.files);
        let path = self.dir.join(&name);
        let file =
            std::fs::File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        let writer = ArrowWriter::try_new(
            file,
            Arc::clone(&self.schema),
            Some(self.properties.clone()),
        )?;
        self.writer = Some((path, writer));
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        let Some((path, writer)) = self.writer.take() else {
            return Ok(());
        };
        writer.close()?;
        self.bytes += std::fs::metadata(&path)?.len();
        self.files += 1;
        Ok(())
    }
}
