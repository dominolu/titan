//! Bounded, exact Parquet adapters. Publish after a valid footer and fsync.
use crate::{FactEnvelope, Result, failure};
use parquet::{
    data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type},
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
        writer::SerializedFileWriter,
    },
    record::RowAccessor,
    schema::parser::parse_message_type,
};
use std::{fs::File, path::Path, sync::Arc};
const FACT_DDL: &str = "message titan_facts_v1 { REQUIRED INT64 sequence (UINT_64); REQUIRED INT64 exchange_at_ns; REQUIRED BINARY kind (UTF8); REQUIRED BINARY envelope (UTF8); }";
const MARKET_DDL: &str = "message titan_market_v1 { REQUIRED INT64 ev (UINT_64); REQUIRED INT64 exch_ts; REQUIRED INT64 local_ts; REQUIRED DOUBLE px; REQUIRED DOUBLE qty; REQUIRED INT64 order_id (UINT_64); REQUIRED INT64 ival; REQUIRED DOUBLE fval; }";
const TAPE_DDL: &str = "message titan_integer_tape_v1 { REQUIRED INT64 ts; REQUIRED INT64 kind; REQUIRED INT64 a; REQUIRED INT64 b; REQUIRED INT64 c; REQUIRED INT64 d; }";
pub struct FactParquetWriter {
    writer: Option<SerializedFileWriter<File>>,
    temporary: tempfile::NamedTempFile,
    destination: std::path::PathBuf,
}
impl FactParquetWriter {
    pub fn create(path: &Path) -> Result<Self> {
        if path.exists() {
            return Err(failure("parquet_destination_exists"));
        }
        let parent = path
            .parent()
            .ok_or_else(|| failure("parquet_path_invalid"))?;
        std::fs::create_dir_all(parent)?;
        let temporary = tempfile::NamedTempFile::new_in(parent)?;
        let writer = SerializedFileWriter::new(
            temporary.reopen()?,
            Arc::new(parse_message_type(FACT_DDL)?),
            Arc::new(WriterProperties::default()),
        )?;
        Ok(Self {
            writer: Some(writer),
            temporary,
            destination: path.into(),
        })
    }
    pub fn write_chunk(&mut self, records: &[FactEnvelope]) -> Result<()> {
        if records.len() > crate::MAX_BATCH {
            return Err(failure("parquet_chunk_too_large"));
        }
        if records.is_empty() {
            return Ok(());
        }
        let mut json = Vec::with_capacity(records.len());
        for r in records {
            r.validate()?;
            let bytes = serde_json::to_vec(r)?;
            if bytes.len() > crate::MAX_FACT_BYTES {
                return Err(failure("fact_payload_too_large"));
            }
            json.push(ByteArray::from(bytes));
        }
        let mut group = self
            .writer
            .as_mut()
            .ok_or_else(|| failure("parquet_closed"))?
            .next_row_group()?;
        let mut c = group
            .next_column()?
            .ok_or_else(|| failure("parquet_schema_invalid"))?;
        c.typed::<Int64Type>().write_batch(
            &records
                .iter()
                .map(|r| r.source_sequence as i64)
                .collect::<Vec<_>>(),
            None,
            None,
        )?;
        c.close()?;
        let mut c = group
            .next_column()?
            .ok_or_else(|| failure("parquet_schema_invalid"))?;
        c.typed::<Int64Type>().write_batch(
            &records.iter().map(|r| r.exchange_at_ns).collect::<Vec<_>>(),
            None,
            None,
        )?;
        c.close()?;
        let mut c = group
            .next_column()?
            .ok_or_else(|| failure("parquet_schema_invalid"))?;
        c.typed::<ByteArrayType>().write_batch(
            &records
                .iter()
                .map(|r| ByteArray::from(r.kind.as_str()))
                .collect::<Vec<_>>(),
            None,
            None,
        )?;
        c.close()?;
        let mut c = group
            .next_column()?
            .ok_or_else(|| failure("parquet_schema_invalid"))?;
        c.typed::<ByteArrayType>().write_batch(&json, None, None)?;
        c.close()?;
        group.close()?;
        Ok(())
    }
    pub fn finish(mut self) -> Result<()> {
        self.writer
            .take()
            .ok_or_else(|| failure("parquet_closed"))?
            .close()?;
        self.temporary.as_file().sync_all()?;
        self.temporary
            .persist_noclobber(&self.destination)
            .map_err(|e| e.error)?;
        File::open(self.destination.parent().unwrap())?.sync_all()?;
        Ok(())
    }
}
pub fn scan_facts(path: &Path, mut visit: impl FnMut(FactEnvelope) -> Result<()>) -> Result<u64> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    if reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        != &parse_message_type(FACT_DDL)?
    {
        return Err(failure("parquet_schema_unsupported"));
    }
    let mut count = 0;
    for row in reader.get_row_iter(None)? {
        let row = row?;
        let text = row.get_string(3)?;
        if text.len() > crate::MAX_FACT_BYTES {
            return Err(failure("fact_payload_too_large"));
        }
        let fact: FactEnvelope = serde_json::from_str(text)?;
        fact.validate()?;
        if row.get_ulong(0)? != fact.source_sequence
            || row.get_long(1)? != fact.exchange_at_ns
            || *row.get_string(2)? != fact.kind
        {
            return Err(failure("parquet_metadata_mismatch"));
        }
        visit(fact)?;
        count += 1;
    }
    Ok(count)
}
pub fn write_market(path: &Path, rows: &[titan_domain_types::Event]) -> Result<()> {
    if path.exists() {
        return Err(failure("parquet_destination_exists"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| failure("parquet_path_invalid"))?;
    std::fs::create_dir_all(parent)?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut writer = SerializedFileWriter::new(
        temporary.reopen()?,
        Arc::new(parse_message_type(MARKET_DDL)?),
        Arc::new(WriterProperties::default()),
    )?;
    for chunk in rows.chunks(4096) {
        let mut group = writer.next_row_group()?;
        macro_rules! column {
            ($t:ty,$field:ident,$cast:ty) => {{
                let values = chunk.iter().map(|r| r.$field as $cast).collect::<Vec<_>>();
                let mut c = group
                    .next_column()?
                    .ok_or_else(|| failure("parquet_schema_invalid"))?;
                c.typed::<$t>().write_batch(&values, None, None)?;
                c.close()?;
            }};
        }
        column!(Int64Type, ev, i64);
        column!(Int64Type, exch_ts, i64);
        column!(Int64Type, local_ts, i64);
        column!(DoubleType, px, f64);
        column!(DoubleType, qty, f64);
        column!(Int64Type, order_id, i64);
        column!(Int64Type, ival, i64);
        column!(DoubleType, fval, f64);
        group.close()?;
    }
    writer.close()?;
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(path).map_err(|e| e.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
pub fn scan_market(
    path: &Path,
    mut visit: impl FnMut(titan_domain_types::Event) -> Result<()>,
) -> Result<u64> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    if reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        != &parse_message_type(MARKET_DDL)?
    {
        return Err(failure("parquet_schema_unsupported"));
    }
    let mut count = 0;
    for row in reader.get_row_iter(None)? {
        let r = row?;
        let event = titan_domain_types::Event {
            ev: r.get_ulong(0)?,
            exch_ts: r.get_long(1)?,
            local_ts: r.get_long(2)?,
            px: r.get_double(3)?,
            qty: r.get_double(4)?,
            order_id: r.get_ulong(5)?,
            ival: r.get_long(6)?,
            fval: r.get_double(7)?,
        };
        if !event.px.is_finite() || !event.qty.is_finite() || !event.fval.is_finite() {
            return Err(failure("market_non_finite"));
        }
        visit(event)?;
        count += 1;
    }
    Ok(count)
}
/// Exact integer tape, with units/quality frozen by the dataset manifest.
pub fn write_tape(path: &Path, mut rows: impl Iterator<Item = [i64; 6]>) -> Result<()> {
    if path.exists() {
        return Err(failure("parquet_destination_exists"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| failure("parquet_path_invalid"))?;
    std::fs::create_dir_all(parent)?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut writer = SerializedFileWriter::new(
        temporary.reopen()?,
        Arc::new(parse_message_type(TAPE_DDL)?),
        Arc::new(WriterProperties::default()),
    )?;
    loop {
        let chunk = rows.by_ref().take(4096).collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        let mut group = writer.next_row_group()?;
        for index in 0..6 {
            let mut column = group
                .next_column()?
                .ok_or_else(|| failure("parquet_schema_invalid"))?;
            column.typed::<Int64Type>().write_batch(
                &chunk.iter().map(|r| r[index]).collect::<Vec<_>>(),
                None,
                None,
            )?;
            column.close()?;
        }
        group.close()?;
    }
    writer.close()?;
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(path).map_err(|e| e.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
pub fn scan_tape(path: &Path, mut visit: impl FnMut([i64; 6]) -> Result<()>) -> Result<u64> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    if reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        != &parse_message_type(TAPE_DDL)?
    {
        return Err(failure("parquet_schema_unsupported"));
    }
    let mut count = 0;
    for row in reader.get_row_iter(None)? {
        let r = row?;
        visit([
            r.get_long(0)?,
            r.get_long(1)?,
            r.get_long(2)?,
            r.get_long(3)?,
            r.get_long(4)?,
            r.get_long(5)?,
        ])?;
        count += 1;
    }
    Ok(count)
}
