use std::{collections::BTreeMap, sync::Arc, time::Duration};

use arrow::array::ArrayRef;
use common_error::DaftResult;
use common_runtime::{OrderedJoinSet, get_io_runtime};
use daft_core::prelude::*;
#[cfg(feature = "python")]
use daft_core::python::PyTimeUnit;
use daft_dsl::ExprRef;
use daft_io::{IOClient, IOStatsRef, SourceType, parse_url};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, TryFutureExt, TryStreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};

use crate::{DaftParquetMetadata, infer_schema_from_daft_metadata};

/// How to decode Parquet BYTE_ARRAY columns annotated as strings.
///
/// - `Utf8` (default): arrow-rs decodes as Utf8/LargeUtf8 with UTF-8 validation.
/// - `Raw`: strip the STRING logical type so arrow-rs decodes as Binary (no validation).
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StringEncoding {
    Raw,
    #[default]
    Utf8,
}

impl std::str::FromStr for StringEncoding {
    type Err = common_error::DaftError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "utf-8" => Ok(Self::Utf8),
            "raw" => Ok(Self::Raw),
            other => Err(common_error::DaftError::ValueError(format!(
                "Unrecognized string encoding: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ParquetSchemaInferenceOptions {
    pub coerce_int96_timestamp_unit: TimeUnit,
    pub string_encoding: StringEncoding,
}

impl ParquetSchemaInferenceOptions {
    #[must_use]
    pub fn new(coerce_int96_timestamp_unit: Option<TimeUnit>) -> Self {
        Self {
            coerce_int96_timestamp_unit: coerce_int96_timestamp_unit
                .unwrap_or(TimeUnit::Nanoseconds),
            string_encoding: StringEncoding::Utf8,
        }
    }

    #[cfg(feature = "python")]
    pub fn from_python(
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
        string_encoding: &str,
    ) -> DaftResult<Self> {
        Ok(Self {
            coerce_int96_timestamp_unit: coerce_int96_timestamp_unit
                .map_or(TimeUnit::Nanoseconds, From::from),
            string_encoding: string_encoding.parse()?,
        })
    }
}

impl Default for ParquetSchemaInferenceOptions {
    fn default() -> Self {
        Self {
            coerce_int96_timestamp_unit: TimeUnit::Nanoseconds,
            string_encoding: StringEncoding::Utf8,
        }
    }
}

/// All projection, pushdown, and decode options for reading one parquet file.
#[derive(Default, Clone)]
pub struct ParquetReadOptions {
    pub columns: Option<Vec<String>>,
    pub start_offset: Option<usize>,
    pub num_rows: Option<usize>,
    pub row_groups: Option<Vec<i64>>,
    pub predicate: Option<ExprRef>,
    pub schema_infer: ParquetSchemaInferenceOptions,
    pub field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    pub delete_rows: Option<Vec<i64>>,
    pub batch_size: Option<usize>,
    // TODO(arrow-rs): wire this through to the arrowrs reader to skip redundant footer reads.
    // The arrowrs reader currently reads its own metadata via ArrowReaderMetadata::load(),
    // but callers (e.g. scan_task.rs) already have pre-fetched DaftParquetMetadata from planning.
    pub metadata: Option<Arc<DaftParquetMetadata>>,
}

/// Per-file overrides for [`ParquetBulkReadOptions`].
#[derive(Default, Clone)]
pub struct PerFileOptions {
    pub row_groups: Option<Vec<i64>>,
    pub delete_rows: Option<Vec<i64>>,
    /// See [`ParquetReadOptions::metadata`].
    pub metadata: Option<Arc<DaftParquetMetadata>>,
}

/// Options for bulk reads. Fields without `per_file` apply to every uri;
/// `per_file[i]` overrides for the i-th uri.
#[derive(Default, Clone)]
pub struct ParquetBulkReadOptions {
    pub columns: Option<Vec<String>>,
    pub start_offset: Option<usize>,
    pub num_rows: Option<usize>,
    pub predicate: Option<ExprRef>,
    pub schema_infer: ParquetSchemaInferenceOptions,
    pub field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    pub batch_size: Option<usize>,
    pub num_parallel_tasks: usize,
    /// Per-uri overrides. Must be empty or `len() == uris.len()`.
    pub per_file: Vec<PerFileOptions>,
}

fn make_source<'a>(
    uri: &'a str,
    local_path: &'a mut String,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<crate::reader::ParquetSource<'a>> {
    let (source_type, fixed_uri) = parse_url(uri)?;
    Ok(if matches!(source_type, SourceType::File) {
        *local_path = daft_io::strip_file_uri_to_path(&fixed_uri)
            .unwrap_or(&fixed_uri)
            .to_string();
        crate::reader::ParquetSource::Local { path: local_path }
    } else {
        crate::reader::ParquetSource::Url {
            uri,
            io_client,
            io_stats,
        }
    })
}

fn check_per_file_len(per_file: &[PerFileOptions], uris_len: usize) -> DaftResult<()> {
    if !per_file.is_empty() && per_file.len() != uris_len {
        return Err(common_error::DaftError::ValueError(format!(
            "Mismatch of length of `uris` and `per_file`. {} vs {}",
            uris_len,
            per_file.len()
        )));
    }
    Ok(())
}

fn single_opts_for(opts: &ParquetBulkReadOptions, i: usize) -> ParquetReadOptions {
    let per = opts.per_file.get(i).cloned().unwrap_or_default();
    ParquetReadOptions {
        columns: opts.columns.clone(),
        start_offset: opts.start_offset,
        num_rows: opts.num_rows,
        row_groups: per.row_groups,
        predicate: opts.predicate.clone(),
        schema_infer: opts.schema_infer,
        field_id_mapping: opts.field_id_mapping.clone(),
        delete_rows: per.delete_rows,
        batch_size: opts.batch_size,
        metadata: per.metadata,
    }
}

/// Read a single parquet file as a stream of `RecordBatch`es.
pub async fn read_parquet(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetReadOptions,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let mut local_path = String::new();
    let source = make_source(uri, &mut local_path, io_client, io_stats)?;
    let (_schema, stream) = Box::pin(crate::reader::stream_parquet(source, &opts)).await?;
    Ok(stream)
}

/// Eager variant of `read_parquet`: collects the full stream into one
/// `RecordBatch`, or a schema-bearing empty batch if the stream produced none.
pub async fn read_parquet_into_recordbatch(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetReadOptions,
) -> DaftResult<RecordBatch> {
    let mut local_path = String::new();
    let source = make_source(uri, &mut local_path, io_client, io_stats)?;
    let (schema, stream) = Box::pin(crate::reader::stream_parquet(source, &opts)).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    if batches.is_empty() {
        return Ok(RecordBatch::empty(Some(schema)));
    }
    RecordBatch::concat(&batches)
}

/// Read a single parquet file and convert to pyarrow-friendly `ArrowChunk`s,
/// preserving file-level kv metadata + per-field nullability from the parquet
/// schema (info that the daft `RecordBatch` path drops).
async fn read_parquet_into_arrow(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetReadOptions,
) -> DaftResult<ParquetPyarrowChunk> {
    debug_assert!(
        opts.predicate.is_none() && opts.delete_rows.is_none() && opts.batch_size.is_none(),
        "read_parquet_into_arrow: predicate/delete_rows/batch_size not supported on the pyarrow path"
    );

    // Read data and metadata concurrently. The metadata read is needed to recover
    // schema-level key-value metadata (e.g. custom metadata like {"str": "foo"}) and
    // per-field nullability — info that Daft's `RecordBatch` doesn't carry.
    let data_fut = read_parquet_into_recordbatch(uri, io_client.clone(), io_stats.clone(), opts);
    let metadata_fut =
        crate::metadata::read_parquet_metadata(uri, None, io_client, io_stats, None, None);
    let (rb, parquet_metadata) =
        Box::pin(futures::future::try_join(data_fut, metadata_fut.err_into())).await?;
    let num_rows_read = rb.len();

    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        parquet_metadata.file_metadata().schema_descr(),
        parquet_metadata.file_metadata().key_value_metadata(),
    )
    .ok();
    let schema_metadata = arrow_schema
        .as_ref()
        .map(|s| s.metadata().clone())
        .unwrap_or_default();

    // Convert each Daft Series → FFI-compatible arrays for the pyarrow bridge.
    // Output layout is COLUMN-MAJOR: all_arrays[col_idx] = [chunks_for_that_column].
    // The Python side (recordbatch.py) zips schema fields with this outer list,
    // so each entry must be the list of chunks for one column.
    let mut ffi_fields = Vec::with_capacity(rb.schema.fields().len());
    let mut all_arrays: Vec<ArrowChunk> = Vec::with_capacity(rb.schema.fields().len());
    for (col, daft_field) in rb.columns().iter().zip(rb.schema.fields()) {
        let arrow_array = col.as_materialized_series().to_arrow()?;
        let nullable = arrow_schema
            .as_ref()
            .and_then(|s| s.field_with_name(&daft_field.name).ok())
            .is_none_or(|f| f.is_nullable());
        ffi_fields.push(arrow::datatypes::Field::new(
            daft_field.name.to_string(),
            arrow_array.data_type().clone(),
            nullable,
        ));
        all_arrays.push(vec![arrow_array]);
    }

    let mut ffi_schema = arrow::datatypes::Schema::new(ffi_fields);
    ffi_schema.metadata = schema_metadata;
    Ok((Arc::new(ffi_schema), all_arrays, num_rows_read))
}

pub type ArrowChunk = Vec<ArrayRef>;
pub type ParquetPyarrowChunk = (arrow::datatypes::SchemaRef, Vec<ArrowChunk>, usize);

pub fn read_parquet_into_pyarrow(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetReadOptions,
    file_timeout_ms: Option<i64>,
) -> DaftResult<ParquetPyarrowChunk> {
    get_io_runtime(multithreaded_io).block_on_current_thread(async {
        let fut = Box::pin(read_parquet_into_arrow(uri, io_client, io_stats, opts));
        match file_timeout_ms {
            Some(timeout) => tokio::time::timeout(Duration::from_millis(timeout as u64), fut)
                .await
                .map_err(|_| crate::Error::FileReadTimeout {
                    path: uri.to_string(),
                    duration_ms: timeout,
                })?,
            None => fut.await,
        }
    })
}

pub async fn read_parquet_bulk(
    uris: Vec<String>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<DaftResult<RecordBatch>>> {
    check_per_file_len(&opts.per_file, uris.len())?;

    let num_parallel = opts.num_parallel_tasks.max(1);
    let io_runtime = get_io_runtime(true);
    let task_stream = futures::stream::iter(uris.into_iter().enumerate().map(|(i, uri)| {
        let single_opts = single_opts_for(&opts, i);
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();
        io_runtime.spawn(async move {
            Box::pin(read_parquet_into_recordbatch(
                &uri,
                io_client,
                io_stats,
                single_opts,
            ))
            .await
        })
    }));

    let mut remaining_rows = opts.num_rows.map(|x| x as i64);
    let tables = task_stream
        .buffered(num_parallel)
        .try_take_while(|result| match (result, remaining_rows) {
            (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
            (Ok(table), Some(rows_left)) => {
                remaining_rows = Some(rows_left - table.len() as i64);
                futures::future::ready(Ok(true))
            }
            (_, None) | (Err(_), _) => futures::future::ready(Ok(true)),
        })
        .try_collect::<Vec<_>>()
        .await?;
    Ok(tables)
}

pub fn read_parquet_bulk_sync(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<RecordBatch>> {
    let uris_owned: Vec<String> = uris.iter().map(|s| (*s).to_string()).collect();
    get_io_runtime(multithreaded_io)
        .block_on_current_thread(read_parquet_bulk(uris_owned, io_client, io_stats, opts))?
        .into_iter()
        .collect()
}

pub fn read_parquet_into_pyarrow_bulk(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<ParquetPyarrowChunk>> {
    check_per_file_len(&opts.per_file, uris.len())?;
    let num_parallel = opts.num_parallel_tasks.max(1);
    let io_runtime = get_io_runtime(multithreaded_io);
    let spawn_runtime = io_runtime.clone();
    let results = io_runtime.block_on_current_thread(async move {
        futures::stream::iter(uris.iter().enumerate().map(|(i, uri)| {
            let uri = (*uri).to_string();
            let single_opts = single_opts_for(&opts, i);
            let io_client = io_client.clone();
            let io_stats = io_stats.clone();
            spawn_runtime.spawn(async move {
                Ok((
                    i,
                    Box::pin(read_parquet_into_arrow(
                        &uri,
                        io_client,
                        io_stats,
                        single_opts,
                    ))
                    .await?,
                ))
            })
        }))
        .buffer_unordered(num_parallel)
        .try_collect::<Vec<_>>()
        .await
    })?;
    let mut collected = results.into_iter().collect::<DaftResult<Vec<_>>>()?;
    collected.sort_by_key(|(idx, _)| *idx);
    Ok(collected.into_iter().map(|(_, v)| v).collect())
}

pub async fn read_parquet_schema_and_metadata(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_inference_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<(Schema, DaftParquetMetadata)> {
    let metadata = crate::metadata::read_parquet_metadata(
        uri,
        None,
        io_client,
        io_stats,
        field_id_mapping,
        None,
    )
    .await?;
    let adapter = DaftParquetMetadata::from_arrowrs(metadata);
    let schema = infer_schema_from_daft_metadata(&adapter, schema_inference_options)?;
    Ok((schema, adapter))
}

pub async fn read_parquet_metadata(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<DaftParquetMetadata> {
    let metadata = crate::metadata::read_parquet_metadata(
        uri,
        None,
        io_client,
        io_stats,
        field_id_mapping,
        None,
    )
    .await?;
    Ok(DaftParquetMetadata::from_arrowrs(metadata))
}

pub async fn read_parquet_metadata_bulk(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<Vec<DaftParquetMetadata>> {
    let io_runtime = get_io_runtime(true);
    let mut joinset: OrderedJoinSet<DaftResult<DaftParquetMetadata>> = OrderedJoinSet::new();
    for uri in uris {
        let uri = (*uri).to_string();
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();
        let field_id_mapping = field_id_mapping.clone();
        joinset.spawn_on(
            async move { read_parquet_metadata(&uri, io_client, io_stats, field_id_mapping).await },
            &io_runtime,
        );
    }
    let mut results = Vec::with_capacity(uris.len());
    while let Some(res) = joinset.join_next().await {
        results.push(res??);
    }
    Ok(results)
}

pub fn read_parquet_statistics(
    uris: &Series,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<RecordBatch> {
    if uris.data_type() != &DataType::Utf8 {
        return Err(common_error::DaftError::ValueError(format!(
            "Expected Utf8 Datatype, got {}",
            uris.data_type()
        )));
    }
    let path_array: &Utf8Array = uris.downcast()?;

    type StatsTuple = (Option<usize>, Option<usize>, Option<i32>);
    let runtime = get_io_runtime(true);
    let spawn_runtime = runtime.clone();
    let all = runtime.block_on_current_thread(async move {
        let mut joinset: OrderedJoinSet<DaftResult<StatsTuple>> = OrderedJoinSet::new();
        for uri in path_array {
            let uri = uri.map(std::string::ToString::to_string);
            let io_client = io_client.clone();
            let io_stats = io_stats.clone();
            let field_id_mapping = field_id_mapping.clone();
            joinset.spawn_on(
                async move {
                    match uri {
                        Some(uri) => {
                            let m =
                                read_parquet_metadata(&uri, io_client, io_stats, field_id_mapping)
                                    .await?;
                            Ok((
                                Some(m.num_rows()),
                                Some(m.num_row_groups()),
                                Some(m.version()),
                            ))
                        }
                        None => Ok((None, None, None)),
                    }
                },
                &spawn_runtime,
            );
        }
        let mut out = Vec::with_capacity(uris.len());
        while let Some(res) = joinset.join_next().await {
            out.push(res??);
        }
        DaftResult::Ok(out)
    })?;
    assert_eq!(all.len(), uris.len());

    let rows = UInt64Array::from_iter(
        Field::new("row_count", DataType::UInt64),
        all.iter().map(|v| v.0.map(|v| v as u64)),
    );
    let rgs = UInt64Array::from_iter(
        Field::new("row_group_count", DataType::UInt64),
        all.iter().map(|v| v.1.map(|v| v as u64)),
    );
    let versions = Int32Array::from_iter(
        Field::new("version", DataType::Int32),
        all.iter().map(|v| v.2),
    );

    RecordBatch::from_nonempty_columns(vec![
        uris.clone(),
        rows.into_series(),
        rgs.into_series(),
        versions.into_series(),
    ])
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, path::PathBuf, sync::Arc};

    use arrow::datatypes::DataType;
    use common_error::DaftResult;
    use daft_io::{IOClient, IOConfig};
    use futures::StreamExt;
    use parquet::schema::types::Type as ParquetSchemaType;

    use super::*;

    const PARQUET_FILE: &str = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
    const PARQUET_FILE_LOCAL: &str = "tests/assets/parquet-data/mvp.parquet";

    fn get_local_parquet_path() -> String {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("../../"); // CARGO_MANIFEST_DIR is at src/daft-parquet
        d.push(PARQUET_FILE_LOCAL);
        d.to_str().unwrap().to_string()
    }

    #[test]
    fn test_parquet_read_from_s3() -> DaftResult<()> {
        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let runtime = get_io_runtime(true);
        let table = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            PARQUET_FILE,
            io_client,
            None,
            ParquetReadOptions::default(),
        ))?;
        assert_eq!(table.len(), 100);
        Ok(())
    }

    #[test]
    fn test_parquet_streaming_read_from_s3() -> DaftResult<()> {
        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let runtime = get_io_runtime(true);
        runtime.block_on_current_thread(async move {
            let stream =
                read_parquet(PARQUET_FILE, io_client, None, ParquetReadOptions::default()).await?;
            let tables = stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<DaftResult<Vec<_>>>()?;
            let total = tables.iter().map(|t| t.len()).sum::<usize>();
            assert_eq!(total, 100);
            Ok(())
        })
    }

    #[test]
    fn test_file_metadata_serialize_roundtrip() -> DaftResult<()> {
        let file = get_local_parquet_path();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into())?);
        let runtime = get_io_runtime(true);

        runtime.block_within_async_context(async move {
            let metadata = read_parquet_metadata(&file, io_client, None, None).await?;
            let config = bincode::config::legacy();
            let serialized = bincode::serde::encode_to_vec(&metadata, config).unwrap();
            let deserialized: DaftParquetMetadata =
                bincode::serde::decode_from_slice(&serialized, config)
                    .unwrap()
                    .0;
            assert_eq!(metadata, deserialized);
            Ok(())
        })?
    }

    #[test]
    fn test_invalid_utf8_parquet_reading() {
        let parquet: Arc<str> = path_macro::path!(
            env!("CARGO_MANIFEST_DIR")
                / ".."
                / ".."
                / "tests"
                / "assets"
                / "parquet-data"
                / "invalid_utf8.parquet"
        )
        .to_str()
        .unwrap()
        .into();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);
        let file_metadata = runtime
            .block_within_async_context({
                let parquet = parquet.clone();
                let io_client = io_client.clone();
                async move { read_parquet_metadata(&parquet, io_client, None, None).await }
            })
            .flatten()
            .unwrap();
        let schema_descr = file_metadata.schema_descriptor();
        let fields = schema_descr.root_schema().get_fields();
        assert_eq!(fields.len(), 1);
        match fields[0].as_ref() {
            ParquetSchemaType::PrimitiveType { basic_info, .. } => {
                assert_eq!(
                    basic_info.logical_type_ref(),
                    Some(&parquet::basic::LogicalType::String),
                );
                assert_eq!(
                    basic_info.converted_type(),
                    parquet::basic::ConvertedType::UTF8,
                );
            }
            ParquetSchemaType::GroupType { .. } => panic!("primitive expected"),
        }
        let opts = ParquetReadOptions {
            schema_infer: ParquetSchemaInferenceOptions {
                string_encoding: StringEncoding::Raw,
                ..Default::default()
            },
            ..Default::default()
        };
        let (schema, _, _) =
            read_parquet_into_pyarrow(&parquet, io_client, None, true, opts, None).unwrap();
        match schema.fields().deref() {
            [field] => assert_eq!(field.data_type(), &DataType::LargeBinary),
            _ => panic!("one field expected"),
        }
    }

    /// Regression test: streaming with a limit equal to the batch size should
    /// return all requested rows, not an empty stream.
    #[test]
    fn test_stream_limit_exact_batch_size() {
        use arrow::{
            array::Int32Array,
            datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        };
        use parquet::arrow::ArrowWriter;

        let dir = std::env::temp_dir().join("daft_test_stream_limit_exact");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        // Write a parquet file with exactly 5 rows.
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap().to_string();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);

        // Stream with num_rows=5 (exactly the file size).
        let total_rows: usize = runtime
            .block_within_async_context(async move {
                let opts = ParquetReadOptions {
                    num_rows: Some(5),
                    ..Default::default()
                };
                let mut stream = read_parquet(&uri, io_client, None, opts).await.unwrap();
                let mut count = 0;
                while let Some(batch) = stream.next().await {
                    count += batch.unwrap().len();
                }
                count
            })
            .unwrap();

        assert_eq!(
            total_rows, 5,
            "stream with limit=5 on 5-row file should return 5 rows"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Verifies pred_mask optimization correctness across multiple selectivities.
    #[test]
    fn test_pred_mask_correctness() {
        use arrow::{
            array::Int64Array,
            datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        };
        use daft_dsl::resolved_col;
        use parquet::arrow::ArrowWriter;

        let dir = std::env::temp_dir().join("daft_test_pred_mask_correctness");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        // 100 rows: id = 0..100, data = id * 7
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("data", ArrowDataType::Int64, false),
        ]));
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let ids: Vec<i64> = (0..100).collect();
        let data: Vec<i64> = ids.iter().map(|&i| i * 7).collect();
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(ids)), Arc::new(Int64Array::from(data))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap().to_string();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);

        // Case 1: id < 10 → 10 rows
        let result = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri, io_client.clone(), None,
            ParquetReadOptions {
                predicate: Some(resolved_col("id").lt(daft_dsl::lit(10i64))),
                ..Default::default()
            },
        )).unwrap();
        assert_eq!(result.len(), 10, "id<10 should return 10 rows");

        // Case 2: id >= 90 → 10 rows
        let result = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri, io_client.clone(), None,
            ParquetReadOptions {
                predicate: Some(resolved_col("id").gt_eq(daft_dsl::lit(90i64))),
                ..Default::default()
            },
        )).unwrap();
        assert_eq!(result.len(), 10, "id>=90 should return 10 rows");

        // Case 3: id < 1 → 1 row (very low selectivity)
        let result = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri, io_client.clone(), None,
            ParquetReadOptions {
                predicate: Some(resolved_col("id").lt(daft_dsl::lit(1i64))),
                ..Default::default()
            },
        )).unwrap();
        assert_eq!(result.len(), 1, "id<1 should return 1 row");

        // Case 4: id < 0 → 0 rows
        let result = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri, io_client.clone(), None,
            ParquetReadOptions {
                predicate: Some(resolved_col("id").lt(daft_dsl::lit(0i64))),
                ..Default::default()
            },
        )).unwrap();
        assert_eq!(result.len(), 0, "id<0 should return 0 rows");

        // Case 5: predicate + limit
        let result = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri, io_client.clone(), None,
            ParquetReadOptions {
                predicate: Some(resolved_col("id").lt(daft_dsl::lit(50i64))),
                num_rows: Some(5),
                ..Default::default()
            },
        )).unwrap();
        assert_eq!(result.len(), 5, "id<50 with limit=5 should return 5 rows");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Test with shuffled data across multiple row groups (simulates TPC-H Q6 scenario).
    #[test]
    fn test_pred_mask_multi_rg_shuffled() {
        use arrow::{
            array::{Float64Array, Int64Array},
            datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        };
        use daft_dsl::resolved_col;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let dir = std::env::temp_dir().join("daft_test_pred_mask_multi_rg");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("year", ArrowDataType::Int64, false),
            ArrowField::new("discount", ArrowDataType::Float64, false),
            ArrowField::new("revenue", ArrowDataType::Int64, false),
        ]));

        // 1000 rows with deterministic shuffle
        let mut years = Vec::with_capacity(1000);
        let mut discounts = Vec::with_capacity(1000);
        let mut revenues = Vec::with_capacity(1000);
        for i in 0..1000i64 {
            years.push(1993 + (i * 7 % 4));
            discounts.push(0.01 + (i * 3 % 10) as f64 * 0.01);
            revenues.push(i * 100);
        }

        // Write with small row groups (100 rows each = 10 RGs)
        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(years.clone())),
                Arc::new(Float64Array::from(discounts.clone())),
                Arc::new(Int64Array::from(revenues.clone())),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap().to_string();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);

        // Multi-column predicate: year=1994 AND discount>0.05
        let predicate = resolved_col("year")
            .eq(daft_dsl::lit(1994i64))
            .and(resolved_col("discount").gt(daft_dsl::lit(0.05f64)));

        let result = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri, io_client.clone(), None,
            ParquetReadOptions {
                predicate: Some(predicate),
                ..Default::default()
            },
        )).unwrap();

        // Compute expected count manually
        let expected_count = (0..1000i64)
            .filter(|&i| {
                let year = 1993 + (i * 7 % 4);
                let discount = 0.01 + (i * 3 % 10) as f64 * 0.01;
                year == 1994 && discount > 0.05
            })
            .count();

        assert_eq!(
            result.len(), expected_count,
            "multi-RG shuffled data with AND predicate should return correct row count"
        );

        // Value verification: collect expected rows in order and compare
        let expected_rows: Vec<(i64, f64, i64)> = (0..1000i64)
            .filter_map(|i| {
                let year = 1993 + (i * 7 % 4);
                let discount = 0.01 + (i * 3 % 10) as f64 * 0.01;
                let revenue = i * 100;
                if year == 1994 && discount > 0.05 {
                    Some((year, discount, revenue))
                } else {
                    None
                }
            })
            .collect();

        let year_idx = result.schema.get_index("year").unwrap();
        let discount_idx = result.schema.get_index("discount").unwrap();
        let revenue_idx = result.schema.get_index("revenue").unwrap();

        let year_arr = result.get_column(year_idx).i64().unwrap();
        let discount_arr = result.get_column(discount_idx).f64().unwrap();
        let revenue_arr = result.get_column(revenue_idx).i64().unwrap();

        for (row_idx, (exp_year, exp_discount, exp_revenue)) in expected_rows.iter().enumerate() {
            let actual_year = year_arr.get(row_idx).unwrap();
            let actual_discount = discount_arr.get(row_idx).unwrap();
            let actual_revenue = revenue_arr.get(row_idx).unwrap();
            assert_eq!(
                actual_year, *exp_year,
                "row {row_idx}: year mismatch: got {actual_year}, expected {exp_year}"
            );
            assert!(
                (actual_discount - exp_discount).abs() < 1e-10,
                "row {row_idx}: discount mismatch: got {actual_discount}, expected {exp_discount}"
            );
            assert_eq!(
                actual_revenue, *exp_revenue,
                "row {row_idx}: revenue mismatch: got {actual_revenue}, expected {exp_revenue}"
            );
        }

        // Also test with limit
        if expected_count > 3 {
            let predicate2 = resolved_col("year")
                .eq(daft_dsl::lit(1994i64))
                .and(resolved_col("discount").gt(daft_dsl::lit(0.05f64)));
            let result_limited = runtime.block_on_current_thread(read_parquet_into_recordbatch(
                &uri, io_client.clone(), None,
                ParquetReadOptions {
                    predicate: Some(predicate2),
                    num_rows: Some(3),
                    ..Default::default()
                },
            )).unwrap();
            assert_eq!(result_limited.len(), 3, "limit=3 should return exactly 3 rows");

            // Value verification for limited result: first 3 rows should match
            let year_arr_lim = result_limited.get_column(year_idx).i64().unwrap();
            let discount_arr_lim = result_limited.get_column(discount_idx).f64().unwrap();
            let revenue_arr_lim = result_limited.get_column(revenue_idx).i64().unwrap();

            for (row_idx, (exp_year, exp_discount, exp_revenue)) in expected_rows.iter().take(3).enumerate() {
                let actual_year = year_arr_lim.get(row_idx).unwrap();
                let actual_discount = discount_arr_lim.get(row_idx).unwrap();
                let actual_revenue = revenue_arr_lim.get(row_idx).unwrap();
                assert_eq!(
                    actual_year, *exp_year,
                    "limited row {row_idx}: year mismatch: got {actual_year}, expected {exp_year}"
                );
                assert!(
                    (actual_discount - exp_discount).abs() < 1e-10,
                    "limited row {row_idx}: discount mismatch: got {actual_discount}, expected {exp_discount}"
                );
                assert_eq!(
                    actual_revenue, *exp_revenue,
                    "limited row {row_idx}: revenue mismatch: got {actual_revenue}, expected {exp_revenue}"
                );
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// End-to-end scan test: streaming read_parquet (the entry point used by
    /// ScanTaskSource) with fully shuffled data across multiple row groups.
    /// Verifies row-level correctness of predicate filtering + projection.
    #[test]
    fn test_scan_e2e_shuffled_multi_rg() {
        use arrow::{
            array::{Float64Array, Int32Array, Int64Array, StringArray},
            datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        };
        use daft_dsl::resolved_col;
        use futures::StreamExt;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let dir = std::env::temp_dir().join("daft_test_scan_e2e_shuffled");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("shuffled.parquet");

        // Schema: id(i64), category(utf8), value(f64), quantity(i32)
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("category", ArrowDataType::Utf8, false),
            ArrowField::new("value", ArrowDataType::Float64, false),
            ArrowField::new("quantity", ArrowDataType::Int32, false),
        ]));

        // Generate 500 rows, then shuffle with a deterministic LCG permutation.
        // LCG: next = (a * cur + c) mod m, with m=512 (next power of 2 >= 500).
        // We pick a=253, c=1 which gives a full period of 512, then filter to [0,500).
        let n = 500usize;
        let m = 512u64;
        let a = 253u64;
        let c = 1u64;
        let mut perm = Vec::with_capacity(n);
        let mut x = 0u64;
        while perm.len() < n {
            x = (a.wrapping_mul(x).wrapping_add(c)) % m;
            if (x as usize) < n {
                perm.push(x as usize);
            }
        }
        // perm is now a permutation of 0..500

        let categories = ["electronics", "clothing", "food", "books", "toys"];

        // Build columns in shuffled order
        let ids: Vec<i64> = perm.iter().map(|&i| i as i64).collect();
        let cats: Vec<&str> = perm.iter().map(|&i| categories[i % 5]).collect();
        let values: Vec<f64> = perm.iter().map(|&i| (i as f64) * 1.5 + 0.1).collect();
        let quantities: Vec<i32> = perm.iter().map(|&i| ((i * 7 + 3) % 100) as i32).collect();

        // Write with 50 rows per RG → 10 RGs, data is shuffled across all of them
        let props = WriterProperties::builder()
            .set_max_row_group_size(50)
            .build();
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(cats.clone())),
                Arc::new(Float64Array::from(values.clone())),
                Arc::new(Int32Array::from(quantities.clone())),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap().to_string();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);

        // === Test 1: predicate only (no projection limit) ===
        // Filter: category == "electronics" AND quantity > 50
        let predicate = resolved_col("category")
            .eq(daft_dsl::lit("electronics"))
            .and(resolved_col("quantity").gt(daft_dsl::lit(50i32)));

        let result = runtime.block_on_current_thread(async {
            let stream = read_parquet(
                &uri,
                io_client.clone(),
                None,
                ParquetReadOptions {
                    predicate: Some(predicate),
                    ..Default::default()
                },
            )
            .await?;
            let batches: Vec<RecordBatch> = stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<DaftResult<Vec<_>>>()?;
            if batches.is_empty() {
                return Ok(RecordBatch::empty(None));
            }
            RecordBatch::concat(&batches)
        }).unwrap();

        // Compute expected rows (in file physical order = shuffled order)
        let expected: Vec<(i64, &str, f64, i32)> = perm
            .iter()
            .map(|&i| {
                (
                    i as i64,
                    categories[i % 5],
                    (i as f64) * 1.5 + 0.1,
                    ((i * 7 + 3) % 100) as i32,
                )
            })
            .filter(|(_, cat, _, qty)| *cat == "electronics" && *qty > 50)
            .collect();

        assert_eq!(
            result.len(),
            expected.len(),
            "predicate filter row count mismatch: got {}, expected {}",
            result.len(),
            expected.len()
        );

        // Value verification
        let id_idx = result.schema.get_index("id").unwrap();
        let val_idx = result.schema.get_index("value").unwrap();
        let qty_idx = result.schema.get_index("quantity").unwrap();

        let id_arr = result.get_column(id_idx).i64().unwrap();
        let val_arr = result.get_column(val_idx).f64().unwrap();
        let qty_arr = result.get_column(qty_idx).i32().unwrap();

        for (row, (exp_id, _, exp_val, exp_qty)) in expected.iter().enumerate() {
            assert_eq!(
                id_arr.get(row).unwrap(),
                *exp_id,
                "row {row}: id mismatch"
            );
            assert!(
                (val_arr.get(row).unwrap() - exp_val).abs() < 1e-10,
                "row {row}: value mismatch"
            );
            assert_eq!(
                qty_arr.get(row).unwrap(),
                *exp_qty,
                "row {row}: quantity mismatch"
            );
        }

        // === Test 2: predicate + column projection ===
        let predicate2 = resolved_col("id").lt(daft_dsl::lit(100i64));
        let result2 = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri,
            io_client.clone(),
            None,
            ParquetReadOptions {
                columns: Some(vec!["id".to_string(), "value".to_string()]),
                predicate: Some(predicate2),
                ..Default::default()
            },
        )).unwrap();

        let expected2: Vec<(i64, f64)> = perm
            .iter()
            .map(|&i| (i as i64, (i as f64) * 1.5 + 0.1))
            .filter(|(id, _)| *id < 100)
            .collect();

        assert_eq!(
            result2.len(),
            expected2.len(),
            "projection+predicate row count mismatch"
        );
        assert_eq!(
            result2.schema.fields().len(),
            2,
            "should only have 2 projected columns"
        );

        let id_idx2 = result2.schema.get_index("id").unwrap();
        let val_idx2 = result2.schema.get_index("value").unwrap();
        let id_arr2 = result2.get_column(id_idx2).i64().unwrap();
        let val_arr2: &DataArray<Float64Type> = result2.get_column(val_idx2).f64().unwrap();

        for (row, (exp_id, exp_val)) in expected2.iter().enumerate() {
            assert_eq!(id_arr2.get(row).unwrap(), *exp_id, "test2 row {row}: id mismatch");
            assert!(
                (val_arr2.get(row).unwrap() - exp_val).abs() < 1e-10,
                "test2 row {row}: value mismatch"
            );
        }

        // === Test 3: predicate + limit (streaming early-stop) ===
        let predicate3 = resolved_col("category")
            .eq(daft_dsl::lit("food"))
            .and(resolved_col("value").gt(daft_dsl::lit(100.0f64)));

        let expected3: Vec<(i64, &str, f64, i32)> = perm
            .iter()
            .map(|&i| {
                (
                    i as i64,
                    categories[i % 5],
                    (i as f64) * 1.5 + 0.1,
                    ((i * 7 + 3) % 100) as i32,
                )
            })
            .filter(|(_, cat, val, _)| *cat == "food" && *val > 100.0)
            .collect();

        if expected3.len() >= 5 {
            let result3 = runtime.block_on_current_thread(async {
                let stream = read_parquet(
                    &uri,
                    io_client.clone(),
                    None,
                    ParquetReadOptions {
                        predicate: Some(predicate3),
                        num_rows: Some(5),
                        ..Default::default()
                    },
                )
                .await?;
                let batches: Vec<RecordBatch> = stream
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<DaftResult<Vec<_>>>()?;
                RecordBatch::concat(&batches)
            }).unwrap();

            assert_eq!(result3.len(), 5, "limit=5 should return exactly 5 rows");

            let id_idx3 = result3.schema.get_index("id").unwrap();
            let id_arr3 = result3.get_column(id_idx3).i64().unwrap();
            for (row, (exp_id, _, _, _)) in expected3.iter().take(5).enumerate() {
                assert_eq!(
                    id_arr3.get(row).unwrap(),
                    *exp_id,
                    "test3 row {row}: id mismatch with limit"
                );
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Verifies that the pred_mask optimization produces the same result as the
    /// "old" approach: full decode (no predicate pushdown) followed by manual
    /// filtering. This catches bugs where the mask or RowSelection is misaligned
    /// with data columns across row group boundaries.
    #[test]
    fn test_pred_mask_matches_full_decode_filter() {
        use arrow::{
            array::{Float64Array, Int64Array},
            datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        };
        use daft_dsl::resolved_col;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let dir = std::env::temp_dir().join("daft_test_pred_mask_vs_full_decode");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("year", ArrowDataType::Int64, false),
            ArrowField::new("discount", ArrowDataType::Float64, false),
            ArrowField::new("revenue", ArrowDataType::Int64, false),
        ]));

        // 600 rows with LCG-shuffled order. LCG: next = (a*x+c) mod m
        // m=1024, a=517, c=1 → full period; take first 600 values < 600.
        let n = 600usize;
        let m = 1024u64;
        let a = 517u64;
        let c = 1u64;
        let mut perm = Vec::with_capacity(n);
        let mut x = 0u64;
        while perm.len() < n {
            x = (a.wrapping_mul(x).wrapping_add(c)) % m;
            if (x as usize) < n {
                perm.push(x as usize);
            }
        }

        let ids: Vec<i64> = perm.iter().map(|&i| i as i64).collect();
        let years: Vec<i64> = perm.iter().map(|&i| 1993 + (i as i64 % 4)).collect();
        let discounts: Vec<f64> = perm.iter().map(|&i| 0.01 + (i % 10) as f64 * 0.01).collect();
        let revenues: Vec<i64> = perm.iter().map(|&i| (i as i64) * 100).collect();

        // Write with 75 rows per RG → 8 RGs
        let props = WriterProperties::builder()
            .set_max_row_group_size(75)
            .build();
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(years.clone())),
                Arc::new(Float64Array::from(discounts.clone())),
                Arc::new(Int64Array::from(revenues.clone())),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap().to_string();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);

        // Predicate: year == 1994 AND discount > 0.05
        let predicate = resolved_col("year")
            .eq(daft_dsl::lit(1994i64))
            .and(resolved_col("discount").gt(daft_dsl::lit(0.05f64)));

        // Path A: optimized (pred_mask). This is what the current code does.
        let optimized = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri,
            io_client.clone(),
            None,
            ParquetReadOptions {
                predicate: Some(predicate),
                ..Default::default()
            },
        )).unwrap();

        // Path B: full decode (no predicate) + manual filter in Rust.
        let full = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri,
            io_client.clone(),
            None,
            ParquetReadOptions::default(),
        )).unwrap();

        // Manually filter `full` to get reference result.
        let full_year_idx = full.schema.get_index("year").unwrap();
        let full_disc_idx = full.schema.get_index("discount").unwrap();
        let full_year = full.get_column(full_year_idx).i64().unwrap();
        let full_disc = full.get_column(full_disc_idx).f64().unwrap();

        let keep: Vec<usize> = (0..full.len())
            .filter(|&row| {
                full_year.get(row) == Some(1994) && full_disc.get(row).is_some_and(|d| d > 0.05)
            })
            .collect();

        // Compare row counts
        assert_eq!(
            optimized.len(),
            keep.len(),
            "optimized ({}) vs reference ({}) row count mismatch",
            optimized.len(),
            keep.len()
        );

        // Compare every value in every column
        let opt_id_idx = optimized.schema.get_index("id").unwrap();
        let opt_year_idx = optimized.schema.get_index("year").unwrap();
        let opt_disc_idx = optimized.schema.get_index("discount").unwrap();
        let opt_rev_idx = optimized.schema.get_index("revenue").unwrap();

        let opt_id = optimized.get_column(opt_id_idx).i64().unwrap();
        let opt_year = optimized.get_column(opt_year_idx).i64().unwrap();
        let opt_disc = optimized.get_column(opt_disc_idx).f64().unwrap();
        let opt_rev = optimized.get_column(opt_rev_idx).i64().unwrap();

        let full_id_idx = full.schema.get_index("id").unwrap();
        let full_rev_idx = full.schema.get_index("revenue").unwrap();
        let full_id = full.get_column(full_id_idx).i64().unwrap();
        let full_rev = full.get_column(full_rev_idx).i64().unwrap();

        for (out_row, &src_row) in keep.iter().enumerate() {
            assert_eq!(
                opt_id.get(out_row).unwrap(),
                full_id.get(src_row).unwrap(),
                "row {out_row}: id mismatch (src_row={src_row})"
            );
            assert_eq!(
                opt_year.get(out_row).unwrap(),
                full_year.get(src_row).unwrap(),
                "row {out_row}: year mismatch"
            );
            assert!(
                (opt_disc.get(out_row).unwrap() - full_disc.get(src_row).unwrap()).abs() < 1e-10,
                "row {out_row}: discount mismatch"
            );
            assert_eq!(
                opt_rev.get(out_row).unwrap(),
                full_rev.get(src_row).unwrap(),
                "row {out_row}: revenue mismatch"
            );
        }

        // Also test with limit: optimized(limit=10) should match reference first 10
        let predicate_lim = resolved_col("year")
            .eq(daft_dsl::lit(1994i64))
            .and(resolved_col("discount").gt(daft_dsl::lit(0.05f64)));
        let optimized_lim = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            &uri,
            io_client.clone(),
            None,
            ParquetReadOptions {
                predicate: Some(predicate_lim),
                num_rows: Some(10),
                ..Default::default()
            },
        )).unwrap();

        let lim = 10.min(keep.len());
        assert_eq!(optimized_lim.len(), lim, "limit row count mismatch");

        let lim_id = optimized_lim.get_column(opt_id_idx).i64().unwrap();
        let lim_rev = optimized_lim.get_column(opt_rev_idx).i64().unwrap();
        for (out_row, &src_row) in keep.iter().take(lim).enumerate() {
            assert_eq!(
                lim_id.get(out_row).unwrap(),
                full_id.get(src_row).unwrap(),
                "limited row {out_row}: id mismatch"
            );
            assert_eq!(
                lim_rev.get(out_row).unwrap(),
                full_rev.get(src_row).unwrap(),
                "limited row {out_row}: revenue mismatch"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
