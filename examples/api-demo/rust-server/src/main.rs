use axum::{
    Router,
    extract::{DefaultBodyLimit, Multipart, Query},
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use serde::Deserialize;
use tower_http::cors::CorsLayer;

#[derive(Debug)]
struct AppError(readstat::ReadStatError);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.0.to_string()).into_response()
    }
}

impl From<readstat::ReadStatError> for AppError {
    fn from(e: readstat::ReadStatError) -> Self {
        Self(e)
    }
}

async fn extract_file_bytes(mut multipart: Multipart) -> Result<Vec<u8>, AppError> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError(readstat::ReadStatError::Other(e.to_string())))?
    {
        if field.name() == Some("file") {
            let bytes = field
                .bytes()
                .await
                .map_err(|e| AppError(readstat::ReadStatError::Other(e.to_string())))?;
            return Ok(bytes.to_vec());
        }
    }
    Err(AppError(readstat::ReadStatError::Other(
        "No 'file' field in multipart upload".into(),
    )))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn metadata(multipart: Multipart) -> Result<Json<serde_json::Value>, AppError> {
    let bytes = extract_file_bytes(multipart).await?;

    let md = tokio::task::spawn_blocking(move || {
        let mut md = readstat::ReadStatMetadata::new();
        md.read_metadata_from_bytes(&bytes, false)?;
        Ok::<_, readstat::ReadStatError>(md)
    })
    .await
    .unwrap()?;

    Ok(Json(serde_json::to_value(&md).unwrap()))
}

#[derive(Deserialize)]
struct PreviewParams {
    rows: Option<u32>,
}

async fn preview(
    Query(params): Query<PreviewParams>,
    multipart: Multipart,
) -> Result<Response, AppError> {
    let bytes = extract_file_bytes(multipart).await?;
    let max_rows = params.rows.unwrap_or(10);

    let csv_bytes = tokio::task::spawn_blocking(move || {
        let mut md = readstat::ReadStatMetadata::new();
        md.read_metadata_from_bytes(&bytes, false)?;

        let end = (max_rows).min(md.row_count as u32);
        let mut d = readstat::ReadStatData::new()
            .set_no_progress(true)
            .init(md, 0, end);
        d.read_data_from_bytes(&bytes)?;

        let batch = d.batch.as_ref().unwrap();
        readstat::write_batch_to_csv_bytes(batch)
    })
    .await
    .unwrap()?;

    Ok(([(header::CONTENT_TYPE, "text/csv")], csv_bytes).into_response())
}

#[derive(Deserialize)]
struct DataParams {
    format: Option<String>,
}

async fn data(
    Query(params): Query<DataParams>,
    multipart: Multipart,
) -> Result<Response, AppError> {
    let bytes = extract_file_bytes(multipart).await?;
    let format = params.format.unwrap_or_else(|| "csv".into());

    let (output_bytes, content_type, filename) = tokio::task::spawn_blocking(move || {
        let mut md = readstat::ReadStatMetadata::new();
        md.read_metadata_from_bytes(&bytes, false)?;

        let row_count = md.row_count as u32;
        let mut d = readstat::ReadStatData::new()
            .set_no_progress(true)
            .init(md, 0, row_count);
        d.read_data_from_bytes(&bytes)?;

        let batch = d.batch.as_ref().unwrap();
        match format.as_str() {
            "csv" => Ok((
                readstat::write_batch_to_csv_bytes(batch)?,
                "text/csv",
                "data.csv",
            )),
            "ndjson" => Ok((
                readstat::write_batch_to_ndjson_bytes(batch)?,
                "application/x-ndjson",
                "data.ndjson",
            )),
            "parquet" => Ok((
                readstat::write_batch_to_parquet_bytes(batch)?,
                "application/octet-stream",
                "data.parquet",
            )),
            "feather" => Ok((
                readstat::write_batch_to_feather_bytes(batch)?,
                "application/octet-stream",
                "data.feather",
            )),
            _ => Err(readstat::ReadStatError::Other(format!(
                "Unknown format '{}'. Use csv, ndjson, parquet, or feather.",
                format
            ))),
        }
    })
    .await
    .unwrap()?;

    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (
                header::CONTENT_DISPOSITION,
                &format!("attachment; filename=\"{filename}\""),
            ),
        ],
        output_bytes,
    )
        .into_response())
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health))
        .route("/metadata", post(metadata))
        .route("/preview", post(preview))
        .route("/data", post(data))
        .layer(CorsLayer::permissive())
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Rust API server listening on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}
