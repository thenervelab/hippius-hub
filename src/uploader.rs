use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_util::codec::{BytesCodec, FramedRead};

#[derive(Debug)]
pub enum UploadError {
    ReqwestError(reqwest::Error),
    IoError(std::io::Error),
    ServerError(u16, String),
}

impl From<reqwest::Error> for UploadError {
    fn from(err: reqwest::Error) -> Self {
        UploadError::ReqwestError(err)
    }
}

impl From<std::io::Error> for UploadError {
    fn from(err: std::io::Error) -> Self {
        UploadError::IoError(err)
    }
}

/// Calcule le SHA256 et la taille totale d'un fichier local
pub async fn hash_file_async(path: &Path) -> Result<(String, u64), UploadError> {
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024]; // 64 KB chunks
    let mut total_size = 0u64;

    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
        total_size += bytes_read as u64;
    }

    Ok((hex::encode(hasher.finalize()), total_size))
}

/// Upload un fichier en streaming vers l URL OCI retournée par /blobs/uploads/ (PUT digest finalise le blob).
/// Affiche une progress bar par appel — utile pour les gros blobs (multi-GB).
pub async fn upload_blob_async(url: &str, path: &Path, auth_token: Option<&str>) -> Result<(), UploadError> {
    let client = Client::builder()
        .timeout(Duration::from_secs(3600)) // 1h timeout pour les uploads massifs
        .build()?;

    let file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    // Progress bar — la stream wrapper la met à jour à chaque chunk émis vers reqwest.
    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );
    let basename = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "blob".to_string());
    pb.set_message(format!("📤 {}", basename));

    // Wrappe le stream pour incrémenter la progress bar à chaque chunk de body
    // émis vers reqwest. ProgressBar est Arc-internalement → clone bon marché.
    let pb_stream = pb.clone();
    let stream = FramedRead::new(file, BytesCodec::new()).map(move |chunk_result| {
        if let Ok(ref bytes) = chunk_result {
            pb_stream.inc(bytes.len() as u64);
        }
        chunk_result
    });
    let body = reqwest::Body::wrap_stream(stream);

    let mut req = client
        .put(url)
        .header(header::CONTENT_LENGTH, file_size)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body);

    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }

    let res = req.send().await?;

    if !res.status().is_success() {
        pb.finish_with_message(format!("❌ {} failed", basename));
        return Err(UploadError::ServerError(
            res.status().as_u16(),
            format!("Upload failed: {:?}", res.status()),
        ));
    }

    pb.finish_with_message(format!("✅ {} uploaded", basename));
    Ok(())
}
