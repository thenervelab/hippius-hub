use reqwest::{Client, header};
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

/// Upload un fichier en mode streaming vers une URL OCI (souvent S3 ou Harbor)
pub async fn upload_blob_async(url: &str, path: &Path, auth_token: Option<&str>) -> Result<(), UploadError> {
    let client = Client::builder()
        .timeout(Duration::from_secs(3600)) // 1 hour timeout for massive uploads
        .build()?;

    let file = File::open(path).await?;
    let file_size = file.metadata().await?.len();
    
    // Convertit le fichier tokio asynchrone en un stream pour reqwest
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = reqwest::Body::wrap_stream(stream);

    let mut req = client.put(url)
        .header(header::CONTENT_LENGTH, file_size)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body);

    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }

    let res = req.send().await?;

    if !res.status().is_success() {
        return Err(UploadError::ServerError(res.status().as_u16(), format!("Upload failed: {:?}", res.status())));
    }

    Ok(())
}
