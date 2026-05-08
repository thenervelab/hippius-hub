use futures::stream::{self, StreamExt};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DEFAULT_CHUNK_SIZE: u64 = 100 * 1024 * 1024; // 100 MB par défaut
const MAX_CONCURRENT_DOWNLOADS: usize = 16;
const MAX_RETRIES: u32 = 3;

#[derive(Debug)]
pub enum DownloadError {
    ReqwestError(reqwest::Error),
    IoError(std::io::Error),
    ServerError(u16, String),
    ChunkFailed(usize),
}

impl From<reqwest::Error> for DownloadError {
    fn from(err: reqwest::Error) -> Self {
        DownloadError::ReqwestError(err)
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(err: std::io::Error) -> Self {
        DownloadError::IoError(err)
    }
}

pub struct ChunkedDownloader {
    client: Client,
    url: String,
    auth_token: Option<String>,
    chunk_size: u64,
}

impl ChunkedDownloader {
    /// Crée une nouvelle instance de téléchargeur concurrent.
    pub fn new(url: String, auth_token: Option<String>, chunk_size_bytes: Option<u64>) -> Result<Self, DownloadError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .build()?;
            
        Ok(Self {
            client,
            url,
            auth_token,
            chunk_size: chunk_size_bytes.unwrap_or(DEFAULT_CHUNK_SIZE),
        })
    }

    /// Télécharge le fichier de manière concurrente en utilisant le Range Header
    /// Assemble ensuite les chunks et retourne le hash SHA256 final.
    pub async fn download(&self, dest_path: &Path) -> Result<String, DownloadError> {
        // 1. Récupération de la taille totale du blob
        let content_length = self.get_content_length().await?;
        
        // Gérer le cas des fichiers vides
        if content_length == 0 {
            return self.create_empty_file(dest_path).await;
        }
        
        let pb = ProgressBar::new(content_length);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .unwrap()
            .progress_chars("#>-"));
            
        let num_chunks = (content_length as f64 / self.chunk_size as f64).ceil() as usize;
        
        // Préparation du dossier d'accueil (habituellement blobs/)
        let parent_dir = dest_path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent_dir).await?;
        
        // Nom de base pour les chunks temporaires
        let base_filename = dest_path.file_name().unwrap_or_default().to_string_lossy();

        // 2. Division en N chunks et exécution concurrente avec StreamExt pour limiter la concurrence
        let mut stream = stream::iter(0..num_chunks).map(|i| {
            let start = i as u64 * self.chunk_size;
            let end = std::cmp::min(start + self.chunk_size - 1, content_length - 1);
            
            let client = self.client.clone();
            let url = self.url.clone();
            let token = self.auth_token.clone();
            let chunk_pb = pb.clone();
            
            let chunk_path = parent_dir.join(format!("{}.part_{}", base_filename, i));
            
            tokio::spawn(async move {
                let res = download_chunk_with_retry(client, url, token, start, end, i, chunk_path, chunk_pb).await;
                (i, res)
            })
        }).buffer_unordered(MAX_CONCURRENT_DOWNLOADS);

        // Attente de l'exécution concurrente contrôlée
        while let Some(res) = stream.next().await {
            let (i, chunk_res) = res.map_err(|_| DownloadError::ChunkFailed(0))?; // 0 is placeholder
            if let Err(e) = chunk_res {
                return Err(DownloadError::ChunkFailed(i));
            }
        }

        // 3. Assemblage des chunks et calcul du SHA256 à la volée
        let hash = self.assemble_chunks(dest_path, num_chunks, parent_dir, &base_filename).await?;
        
        pb.finish_with_message("Download complete");
        Ok(hash)
    }

    /// Exécute une requête HEAD pour obtenir le Content-Length
    async fn get_content_length(&self) -> Result<u64, DownloadError> {
        let mut req = self.client.head(&self.url);
        if let Some(ref token) = self.auth_token {
            req = req.bearer_auth(token);
        }
        
        let res = req.send().await?;
        if !res.status().is_success() {
            return Err(DownloadError::ServerError(res.status().as_u16(), format!("Failed HEAD request: {:?}", res.status())));
        }
        
        let content_length = res.headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|val| val.to_str().ok())
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(0);
            
        Ok(content_length)
    }

    /// Assemble tous les fichiers partiels en un fichier final tout en calculant le SHA256
    async fn assemble_chunks(&self, dest_path: &Path, num_chunks: usize, parent_dir: &Path, base_filename: &str) -> Result<String, DownloadError> {
        let mut final_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest_path)
            .await?;
            
        let mut hasher = Sha256::new();
        // Utilisation d'un gros buffer pour maximiser la vitesse d'I/O disque
        let mut buffer = vec![0u8; 64 * 1024]; // 64 KB
        
        for i in 0..num_chunks {
            let chunk_path = parent_dir.join(format!("{}.part_{}", base_filename, i));
            let mut chunk_file = File::open(&chunk_path).await?;
            
            loop {
                let bytes_read = chunk_file.read(&mut buffer).await?;
                if bytes_read == 0 {
                    break; // Fin du chunk
                }
                
                final_file.write_all(&buffer[..bytes_read]).await?;
                // Mise à jour du hash SHA256 purement en Rust
                hasher.update(&buffer[..bytes_read]);
            }
            
            // Suppression du chunk temporaire pour libérer l'espace disque
            tokio::fs::remove_file(&chunk_path).await?;
        }
        
        final_file.flush().await?;
        
        // Finalisation et conversion en string hexadécimal
        let result = hasher.finalize();
        Ok(hex::encode(result)) // Requiert la crate 'hex'
    }

    /// Cas spécifique pour créer un fichier vide si la taille est de 0
    async fn create_empty_file(&self, dest_path: &Path) -> Result<String, DownloadError> {
        let mut final_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest_path)
            .await?;
        final_file.flush().await?;
        
        let mut hasher = Sha256::new();
        hasher.update(&[]);
        Ok(hex::encode(hasher.finalize()))
    }
}

/// Gère un téléchargement de chunk avec logique de retry (Exponential Backoff)
async fn download_chunk_with_retry(
    client: Client,
    url: String,
    token: Option<String>,
    start: u64,
    end: u64,
    _chunk_index: usize,
    chunk_path: PathBuf,
    pb: ProgressBar,
) -> Result<(), DownloadError> {
    let mut retries = 0;
    
    loop {
        match try_download_chunk(&client, &url, &token, start, end, &chunk_path, &pb).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                retries += 1;
                if retries > MAX_RETRIES {
                    return Err(e);
                }
                // Exponential backoff : 200ms, 400ms, 800ms
                let wait_time = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
            }
        }
    }
}

/// Logique interne du téléchargement d'un chunk
async fn try_download_chunk(
    client: &Client,
    url: &str,
    token: &Option<String>,
    start: u64,
    end: u64,
    chunk_path: &Path,
    pb: &ProgressBar,
) -> Result<(), DownloadError> {
    // Header Range: bytes=start-end pour Apache Traffic Server (Edge Cache)
    let mut req = client.get(url)
        .header(header::RANGE, format!("bytes={}-{}", start, end));
        
    if let Some(ref t) = token {
        req = req.bearer_auth(t);
    }
    
    let mut res = req.send().await?;
    
    // Support des codes 200 (OK complet sans range) et 206 (Partial Content)
    if !res.status().is_success() {
        return Err(DownloadError::ServerError(res.status().as_u16(), format!("Failed chunk bytes {}-{}", start, end)));
    }
    
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(chunk_path)
        .await?;
        
    let mut bytes_downloaded_this_attempt = 0;
    // Stream des données dans le fichier temporaire pour limiter l'utilisation de RAM
    loop {
        match res.chunk().await {
            Ok(Some(chunk)) => {
                file.write_all(&chunk).await?;
                bytes_downloaded_this_attempt += chunk.len() as u64;
                pb.inc(chunk.len() as u64);
            },
            Ok(None) => break,
            Err(e) => {
                pb.set_position(pb.position().saturating_sub(bytes_downloaded_this_attempt));
                return Err(e.into());
            }
        }
    }
    
    file.flush().await?;
    
    Ok(())
}
