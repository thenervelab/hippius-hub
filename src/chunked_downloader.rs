use futures::stream::{self, StreamExt};
use reqwest::{header, Client};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

const DEFAULT_CHUNK_SIZE: u64 = 100 * 1024 * 1024; // 100 MB par défaut
const MAX_CONCURRENT_DOWNLOADS: usize = 16;
const MAX_RETRIES: u32 = 3;
const VERIFY_READ_BUFFER: usize = 8 * 1024 * 1024; // 8 MB pour lecture de vérification SHA256

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

    /// Télécharge le fichier de manière concurrente en streamant chaque chunk
    /// directement à son offset dans le fichier final (sparse pre-allocated).
    /// Si verify_hash est vrai, lit le fichier complet en fin de course pour
    /// produire le SHA256. Sinon retourne une chaîne vide.
    pub async fn download(&self, dest_path: &Path, verify_hash: bool) -> Result<String, DownloadError> {
        // 1. Récupération de la taille totale du blob
        let content_length = self.get_content_length().await?;

        // Gérer le cas des fichiers vides
        if content_length == 0 {
            return self.create_empty_file(dest_path).await;
        }

        let pb = ProgressBar::new(content_length);
        pb.set_style(ProgressStyle::default_bar()
            .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .unwrap()
            .progress_chars("#>-"));
        pb.set_message("📥 Downloading");

        let num_chunks = (content_length as f64 / self.chunk_size as f64).ceil() as usize;

        // Préparation du dossier d'accueil
        let parent_dir = dest_path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent_dir).await?;

        // 2. Pré-allocation du fichier final à la taille exacte (sparse OK)
        //    Chaque chunk task ouvrira son propre handle et seekera à son offset.
        //    Les writes concurrents avec handles distincts sur des ranges disjointes
        //    sont safe au niveau OS (chaque handle a son propre file pointer).
        {
            let f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(dest_path)
                .await?;
            f.set_len(content_length).await?;
            f.sync_all().await?; // S'assurer que la taille est persistée avant les writes parallèles
        }

        let dest_path_buf = dest_path.to_path_buf();

        // 3. Lancement des téléchargements concurrents — chacun stream directement
        //    au bon offset dans le fichier final.
        let mut stream = stream::iter(0..num_chunks).map(|i| {
            let start = i as u64 * self.chunk_size;
            let end = std::cmp::min(start + self.chunk_size - 1, content_length - 1);

            let client = self.client.clone();
            let url = self.url.clone();
            let token = self.auth_token.clone();
            let chunk_pb = pb.clone();
            let path = dest_path_buf.clone();

            tokio::spawn(async move {
                let res = download_chunk_with_retry(client, url, token, start, end, i, path, chunk_pb).await;
                (i, res)
            })
        }).buffer_unordered(MAX_CONCURRENT_DOWNLOADS);

        while let Some(res) = stream.next().await {
            let (i, chunk_res) = res.map_err(|_| DownloadError::ChunkFailed(0))?;
            if let Err(_e) = chunk_res {
                return Err(DownloadError::ChunkFailed(i));
            }
        }

        pb.finish_with_message("✅ Download complete");

        // 4. SHA256 optionnel — un seul read-pass séquentiel sur le fichier final.
        //    Bien plus rapide que l'ancienne assembly phase (pas de réécriture).
        if verify_hash {
            let pb_hash = ProgressBar::new(content_length);
            pb_hash.set_style(ProgressStyle::default_bar()
                .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.magenta/red}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .unwrap()
                .progress_chars("=>-"));
            pb_hash.set_message("🔐 Verifying SHA256");

            let hash = compute_sha256(dest_path, &pb_hash).await?;
            pb_hash.finish_with_message("✅ Verified");
            Ok(hash)
        } else {
            Ok(String::new())
        }
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

    /// Cas spécifique pour créer un fichier vide si la taille est de 0
    async fn create_empty_file(&self, dest_path: &Path) -> Result<String, DownloadError> {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest_path)
            .await?;
        f.sync_all().await?;
        drop(f);

        let mut hasher = Sha256::new();
        hasher.update(&[]);
        Ok(hex::encode(hasher.finalize()))
    }
}

/// Calcule le SHA256 du fichier final en un seul read-pass séquentiel.
async fn compute_sha256(path: &Path, pb: &ProgressBar) -> Result<String, DownloadError> {
    let mut file = OpenOptions::new().read(true).open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; VERIFY_READ_BUFFER];

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        pb.inc(n as u64);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Wrapper avec retry exponential-backoff pour le download d'un chunk.
async fn download_chunk_with_retry(
    client: Client,
    url: String,
    token: Option<String>,
    start: u64,
    end: u64,
    _chunk_index: usize,
    dest_path: std::path::PathBuf,
    pb: ProgressBar,
) -> Result<(), DownloadError> {
    let mut retries = 0;

    loop {
        match try_download_chunk_to_offset(&client, &url, &token, start, end, &dest_path, &pb).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                retries += 1;
                if retries > MAX_RETRIES {
                    return Err(e);
                }
                let wait_time = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
            }
        }
    }
}

/// Téléchargement d'un chunk en streaming direct vers son offset dans le fichier
/// final (déjà pré-alloué). Chaque task ouvre son propre handle, seek à son offset,
/// et écrit les bytes au fur et à mesure qu'ils arrivent du stream HTTP.
/// Les writes parallèles sur des ranges disjointes sont safe.
async fn try_download_chunk_to_offset(
    client: &Client,
    url: &str,
    token: &Option<String>,
    start: u64,
    end: u64,
    dest_path: &Path,
    pb: &ProgressBar,
) -> Result<(), DownloadError> {
    let mut req = client.get(url)
        .header(header::RANGE, format!("bytes={}-{}", start, end));

    if let Some(ref t) = token {
        req = req.bearer_auth(t);
    }

    let mut res = req.send().await?;

    if !res.status().is_success() {
        return Err(DownloadError::ServerError(
            res.status().as_u16(),
            format!("Failed chunk bytes {}-{}", start, end),
        ));
    }

    // Open this task's own handle on the pre-allocated final file, seek to start.
    let mut file = OpenOptions::new()
        .write(true)
        .open(dest_path)
        .await?;
    file.seek(SeekFrom::Start(start)).await?;

    // Stream HTTP body chunks directly to disk at our position.
    // No temp file, no assembly phase.
    loop {
        match res.chunk().await {
            Ok(Some(buf)) => {
                let len = buf.len();
                file.write_all(&buf).await?;
                pb.inc(len as u64);
            }
            Ok(None) => break,
            Err(e) => return Err(e.into()),
        }
    }

    file.flush().await?;
    Ok(())
}
