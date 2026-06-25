use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::fs::File;
use tokio::process::Command;
use tokio_util::io::ReaderStream;

use crate::db;

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: Option<String>,
}

#[derive(Deserialize)]
pub struct DownloadRequest {
    pub url: String,
    pub folder: Option<String>,
}

#[derive(Deserialize)]
pub struct PreviewQuery {
    pub id: String,
}

#[derive(Serialize)]
pub struct DownloadResponse {
    pub message: String,
}

#[derive(Deserialize)]
pub struct MkdirRequest {
    pub path: String, // Music からの相対パス（例: "Artist/Album"）
}

#[derive(Deserialize)]
pub struct DeleteRequest {
    pub path: String, // Music からの相対パス
}

#[derive(Deserialize)]
pub struct RenameRequest {
    pub old_path: String,
    pub new_path: String,
}

#[derive(Serialize)]
pub struct YtResult {
    pub title: String,
    pub channel: String,
    pub duration: i64,
    pub url: String,
}

#[derive(Deserialize)]
struct YtJson {
    title: Option<String>,
    uploader: Option<String>,
    channel: Option<String>,
    duration: Option<f64>,
    id: Option<String>,
}

pub async fn list_songs(
    State(pool): State<SqlitePool>,
    Query(params): Query<SearchQuery>,
) -> impl IntoResponse {
    let result = match params.q.as_deref() {
        Some(q) if !q.is_empty() => db::search_songs(&pool, q).await,
        _ => db::all_songs(&pool).await,
    };
    match result {
        Ok(songs) => Json(songs).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn stream_song(
    State(pool): State<SqlitePool>,
    Path(id): Path<i64>,
) -> Response {
    let song = match db::find_song(&pool, id).await {
        Ok(Some(s)) => s,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let file = match File::open(&song.path).await {
        Ok(f) => f,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Response::builder()
        .header(header::CONTENT_TYPE, "audio/mpeg")
        .body(body)
        .unwrap()
}

#[derive(Deserialize)]
pub struct BrowseQuery {
    pub path: Option<String>,
}

#[derive(Serialize)]
pub struct BrowseResponse {
    pub path: String,
    pub dirs: Vec<String>,
    pub songs: Vec<db::Song>,
}

pub async fn browse(
    State(pool): State<SqlitePool>,
    Query(params): Query<BrowseQuery>,
) -> impl IntoResponse {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let music_root = format!("{}/Music", home);

    // パストラバーサル防止
    let rel = params.path.unwrap_or_default();
    let rel = rel.trim_matches('/').replace("..", "");
    let full_path = if rel.is_empty() {
        music_root.clone()
    } else {
        format!("{}/{}", music_root, rel)
    };

    let mut dirs = vec![];
    let mut song_paths = vec![];

    if let Ok(mut entries) = tokio::fs::read_dir(&full_path).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let p = entry.path();
            if p.is_dir() {
                if let Some(n) = p.file_name().and_then(|n| n.to_str()) {
                    dirs.push(n.to_string());
                }
            } else if p.extension().map(|e| e.eq_ignore_ascii_case("mp3")).unwrap_or(false) {
                if let Some(s) = p.to_str() {
                    song_paths.push(s.to_string());
                }
            }
        }
    }
    dirs.sort();

    let mut songs = vec![];
    for path in &song_paths {
        let song = match db::find_song_by_path(&pool, path).await {
            Ok(Some(s)) => Some(s),
            // DBにない曲はその場でタグを読んで登録
            _ => {
                if let Some(s) = crate::read_tags(std::path::Path::new(path)) {
                    let _ = db::upsert_song(&pool, &s).await;
                    db::find_song_by_path(&pool, path).await.ok().flatten()
                } else {
                    None
                }
            }
        };
        if let Some(s) = song {
            songs.push(s);
        }
    }
    songs.sort_by(|a, b| a.title.cmp(&b.title));

    Json(BrowseResponse { path: rel, dirs, songs }).into_response()
}

fn safe_path(music_root: &str, rel: &str) -> Option<std::path::PathBuf> {
    let path = if rel.starts_with('/') {
        // 絶対パスはそのまま使う（Music ルート内かチェック）
        std::path::PathBuf::from(rel)
    } else {
        let rel = rel.trim_matches('/').replace("..", "");
        std::path::Path::new(music_root).join(&rel)
    };
    if path.starts_with(music_root) { Some(path) } else { None }
}

pub async fn mkdir(
    axum::Json(req): axum::Json<MkdirRequest>,
) -> impl IntoResponse {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let music_root = format!("{}/Music", home);

    match safe_path(&music_root, &req.path) {
        Some(path) => match tokio::fs::create_dir_all(&path).await {
            Ok(_) => StatusCode::OK.into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR,
                Json(DownloadResponse { message: e.to_string() })).into_response(),
        },
        None => StatusCode::BAD_REQUEST.into_response(),
    }
}

pub async fn delete_entry(
    State(pool): State<SqlitePool>,
    axum::Json(req): axum::Json<DeleteRequest>,
) -> impl IntoResponse {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let music_root = format!("{}/Music", home);

    let path = match safe_path(&music_root, &req.path) {
        Some(p) => p,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    if path.is_dir() {
        if let Err(e) = tokio::fs::remove_dir_all(&path).await {
            return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(DownloadResponse { message: e.to_string() })).into_response();
        }
    } else if path.is_file() {
        if let Err(e) = tokio::fs::remove_file(&path).await {
            return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(DownloadResponse { message: e.to_string() })).into_response();
        }
        // DB からも削除
        let path_str = path.to_string_lossy().to_string();
        let _ = sqlx::query("DELETE FROM songs WHERE path = ?")
            .bind(&path_str)
            .execute(&pool)
            .await;
    }

    StatusCode::OK.into_response()
}

pub async fn rename_entry(
    State(pool): State<SqlitePool>,
    axum::Json(req): axum::Json<RenameRequest>,
) -> impl IntoResponse {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let music_root = format!("{}/Music", home);

    let old_full_path = match safe_path(&music_root, &req.old_path) {
        Some(p) => p,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    let new_full_path = match safe_path(&music_root, &req.new_path) {
        Some(p) => p,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    if let Err(e) = tokio::fs::rename(&old_full_path, &new_full_path).await {
        return (StatusCode::INTERNAL_SERVER_ERROR,
            Json(DownloadResponse { message: e.to_string() })).into_response();
    }

    let old_path_str = old_full_path.to_string_lossy().to_string();
    let new_path_str = new_full_path.to_string_lossy().to_string();

    let pattern = format!("{}%", old_path_str);
    let _ = sqlx::query("UPDATE songs SET path = REPLACE(path, ?, ?) WHERE path LIKE ?")
        .bind(&old_path_str)
        .bind(&new_path_str)
        .bind(&pattern)
        .execute(&pool)
        .await;

    StatusCode::OK.into_response()
}

pub async fn search_yt(
    Query(params): Query<SearchQuery>,
) -> impl IntoResponse {
    let q = match params.q.as_deref() {
        Some(q) if !q.is_empty() => q.to_string(),
        _ => return (StatusCode::BAD_REQUEST, Json(Vec::<YtResult>::new())).into_response(),
    };

    let output = Command::new("yt-dlp")
        .args([
            &format!("ytsearch5:{}", q),
            "--flat-playlist",
            "--dump-json",
            "--no-warnings",
        ])
        .output()
        .await;

    let out = match output {
        Ok(o) if o.status.success() => o,
        _ => return (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::<YtResult>::new())).into_response(),
    };

    let results: Vec<YtResult> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<YtJson>(line).ok())
        .filter_map(|j| {
            let id = j.id?;
            Some(YtResult {
                title: j.title.unwrap_or_else(|| "Unknown".into()),
                channel: j.channel.or(j.uploader).unwrap_or_else(|| "Unknown".into()),
                duration: j.duration.unwrap_or(0.0) as i64,
                url: format!("https://www.youtube.com/watch?v={}", id),
            })
        })
        .collect();

    Json(results).into_response()
}

pub async fn preview_song(
    Query(params): Query<PreviewQuery>,
) -> Response {
    if !params.id.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        return StatusCode::BAD_REQUEST.into_response();
    }

    // yt-dlp の -x と -o - は後処理（ffmpeg変換）がstdoutに対応していないため
    // シェルパイプで yt-dlp → ffmpeg と繋ぐ
    let cmd = format!(
        "yt-dlp -f 'bestaudio' -o - --no-playlist --quiet 'https://youtu.be/{}' | ffmpeg -i pipe:0 -f mp3 -q:a 5 pipe:1 2>/dev/null",
        params.id
    );

    let mut child = match tokio::process::Command::new("sh")
        .args(["-c", &cmd])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    tokio::spawn(async move { let _ = child.wait().await; });

    Response::builder()
        .header(header::CONTENT_TYPE, "audio/mpeg")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(ReaderStream::new(stdout)))
        .unwrap()
}

pub async fn download_song(
    State(pool): State<SqlitePool>,
    axum::Json(req): axum::Json<DownloadRequest>,
) -> Response {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let output_tmpl = match req.folder.as_deref() {
        Some(f) if !f.is_empty() => {
            let safe = f.trim_matches('/').replace("..", "");
            format!("{}/Music/{}/%(title)s.%(ext)s", home, safe)
        }
        _ => format!("{}/Music/%(artist,uploader|Unknown)s/%(title)s.%(ext)s", home),
    };

    let output = Command::new("yt-dlp")
        .args([
            "-x",
            "--audio-format", "mp3",
            "--audio-quality", "0",
            "--no-playlist",
            "-o", &output_tmpl,
            "--print", "after_move:filepath",
            &req.url,
        ])
        .output()
        .await;

    match output {
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DownloadResponse { message: "yt-dlp not found".into() }),
        ).into_response(),
        Ok(out) if !out.status.success() => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DownloadResponse {
                message: String::from_utf8_lossy(&out.stderr).to_string(),
            }),
        ).into_response(),
        Ok(out) => {
            let filepath = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if let Some(song) = crate::read_tags(std::path::Path::new(&filepath)) {
                let _ = db::upsert_song(&pool, &song).await;
            }
            (
                StatusCode::OK,
                Json(DownloadResponse { message: format!("downloaded: {}", filepath) }),
            ).into_response()
        }
    }
}
