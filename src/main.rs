mod db;
mod routes;

use axum::{
    Router,
    routing::{get, post},
};
use lofty::{file::TaggedFileExt, prelude::*, probe::Probe};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;
use tower_http::cors::CorsLayer;
use walkdir::WalkDir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let music_dir = format!("{}/Music", home);
    let db_path = format!("sqlite:{}/mustream.db", home);

    let opts = SqliteConnectOptions::from_str(&db_path)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new().connect_with(opts).await?;

    db::init(&pool).await?;
    scan_music(&pool, &music_dir).await;

    let app = Router::new()
        .route("/songs", get(routes::list_songs))
        .route("/stream/{id}", get(routes::stream_song))
        .route("/browse", get(routes::browse))
        .route("/mkdir", post(routes::mkdir))
        .route("/delete", post(routes::delete_entry))
        .route("/rename", post(routes::rename_entry))
        .route("/preview", get(routes::preview_song))
        .route("/search-yt", get(routes::search_yt))
        .route("/download", post(routes::download_song))
        .layer(CorsLayer::permissive())
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn scan_music(pool: &sqlx::SqlitePool, dir: &str) {
    for entry in WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext.eq_ignore_ascii_case("mp3"))
                .unwrap_or(false)
        })
    {
        let path = entry.path();
        if let Some(song) = read_tags(path) {
            let _ = db::upsert_song(pool, &song).await;
        }
    }
    println!("scan complete");
}

pub fn read_tags(path: &Path) -> Option<db::Song> {
    let tagged = Probe::open(path).ok()?.read().ok()?;
    let tag = tagged.primary_tag().or_else(|| tagged.first_tag())?;

    let title = tag.title().map(|s| s.to_string()).unwrap_or_else(|| {
        path.file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string()
    });
    let artist = tag
        .artist()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "Unknown".to_string());
    let album = tag
        .album()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "Unknown".to_string());

    let duration = tagged.properties().duration().as_secs() as i64;

    Some(db::Song {
        id: 0,
        title,
        artist,
        album,
        path: path.to_string_lossy().to_string(),
        duration,
    })
}
